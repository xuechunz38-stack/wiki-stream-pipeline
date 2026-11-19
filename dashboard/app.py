"""Live dashboard for the Wikipedia edit stream (reads Postgres, refreshes itself every 5 s)."""
import os
from datetime import timedelta

import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Wikipedia Live Edits", page_icon="📝", layout="wide")

REFRESH = "5s"
NS_NAMES = {0: "Article", 1: "Talk", 2: "User", 3: "User talk", 4: "Project", 6: "File",
            10: "Template", 14: "Category", 118: "Draft"}


def db_url() -> str:
    # env var (docker-compose / local) first, then Streamlit Cloud secrets
    url = os.getenv("DATABASE_URL")
    if not url:
        try:
            url = st.secrets["DATABASE_URL"]
        except Exception:
            url = "postgresql://wiki:wiki@localhost:5432/wikistream"
    return url.replace("postgresql://", "postgresql+psycopg2://", 1)


@st.cache_resource
def engine():
    return create_engine(db_url(), pool_pre_ping=True, pool_size=2, max_overflow=0)


def q(sql: str, **params) -> pd.DataFrame:
    with engine().connect() as c:
        return pd.read_sql(text(sql), c, params=params)


st.title("📝 Wikipedia Live Edits")
st.caption("Real-time edits to every Wikipedia language edition · Wikimedia EventStreams → "
           "Kafka `wikipedia.edits` → Postgres → this dashboard")


@st.fragment(run_every=REFRESH)
def live():
    ref = q("SELECT max(event_time) AS t, count(*) AS n FROM wiki_edits")
    if ref["n"].iloc[0] == 0:
        st.info("Waiting for the first events …")
        return
    latest = pd.Timestamp(ref["t"].iloc[0])
    lag = (pd.Timestamp.now(tz="UTC") - latest).total_seconds()
    if lag < 90:
        st.success(f"🟢 LIVE — newest edit {lag:.0f} s ago · {ref['n'].iloc[0]:,} edits stored "
                   f"· refreshes every {REFRESH}")
    else:
        st.warning(f"⏸ Producer is not running right now — showing the last captured data "
                   f"(newest edit {latest:%Y-%m-%d %H:%M} UTC). Windows below are relative to that time.")

    t5, t15, t60 = latest - timedelta(minutes=5), latest - timedelta(minutes=15), latest - timedelta(minutes=60)

    k = q("""SELECT count(*) AS edits,
                    avg(is_bot::int) * 100 AS bot_pct,
                    avg(is_anonymous::int) * 100 AS anon_pct,
                    count(DISTINCT wiki) AS wikis,
                    sum(GREATEST(bytes_change, 0)) AS added
             FROM wiki_edits WHERE event_time > :t5""", t5=t5).iloc[0]
    k_prev = q("SELECT count(*) AS edits FROM wiki_edits WHERE event_time > :a AND event_time <= :b",
               a=t5 - timedelta(minutes=5), b=t5).iloc[0]
    new_pages = q("SELECT count(*) AS n FROM wiki_edits WHERE edit_type='new' AND event_time > :t", t=t60)["n"][0]
    n_alerts = q("SELECT count(*) AS n FROM edit_alerts WHERE event_time > :t", t=t60)["n"][0]

    c = st.columns(6)
    c[0].metric("Edits · last 5 min", f"{int(k.edits):,}", f"{int(k.edits - k_prev.edits):+,} vs prev 5 min")
    c[1].metric("Edits per second", f"{k.edits / 300:.1f}")
    c[2].metric("Bot share", f"{(k.bot_pct or 0):.1f} %")
    c[3].metric("Logged-out edits", f"{(k.anon_pct or 0):.1f} %",
                help="IP or temporary (~) accounts")
    c[4].metric("New articles · last hour", f"{int(new_pages):,}")
    c[5].metric("Alerts · last hour", f"{int(n_alerts):,}")

    # ---------- derived result: EWMA forecast vs actual + anomalies ----------
    st.subheader("Edits per minute vs. EWMA forecast (stream-processor output)")
    wikis = q("""SELECT wiki FROM edit_stats_minute WHERE window_start > :t AND wiki <> '_all'
                 GROUP BY wiki ORDER BY sum(edits) DESC LIMIT 12""", t=t60)["wiki"].tolist()
    choice = st.selectbox("Series", ["_all (all Wikipedias)"] + wikis, key="series")
    wiki = choice.split(" ")[0]
    s = q("""SELECT window_start, edits, expected_edits, is_anomaly FROM edit_stats_minute
             WHERE wiki = :w AND window_start > :t ORDER BY window_start""", w=wiki, t=t60)
    if s.empty:
        st.caption("No closed windows yet.")
    else:
        long = s.melt("window_start", ["edits", "expected_edits"], var_name="series", value_name="value")
        long["series"] = long["series"].map({"edits": "actual", "expected_edits": "EWMA forecast"})
        lines = alt.Chart(long).mark_line(point=False).encode(
            x=alt.X("utchoursminutes(window_start):T", title="minute (UTC)", axis=alt.Axis(format="%H:%M")),
            y=alt.Y("value:Q", title="edits / minute"),
            color=alt.Color("series:N", scale=alt.Scale(range=["#3366cc", "#ff9900"])),
            strokeDash=alt.condition("datum.series == 'EWMA forecast'", alt.value([5, 4]), alt.value([0])),
            tooltip=["window_start:T", "series:N", alt.Tooltip("value:Q", format=".1f")])
        pts = alt.Chart(s[s.is_anomaly]).mark_point(color="red", size=160, filled=True).encode(
            x="utchoursminutes(window_start):T", y="edits:Q", tooltip=["window_start:T", "edits:Q"])
        st.altair_chart((lines + pts).properties(height=260), use_container_width=True)
        st.caption("Each closed 1-minute window is compared with the forecast made before it closed; "
                   "red dots = spike alerts (z ≥ 3.5).")

    left, right = st.columns(2)
    with left:
        st.subheader("Most active Wikipedias · last 15 min")
        top = q("""SELECT wiki, count(*) AS edits, sum(is_bot::int) AS bot FROM wiki_edits
                   WHERE event_time > :t GROUP BY wiki ORDER BY edits DESC LIMIT 12""", t=t15)
        top["human"] = top["edits"] - top["bot"]
        tl = top.melt("wiki", ["human", "bot"], var_name="editor", value_name="n")
        st.altair_chart(alt.Chart(tl).mark_bar().encode(
            x=alt.X("n:Q", title="edits"), y=alt.Y("wiki:N", sort=top["wiki"].tolist(), title=None),
            color=alt.Color("editor:N", scale=alt.Scale(domain=["human", "bot"], range=["#4c78a8", "#e45756"])),
            tooltip=["wiki", "editor", "n"]).properties(height=330), use_container_width=True)
    with right:
        st.subheader("Bytes added vs. removed per minute")
        b = q("""SELECT window_start, bytes_added, bytes_removed FROM edit_stats_minute
                 WHERE wiki='_all' AND window_start > :t ORDER BY window_start""", t=t60)
        if not b.empty:
            b["bytes_removed"] = -b["bytes_removed"]
            bl = b.melt("window_start", ["bytes_added", "bytes_removed"], var_name="kind", value_name="bytes")
            st.altair_chart(alt.Chart(bl).mark_bar().encode(
                x=alt.X("utchoursminutes(window_start):T", title="minute (UTC)", axis=alt.Axis(format="%H:%M")), y=alt.Y("bytes:Q", title="bytes"),
                color=alt.Color("kind:N", scale=alt.Scale(range=["#54a24b", "#e45756"])),
                tooltip=["window_start:T", "kind", "bytes"]).properties(height=330), use_container_width=True)

    left, right = st.columns(2)
    with left:
        st.subheader("What is being edited · last 15 min")
        ns = q("""SELECT namespace, count(*) AS edits FROM wiki_edits WHERE event_time > :t
                  GROUP BY namespace ORDER BY edits DESC""", t=t15)
        ns["page type"] = ns["namespace"].map(lambda n: NS_NAMES.get(n, "Other"))
        ns = ns.groupby("page type", as_index=False)["edits"].sum()
        st.altair_chart(alt.Chart(ns).mark_arc(innerRadius=60).encode(
            theta="edits:Q", color=alt.Color("page type:N"), tooltip=["page type", "edits"]
        ).properties(height=300), use_container_width=True)
    with right:
        st.subheader("Edit size distribution · last 15 min")
        h = q("""SELECT bytes_change FROM wiki_edits WHERE event_time > :t
                 AND abs(bytes_change) <= 5000""", t=t15)
        st.altair_chart(alt.Chart(h).mark_bar().encode(
            x=alt.X("bytes_change:Q", bin=alt.Bin(maxbins=50), title="bytes changed by one edit"),
            y=alt.Y("count():Q", title="edits")).properties(height=300), use_container_width=True)

    st.subheader("🚨 Latest alerts from the stream processor")
    a = q("""SELECT event_time, alert_type, wiki, value, round(expected::numeric, 1) AS expected,
                    round(zscore::numeric, 2) AS z, detail
             FROM edit_alerts ORDER BY event_time DESC LIMIT 15""")
    st.dataframe(a, use_container_width=True, hide_index=True)

    st.subheader("Most recent edits")
    r = q("""SELECT event_time, wiki, edit_type, title, bytes_change, is_bot, is_anonymous
             FROM wiki_edits ORDER BY event_time DESC LIMIT 12""")
    st.dataframe(r, use_container_width=True, hide_index=True)


live()
