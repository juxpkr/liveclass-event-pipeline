import os

import pandas as pd
import plotly.express as px
import streamlit as st
from deltalake import DeltaTable
from streamlit_autorefresh import st_autorefresh


DELTA_TABLE_PATH = os.getenv("DELTA_TABLE_PATH", "s3://warehouse/events")

STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "AWS_ALLOW_HTTP": "true",
    "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
}


st.set_page_config(page_title="LiveClass Analytics", layout="wide")
st.title("LiveClass 실시간 이벤트 대시보드")

# Spark consumer가 10초마다 micro-batch를 쓰므로, dashboard는 5초마다 새로고침
st_autorefresh(interval=5000, key="data_refresh")


@st.cache_data(ttl=5)
def load_data():
    try:
        dt = DeltaTable(DELTA_TABLE_PATH, storage_options=STORAGE_OPTIONS)
        return dt.to_pandas()
    except Exception as e:
        st.error(f"Delta table load failed: {e}")
        return pd.DataFrame()


df = load_data()

if df.empty:
    st.warning("데이터를 불러오는 중이거나 아직 데이터가 없습니다. Spark가 데이터를 쓸 때까지 잠시만 기다려주세요.")
    st.stop()


df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)

total_events = len(df)
total_purchases = len(df[df["event_type"] == "purchase"])
total_revenue = df["price"].sum()
unique_users = df["user_id"].nunique()


st.subheader("실시간 핵심 지표")

col1, col2, col3, col4 = st.columns(4)
col1.metric("총 이벤트 수", f"{total_events:,}")
col2.metric("총 구매 건수", f"{total_purchases:,}")
col3.metric("누적 매출액", f"₩{total_revenue:,.0f}")
col4.metric("고유 접속 유저", f"{unique_users:,}")

st.markdown("---")

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("이벤트 타입별 발생 비율")

    event_counts = df["event_type"].value_counts().reset_index()
    event_counts.columns = ["event_type", "count"]

    fig_pie = px.pie(
        event_counts,
        values="count",
        names="event_type",
        hole=0.4,
    )
    st.plotly_chart(fig_pie, use_container_width=True)

with col_right:
    st.subheader("교육 서비스 핵심 퍼널")

    funnel_order = ["class_view", "video_play", "video_complete", "purchase"]

    funnel_data = (
        df[df["event_type"].isin(funnel_order)]["event_type"]
        .value_counts()
        .reindex(funnel_order)
        .fillna(0)
        .astype(int)
        .reset_index()
    )
    funnel_data.columns = ["step", "count"]

    fig_funnel = px.funnel(
        funnel_data,
        x="count",
        y="step",
    )
    st.plotly_chart(fig_funnel, use_container_width=True)

st.markdown("---")

st.subheader("시간대별 이벤트 추이")

trend_df = (
    df.dropna(subset=["event_time"])
    .set_index("event_time")
    .groupby("event_type")
    .resample("1min")
    .size()
    .reset_index(name="count")
)

fig_line = px.line(
    trend_df,
    x="event_time",
    y="count",
    color="event_type",
    markers=True,
)

st.plotly_chart(fig_line, use_container_width=True)

st.caption("이 대시보드는 5초마다 최신 Delta Lake 데이터를 읽어옵니다.")