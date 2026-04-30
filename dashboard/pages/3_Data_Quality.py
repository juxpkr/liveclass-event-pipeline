import pandas as pd
import plotly.express as px
import streamlit as st

from src.config import GOLD_QUALITY_PATH
from src.delta_loader import load_delta_table

# 차트에 표시될 한글 레이블 매핑
LABEL_MAP = {
    "total_bronze_messages": "Bronze 메시지",
    "matched_silver_messages": "Silver 매칭",
    "missing_in_silver_messages": "누락",
    "valid_events": "정상",
    "invalid_events": "비정상",
    "late_arrival_events": "지연 도착",
    "avg_latency_sec": "평균 지연",
    "max_latency_sec": "최대 지연",
}

st.header("Data Quality")
st.caption("Bronze → Silver 처리 과정의 정합성, 누락 이벤트, 지연 도착 이벤트를 모니터링합니다.")

df = load_delta_table(GOLD_QUALITY_PATH)

if df.empty:
    st.info("Gold data_quality_metrics 데이터가 아직 없습니다.")
    # 이후 코드 실행을 막아 빈 데이터로 차트 오류 방지
    st.stop()

# Delta Lake는 UTC로 저장 -> KST로 변환
for col in ["window_start", "window_end", "updated_at"]:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True).dt.tz_convert("Asia/Seoul")

# Delta 읽기 시 숫자 컬럼이 object 타입으로 올 수 있으니까 명시적으로 변환
numeric_cols = [
    "total_bronze_messages", "matched_silver_messages", "missing_in_silver_messages",
    "bronze_to_silver_match_rate", "valid_events", "invalid_events", "late_arrival_events",
    "avg_latency_sec", "max_latency_sec",
]
for c in numeric_cols:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

df = df.sort_values("window_start")
# KPI에 표시할 가장 최근 window 행
latest_row = df.dropna(subset=["window_start"]).iloc[-1]

# KPI: 가장 최근 30초 window 기준
st.subheader("최근 30초 집계")

# 6개 컬럼으로 KPI 카드 배치
col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("Bronze 메시지", f"{int(latest_row.get('total_bronze_messages', 0)):,}")
col2.metric("Silver 매칭", f"{int(latest_row.get('matched_silver_messages', 0)):,}")
col3.metric("매칭률", f"{latest_row.get('bronze_to_silver_match_rate', 0) * 100:.1f}%")
col4.metric("누략", f"{int(latest_row.get('missing_in_silver_messages', 0)):,}")
col5.metric("유효하지않음", f"{int(latest_row.get('invalid_events', 0)):,}")
col6.metric("평균 지연 시간", f"{latest_row.get('avg_latency_sec', 0):.2f}s")

# 정합성 + 품질 차트 나란히
left, right = st.columns(2)

with left:
    st.subheader("Bronze → Silver 정합성 추이")
    st.caption("Bronze 이벤트가 Silver 정제 테이블까지 정상 반영되는지 확인합니다.")
    lineage_cols = ["total_bronze_messages", "matched_silver_messages", "missing_in_silver_messages"]
    existing = [c for c in lineage_cols if c in df.columns]
    if existing:
        # 여러 컬럼을 하나의 color 범례로 묶어서 선 차트로 표현
        melt_df = df[["window_start"] + existing].melt(
            id_vars="window_start", var_name="metric", value_name="count"
        )
        melt_df["metric"] = melt_df["metric"].map(LABEL_MAP)
        fig_lineage = px.line(
            melt_df, x="window_start", y="count", color="metric", markers=True,
            labels={"window_start": "시간", "count": "메시지 수", "metric": ""},
        )
        st.plotly_chart(fig_lineage, use_container_width=True)
    else:
        st.info("Lineage 지표가 아직 없습니다.")

with right:
    st.subheader("시간대별 품질 상태")
    st.caption("정상 이벤트, 비정상 이벤트, 지연 도착 이벤트 비중을 window 단위로 모니터링합니다.")
    quality_cols = ["valid_events", "invalid_events", "late_arrival_events"]
    existing = [c for c in quality_cols if c in df.columns]
    if existing:
        melt_df = df[["window_start"] + existing].melt(
            id_vars="window_start", var_name="status", value_name="count"
        )
        melt_df["status"] = melt_df["status"].map(LABEL_MAP)
        # barmode="stack": 막대를 쌓아서 전체 대비 비중을 시각화
        fig_quality = px.bar(
            melt_df, x="window_start", y="count", color="status", barmode="stack",
            labels={"window_start": "시간", "count": "이벤트 수", "status": ""},
        )
        st.plotly_chart(fig_quality, use_container_width=True)
    else:
        st.info("Quality status 지표가 아직 없습니다.")

# 지연 시간 추이
st.subheader("지연 시간 추이")
st.caption("이벤트 발생 시각과 처리 시각의 차이를 기준으로 파이프라인 지연을 확인합니다.")

if "avg_latency_sec" in df.columns:
    latency_df = df[["window_start", "avg_latency_sec"]].dropna()
    fig_latency = px.line(
        latency_df, x="window_start", y="avg_latency_sec", markers=True,
        labels={"window_start": "시간", "avg_latency_sec": "평균 지연 (초)"},
    )
    fig_latency.update_traces(name="평균 지연")
    if "max_latency_sec" in df.columns:
        max_df = df[["window_start", "max_latency_sec"]].dropna()
        # add_scatter: 기존 fig에 최대 지연 선을 점선으로 추가
        fig_latency.add_scatter(
            x=max_df["window_start"], y=max_df["max_latency_sec"],
            mode="lines+markers", name="최대 지연", line=dict(dash="dot"),
        )
    st.plotly_chart(fig_latency, use_container_width=True)
else:
    st.info("Latency 지표가 아직 없습니다.")

# Raw 테이블 (펼쳐보기)
with st.expander("Raw Quality Gold Table"):
    display_cols = [
        "window_start", "window_end",
        "total_bronze_messages", "matched_silver_messages", "missing_in_silver_messages",
        "bronze_to_silver_match_rate",
        "valid_events", "invalid_events", "late_arrival_events",
        "avg_latency_sec", "max_latency_sec",
        "updated_at",
    ]
    existing = [c for c in display_cols if c in df.columns]
    st.dataframe(
        df[existing].sort_values("window_start", ascending=False).head(50),
        use_container_width=True,
        hide_index=True,
    )
