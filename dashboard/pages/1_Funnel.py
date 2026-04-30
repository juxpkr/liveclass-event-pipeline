import pandas as pd
import plotly.express as px
import streamlit as st

from src.config import GOLD_FUNNEL_PATH
from src.delta_loader import load_delta_table


def to_kst(df: pd.DataFrame) -> pd.DataFrame:
    # window_start, window_end를 UTC -> KST로 변환
    df = df.copy()
    for c in ["window_start", "window_end"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=True).dt.tz_convert("Asia/Seoul")
    return df


def render_funnel_page():
    st.header("조회 → 구매 전환 분석")
    st.caption("강의 조회 이벤트가 실제 구매로 이어지는 흐름을 시간대와 유저 세그먼트 기준으로 분석합니다.")

    df = load_delta_table(GOLD_FUNNEL_PATH)

    if df.empty:
        st.info("Funnel Gold 테이블이 아직 생성되지 않았습니다.")
        return

    df = to_kst(df)

    # KPI 집계
    # funnel_df는 window * user_segment 단위라 전체 합산 필요
    total_view = int(df["view_users"].fillna(0).sum())
    total_purchase = int(df["purchase_users"].fillna(0).sum())
    total_revenue = int(df["revenue"].fillna(0).sum()) if "revenue" in df.columns else 0
    avg_conversion = total_purchase / total_view if total_view > 0 else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("총 조회 유저", f"{total_view:,}")
    col2.metric("구매 유저", f"{total_purchase:,}")
    col3.metric("평균 조회 → 구매 전환율", f"{avg_conversion * 100:.2f}%")
    col4.metric("총 매출", f"{total_revenue:,}")

    # 차트 2개: 퍼널 막대 + 시간대별 추이
    # st.columns([1, 3]): 좌측 1/4, 우측 3/4 비율로 분할
    col_left, col_right = st.columns([1, 3])

    with col_left:
        st.subheader("조회 유저 수 vs 구매 유저 수")
        funnel_counts = pd.DataFrame({
            "stage": ["조회", "구매"],
            "users": [total_view, total_purchase],
        })
        fig_counts = px.bar(funnel_counts, x="stage", y="users", text="users")
        fig_counts.update_traces(textposition="outside")
        fig_counts.update_layout(xaxis_title=None, yaxis_title="유저 수")
        st.plotly_chart(fig_counts, use_container_width=True)

    with col_right:
        st.subheader("시간대별 전환 추이")
        # window_start별로 합산
        trend_df = (
            df
            .groupby("window_start", as_index=False)[["view_users", "purchase_users", "revenue"]]
            .sum()
            .sort_values("window_start")
        )
        trend_df["window_start_kst"] = trend_df["window_start"].dt.strftime("%H:%M")
        # melt: 여러 컬럼을 하나의 color 범례로 묶어서 선 차트로 표현
        trend_df = trend_df.rename(columns={"view_users": "조회 유저", "purchase_users": "구매 유저"})
        fig_trend = px.line(
            trend_df.melt(
                id_vars="window_start_kst",
                value_vars=["조회 유저", "구매 유저"],
                var_name="지표",
                value_name="유저 수",
            ),
            x="window_start_kst",
            y="유저 수",
            color="지표",
            markers=True,
        )
        fig_trend.update_layout(xaxis_tickangle=-30, xaxis_title="시간 (KST)", yaxis_title="유저 수")
        st.plotly_chart(fig_trend, use_container_width=True)

    # 세그먼트별 전환율 테이블
    st.subheader("유저 세그먼트별 전환율")

    # user_segment 기준으로 재집계
    segment_df = (
        df
        .groupby("user_segment", as_index=False)
        .agg(
            view_users=("view_users", "sum"),
            purchase_users=("purchase_users", "sum"),
            revenue=("revenue", "sum"),
        )
    )
    # replace(0, nan): 분모가 0이면 nan 처리 후 fillna(0)으로 0% 표시
    segment_df["view_to_purchase_rate"] = (
        segment_df["purchase_users"] / segment_df["view_users"].replace(0, float("nan"))
    ).fillna(0).map("{:.2%}".format)

    st.dataframe(
        segment_df[["user_segment", "view_users", "purchase_users", "view_to_purchase_rate", "revenue"]]
        .sort_values("purchase_users", ascending=False)
        .rename(columns={
            "user_segment": "유저 세그먼트",
            "view_users": "조회 유저",
            "purchase_users": "구매 유저",
            "view_to_purchase_rate": "조회 → 구매 전환율",
            "revenue": "매출",
        }),
        use_container_width=True,
        hide_index=True,
    )


render_funnel_page()
