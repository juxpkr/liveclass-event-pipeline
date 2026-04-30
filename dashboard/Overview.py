import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.config import GOLD_FUNNEL_PATH, GOLD_COURSE_PATH
from src.delta_loader import load_delta_table

# 브라우저 탭 제목과 레이아웃 설정
st.set_page_config(
    page_title="LiveClass Event Pipeline",
    layout="wide",
)


def prepare_recent_funnel_df(df, minutes=15):
    #funnel_df에서 최근 N분 데이터만 추려서 반환
    if df.empty or "window_start" not in df.columns:
        return pd.DataFrame()

    df = df.copy()

    # Delta Lake는 UTC로 저장 -> KST로 변환해서 표시
    for col in ["window_start", "window_end", "updated_at"]:
        if col in df.columns:
            df[col] = (
                pd.to_datetime(df[col], errors="coerce", utc=True)
                .dt.tz_convert("Asia/Seoul")
            )

    df = df.dropna(subset=["window_start"])

    now_kst = pd.Timestamp.now(tz="Asia/Seoul")
    cutoff = now_kst - pd.Timedelta(minutes=minutes)
    df = df[df["window_start"] >= cutoff]

    # 유저 활동이 없는 window(모든 유저 수가 0)는 제외
    funnel_cols = ["view_users", "purchase_users", "play_users", "complete_users"]
    existing_cols = [c for c in funnel_cols if c in df.columns]
    if existing_cols:
        df = df[df[existing_cols].fillna(0).sum(axis=1) > 0]

    return df.sort_values("window_start")


st.header("Overview")
st.caption("서비스 이벤트 파이프라인의 핵심 매출, 구매 지표를 요약합니다.")

# Gold 테이블에서 데이터 로드 (5초 캐시 적용)
funnel_df = load_delta_table(GOLD_FUNNEL_PATH)
course_df = load_delta_table(GOLD_COURSE_PATH)

if funnel_df.empty and course_df.empty:
    st.info("Gold 데이터가 아직 생성되지 않았습니다. 잠시 후 새로고침하세요.")
else:
    # KPI 집계
    total_revenue = 0
    total_purchase_users = 0
    avg_view_to_purchase_rate = 0
    top_course_label = "-"
    top_course_revenue = None

    if not course_df.empty:
        total_revenue = int(course_df["revenue"].fillna(0).sum())
        total_purchase_users = int(course_df["purchase_users"].fillna(0).sum())

        if "view_to_purchase_rate" in course_df.columns:
            avg_view_to_purchase_rate = float(course_df["view_to_purchase_rate"].fillna(0).mean())

        top_course_name = ""
        if "revenue" in course_df.columns and len(course_df) > 0:
            top_row = course_df.sort_values("revenue", ascending=False).iloc[0]
            top_course_label = str(top_row.get("course_id", "-"))
            top_course_revenue = f"매출 {int(top_row.get('revenue', 0)):,}"
            top_course_name = str(top_row.get("course_name", ""))

    # st.columns(4): 화면을 4등분해서 KPI 카드 배치
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("총 매출", f"{total_revenue:,}")
    col2.metric("구매 유저", f"{total_purchase_users:,}")
    col3.metric("평균 조회 → 구매 전환율", f"{avg_view_to_purchase_rate * 100:.2f}%")
    # delta: KPI 카드 아래 초록색으로 표시되는 보조 수치
    col4.metric("최고 매출 강의", top_course_label, delta=top_course_revenue)
    if top_course_name:
        # HTML 태그 직접 삽입 (강의명 작게 표시)
        col4.markdown(f"<p style='margin-top:-12px; font-size:0.8rem;'>{top_course_name}</p>", unsafe_allow_html=True)

    # 매출 추이 차트
    st.subheader("매출 추이")
    st.caption("최근 15분 동안 1분 단위 매출과 구매 유저 수를 보여줍니다.")

    funnel_df = prepare_recent_funnel_df(funnel_df, minutes=15)

    if not funnel_df.empty and "revenue" in funnel_df.columns:
        # window_start별로 합산
        trend_df = (
            funnel_df
            .groupby("window_start", as_index=False)[["revenue", "purchase_users"]]
            .sum()
            .sort_values("window_start")
        )
        trend_df["window_start"] = trend_df["window_start"].dt.strftime("%H:%M:%S")

        # 이중 y축: 매출(막대)과 구매 유저(선)를 같은 차트에 표시
        fig = go.Figure()
        fig.add_trace(go.Bar(x=trend_df["window_start"], y=trend_df["revenue"], name="매출", yaxis="y1"))
        fig.add_trace(go.Scatter(x=trend_df["window_start"], y=trend_df["purchase_users"], name="구매 유저", yaxis="y2", mode="lines+markers"))
        fig.update_layout(
            xaxis=dict(tickangle=-30),
            yaxis=dict(title="매출"),
            yaxis2=dict(title="구매 유저", overlaying="y", side="right"),
            legend=dict(orientation="h", y=1.1),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Funnel 데이터가 아직 없습니다.")

    # 매출 상위 강의 테이블
    st.subheader("매출 상위 강의")

    if not course_df.empty:
        display_cols = [
            "course_id", "course_name", "category", "creator_id", "creator_name",
            "unique_users", "purchase_users", "revenue", "view_to_purchase_rate",
        ]
        # Gold 테이블에 없는 컬럼은 자동 제외
        display_cols = [c for c in display_cols if c in course_df.columns]
        top_courses = (
            course_df
            .sort_values("revenue", ascending=False)
            .head(10)[display_cols]
            .reset_index(drop=True)
        )
        top_courses.index = top_courses.index + 1
        if "view_to_purchase_rate" in top_courses.columns:
            top_courses["view_to_purchase_rate"] = (top_courses["view_to_purchase_rate"].fillna(0) * 100).round(2).astype(str) + "%"
        top_courses = top_courses.rename(columns={
            "course_id": "강의 ID",
            "course_name": "강의명",
            "category": "카테고리",
            "creator_id": "크리에이터 ID",
            "creator_name": "크리에이터명",
            "unique_users": "조회 유저",
            "purchase_users": "구매 유저",
            "revenue": "매출",
            "view_to_purchase_rate": "조회 → 구매 전환율",
        })
        st.dataframe(top_courses, use_container_width=True)
    else:
        st.info("Course 데이터가 아직 없습니다.")
