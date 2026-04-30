import pandas as pd
import plotly.express as px
import streamlit as st

from src.config import GOLD_COURSE_PATH
from src.delta_loader import load_delta_table


st.header("강의 성과")
st.caption("강의, 카테고리, 크리에이터 기준으로 매출과 구매 성과를 비교합니다.")

# course_metrics는 window 없이 전체 기간을 강의 단위로 집계한 테이블
df = load_delta_table(GOLD_COURSE_PATH)

if df.empty:
    st.info("Course Gold 테이블이 아직 생성되지 않았습니다.")
else:
    # KPI 집계
    total_revenue = int(df["revenue"].fillna(0).sum())
    total_purchase_users = int(df["purchase_users"].fillna(0).sum())

    best_course_id = "-"
    best_category = "-"
    best_course_name = ""

    if len(df) > 0:
        top_row = df.sort_values("revenue", ascending=False).iloc[0]
        best_course_id = top_row["course_id"]
        best_course_name = top_row.get("course_name", "")

    if "category" in df.columns:
        # idxmax: 가장 큰 값의 인덱스(카테고리명)를 반환
        cat_revenue = df.groupby("category")["revenue"].sum()
        if not cat_revenue.empty:
            best_category = cat_revenue.idxmax()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("총 매출", f"{total_revenue:,}")
    col2.metric("구매 유저", f"{total_purchase_users:,}")
    col3.metric("최고 매출 강의", best_course_id)
    if best_course_name:
        # HTML 태그 직접 삽입 (강의명 초록색으로 작게 표시)
        col3.markdown(f"<p style='color:green; margin-top:-12px; font-size:0.8rem;'>{best_course_name}</p>", unsafe_allow_html=True)
    col4.metric("최고 매출 카테고리", best_category)

    # 차트 2개 나란히
    left, right = st.columns(2)

    with left:
        st.subheader("강의별 매출")
        top_df = df.sort_values("revenue", ascending=False).head(10).copy()
        top_df["view_to_purchase_rate"] = (top_df["view_to_purchase_rate"].fillna(0) * 100).round(2).astype(str) + "%"
        # x축에 course_id + 강의명을 HTML로 2줄 표시
        top_df["course_label"] = top_df["course_id"] + "<br><sub><span style='color:green'>" + top_df["course_name"].fillna("") + "</span></sub>"
        fig_course = px.bar(
            top_df,
            x="course_label",
            y="revenue",
            # 마우스 올렸을 때 추가로 보여줄 컬럼
            hover_data={
                "course_name": True,
                "category": True,
                "purchase_users": True,
                "view_to_purchase_rate": True,
            },
            labels={"course_label": "강의", "revenue": "매출", "category": "카테고리", "purchase_users": "구매 유저", "view_to_purchase_rate": "조회 → 구매 전환율"},
        )
        fig_course.update_layout(xaxis=dict(tickangle=0))
        st.plotly_chart(fig_course, use_container_width=True)

    with right:
        st.subheader("카테고리별 매출")
        # 카테고리별로 매출 합산
        cat_df = (
            df.groupby("category", as_index=False)
            .agg(revenue=("revenue", "sum"), purchase_users=("purchase_users", "sum"))
            .sort_values("revenue", ascending=False)
        )
        fig_cat = px.bar(
            cat_df,
            x="category",
            y="revenue",
            hover_data={"purchase_users": True},
            labels={"category": "카테고리", "revenue": "매출", "purchase_users": "구매 유저"},
        )
        st.plotly_chart(fig_cat, use_container_width=True)

    # 강의 순위 테이블
    st.subheader("강의 순위")

    ranking_df = (
        df.sort_values("revenue", ascending=False)
        .reset_index(drop=True)
    )
    # 순위 표시 index 1부터
    ranking_df.index = ranking_df.index + 1

    display_cols = ["course_id", "course_name", "category", "creator_name",
                    "unique_users", "purchase_users", "revenue", "view_to_purchase_rate"]
    # Gold 테이블에 없는 컬럼은 자동 제외
    display_cols = [c for c in display_cols if c in ranking_df.columns]

    ranking_df = ranking_df[display_cols].copy()
    if "view_to_purchase_rate" in ranking_df.columns:
        ranking_df["view_to_purchase_rate"] = (ranking_df["view_to_purchase_rate"].fillna(0) * 100).round(2).astype(str) + "%"
    ranking_df = ranking_df.rename(columns={
        "course_id": "강의 ID",
        "course_name": "강의명",
        "category": "카테고리",
        "creator_name": "크리에이터",
        "unique_users": "조회 유저",
        "purchase_users": "구매 유저",
        "revenue": "매출",
        "view_to_purchase_rate": "조회 → 구매 전환율",
    })

    st.dataframe(ranking_df, use_container_width=True)
