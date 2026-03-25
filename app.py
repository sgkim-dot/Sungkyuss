"""
마케팅 리포트 대시보드 (Streamlit)
실행: streamlit run app.py

인증: SHA-256 비밀번호 해시 비교 (요청의 sha-254는 SHA-256으로 처리)
"""
from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "marketing.db"

ADMIN_USER = "admin"
# SHA-256("admin1234")
ADMIN_PASSWORD_HASH = "ac9689e2272427085e35b9d3e3e8bed88cb3434828b43b86fc0596cad4c6e270"

MAX_FAILED_ATTEMPTS = 3
LOCKOUT_MINUTES = 5


def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def verify_admin(password: str) -> bool:
    return _hash_password(password) == ADMIN_PASSWORD_HASH


def init_session_state() -> None:
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
    if "username" not in st.session_state:
        st.session_state.username = None
    if "failed_attempts" not in st.session_state:
        st.session_state.failed_attempts = 0
    if "lockout_until" not in st.session_state:
        st.session_state.lockout_until = None


def _lockout_remaining() -> timedelta | None:
    until = st.session_state.lockout_until
    if until is None:
        return None
    now = datetime.now()
    if now >= until:
        st.session_state.lockout_until = None
        st.session_state.failed_attempts = 0
        return None
    return until - now


def register_failed_login() -> None:
    st.session_state.failed_attempts += 1
    if st.session_state.failed_attempts >= MAX_FAILED_ATTEMPTS:
        st.session_state.lockout_until = datetime.now() + timedelta(minutes=LOCKOUT_MINUTES)
        st.session_state.failed_attempts = 0


def clear_login_security() -> None:
    st.session_state.failed_attempts = 0
    st.session_state.lockout_until = None


def logout() -> None:
    st.session_state.logged_in = False
    st.session_state.username = None
    st.rerun()


@st.cache_data
def load_report() -> pd.DataFrame:
    if not DB_PATH.is_file():
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(
            """
            SELECT date, channel, campaign, impressions, clicks, cost, conversions, revenue
            FROM daily_report
            ORDER BY date
            """,
            conn,
        )
    finally:
        conn.close()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df


def render_login() -> None:
    st.title("로그인")
    st.caption("관리자 계정: admin / admin1234")

    remaining = _lockout_remaining()
    if remaining is not None:
        total_sec = int(remaining.total_seconds())
        m, s = total_sec // 60, total_sec % 60
        st.error(f"로그인을 {MAX_FAILED_ATTEMPTS}회 이상 실패했습니다. **{m}분 {s}초** 후 다시 시도할 수 있습니다.")

    with st.form("login_form"):
        username = st.text_input("아이디", value="", placeholder="admin")
        password = st.text_input("비밀번호", type="password")
        submitted = st.form_submit_button("로그인", disabled=remaining is not None)

    if not submitted or remaining is not None:
        return

    uid = username.strip().lower()
    if uid != ADMIN_USER:
        register_failed_login()
        left = MAX_FAILED_ATTEMPTS - st.session_state.failed_attempts
        if st.session_state.lockout_until:
            st.error("로그인에 반복 실패하여 5분간 제한되었습니다.")
        else:
            st.error(f"아이디 또는 비밀번호가 올바르지 않습니다. (남은 시도: {left}회)")
        st.rerun()
        return

    if not verify_admin(password):
        register_failed_login()
        if st.session_state.lockout_until:
            st.error("로그인에 반복 실패하여 5분간 제한되었습니다.")
        else:
            left = MAX_FAILED_ATTEMPTS - st.session_state.failed_attempts
            st.error(f"아이디 또는 비밀번호가 올바르지 않습니다. (남은 시도: {left}회)")
        st.rerun()
        return

    clear_login_security()
    st.session_state.logged_in = True
    st.session_state.username = ADMIN_USER
    st.rerun()


def render_dashboard(df: pd.DataFrame) -> None:
    st.title("마케팅 성과 대시보드")

    if df.empty:
        st.warning(f"데이터가 없습니다. `marketing.db`를 확인하세요: `{DB_PATH}`")
        return

    channels = sorted(df["channel"].unique().tolist())
    min_d = df["date"].min().date()
    max_d = df["date"].max().date()

    with st.sidebar:
        st.markdown(f"**사용자:** `{st.session_state.username}`")
        if st.button("로그아웃", use_container_width=True):
            logout()
        st.divider()
        st.header("필터")
        channel_sel = st.multiselect("채널", options=channels, default=channels)
        all_campaigns = sorted(df["campaign"].unique().tolist())
        campaign_sel = st.multiselect("캠페인", options=all_campaigns, default=all_campaigns)
        date_range = st.date_input("기간", value=(min_d, max_d), min_value=min_d, max_value=max_d)

    filtered = df.copy()
    if channel_sel:
        filtered = filtered[filtered["channel"].isin(channel_sel)]
    else:
        filtered = filtered.iloc[0:0]
    if campaign_sel:
        filtered = filtered[filtered["campaign"].isin(campaign_sel)]
    else:
        filtered = filtered.iloc[0:0]

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start, end = date_range
        filtered = filtered[(filtered["date"].dt.date >= start) & (filtered["date"].dt.date <= end)]
    elif hasattr(date_range, "year"):
        filtered = filtered[filtered["date"].dt.date == date_range]

    if filtered.empty:
        st.info("선택한 필터에 맞는 데이터가 없습니다.")
        return

    total_cost = int(filtered["cost"].sum())
    total_rev = int(filtered["revenue"].sum())
    total_imp = int(filtered["impressions"].sum())
    total_clicks = int(filtered["clicks"].sum())
    total_conv = int(filtered["conversions"].sum())
    roas = (total_rev / total_cost) if total_cost else 0.0
    ctr = (total_clicks / total_imp * 100) if total_imp else 0.0
    cvr = (total_conv / total_clicks * 100) if total_clicks else 0.0
    cpc = (total_cost / total_clicks) if total_clicks else 0.0

    r1 = st.columns(4)
    r1[0].metric("총 비용", f"{total_cost:,}원")
    r1[1].metric("총 매출", f"{total_rev:,}원")
    r1[2].metric("ROAS", f"{roas:.2f}")
    r1[3].metric("노출", f"{total_imp:,}")

    r2 = st.columns(5)
    r2[0].metric("클릭", f"{total_clicks:,}")
    r2[1].metric("CTR", f"{ctr:.2f}%")
    r2[2].metric("전환", f"{total_conv:,}")
    r2[3].metric("CVR", f"{cvr:.2f}%")
    r2[4].metric("평균 CPC", f"{cpc:,.0f}원")

    st.subheader("일별 비용·매출 추이")
    _fd = filtered.assign(_day=filtered["date"].dt.date)
    daily = (
        _fd.groupby("_day", as_index=False)
        .agg(cost=("cost", "sum"), revenue=("revenue", "sum"))
        .rename(columns={"_day": "날짜"})
        .set_index("날짜")
    )
    st.line_chart(daily)

    cleft, cright = st.columns(2)
    with cleft:
        st.subheader("채널별 비용")
        by_ch = filtered.groupby("channel", as_index=False)["cost"].sum().sort_values("cost", ascending=False)
        st.bar_chart(by_ch.set_index("channel"))
    with cright:
        st.subheader("캠페인별 매출 (상위 10)")
        top_c = (
            filtered.groupby("campaign", as_index=False)["revenue"]
            .sum()
            .sort_values("revenue", ascending=False)
            .head(10)
        )
        st.bar_chart(top_c.set_index("campaign"))

    st.subheader("채널 요약")
    ch_agg = filtered.groupby("channel", as_index=False).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        cost=("cost", "sum"),
        conversions=("conversions", "sum"),
        revenue=("revenue", "sum"),
    )
    ch_agg["ROAS"] = (ch_agg["revenue"] / ch_agg["cost"]).replace([float("inf")], 0).fillna(0).round(2)
    ch_agg["CTR_%"] = (ch_agg["clicks"] / ch_agg["impressions"] * 100).replace([float("inf")], 0).fillna(0).round(2)
    ch_agg["CVR_%"] = (ch_agg["conversions"] / ch_agg["clicks"] * 100).replace([float("inf")], 0).fillna(0).round(2)
    ch_agg = ch_agg.rename(
        columns={
            "channel": "채널",
            "impressions": "노출",
            "clicks": "클릭",
            "cost": "비용",
            "conversions": "전환",
            "revenue": "매출",
        }
    )
    st.dataframe(ch_agg, use_container_width=True, hide_index=True)

    st.subheader("원본 행")
    st.dataframe(
        filtered.sort_values(["date", "channel", "campaign"], ascending=False),
        use_container_width=True,
        hide_index=True,
    )


def main() -> None:
    st.set_page_config(page_title="마케팅 대시보드", layout="wide")
    init_session_state()

    if not st.session_state.logged_in:
        render_login()
        return

    render_dashboard(load_report())


if __name__ == "__main__":
    main()
