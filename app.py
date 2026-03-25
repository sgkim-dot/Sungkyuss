"""
마케팅 리포트 대시보드 (Streamlit)
실행: streamlit run app.py

인증: SHA-256 비밀번호 해시 비교 (요청의 sha-254는 SHA-256으로 처리)
"""
from __future__ import annotations

import hashlib
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
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


PERIOD_PRESETS = ("전체", "최근7일", "최근14일", "이번주", "저번주")


def _preset_date_range(preset: str, data_min: date, data_max: date) -> tuple[date, date]:
    """데이터 범위와 겹치도록 기간 프리셋을 [시작, 종료] 날짜로 변환 (주 시작: 월요일)."""
    if preset == "전체":
        return data_min, data_max

    today = date.today()
    anchor = min(data_max, max(data_min, today))

    if preset == "최근7일":
        end, start = anchor, anchor - timedelta(days=6)
    elif preset == "최근14일":
        end, start = anchor, anchor - timedelta(days=13)
    elif preset == "이번주":
        mon = anchor - timedelta(days=anchor.weekday())
        start, end = mon, mon + timedelta(days=6)
    elif preset == "저번주":
        this_mon = anchor - timedelta(days=anchor.weekday())
        start = this_mon - timedelta(days=7)
        end = start + timedelta(days=6)
    else:
        return data_min, data_max

    start = max(start, data_min)
    end = min(end, data_max)
    if start > end:
        return data_max, data_min
    return start, end


def _apply_data_filters(
    df: pd.DataFrame,
    *,
    date_start: date,
    date_end: date,
    channels: list[str],
    campaigns: list[str],
    roas_min: float,
    cpa_max: float | None,
) -> pd.DataFrame:
    out = df.copy()
    if date_start > date_end:
        return out.iloc[0:0]
    out = out[(out["date"].dt.date >= date_start) & (out["date"].dt.date <= date_end)]
    if not channels:
        return out.iloc[0:0]
    out = out[out["channel"].isin(channels)]
    if not campaigns:
        return out.iloc[0:0]
    out = out[out["campaign"].isin(campaigns)]

    out = out.assign(_row_roas=0.0)
    m_cost = out["cost"] > 0
    out.loc[m_cost, "_row_roas"] = out.loc[m_cost, "revenue"] / out.loc[m_cost, "cost"]
    out = out[out["_row_roas"] >= roas_min].drop(columns="_row_roas")

    if cpa_max is not None and cpa_max > 0:
        out = out.assign(_cpa=float("inf"))
        m_conv = out["conversions"] > 0
        out.loc[m_conv, "_cpa"] = out.loc[m_conv, "cost"] / out.loc[m_conv, "conversions"]
        out = out[(out["conversions"] == 0) | (out["_cpa"] <= cpa_max)].drop(columns="_cpa")

    return out


def _aggregate_kpis(fr: pd.DataFrame) -> dict[str, float] | None:
    if fr.empty:
        return None
    cost = float(fr["cost"].sum())
    rev = float(fr["revenue"].sum())
    conv = float(fr["conversions"].sum())
    roas = rev / cost if cost else 0.0
    cpa = cost / conv if conv else float("nan")
    return {"cost": cost, "revenue": rev, "roas": roas, "cpa": cpa}


def _pct_delta_str(curr: float, prev: float) -> str | None:
    if prev == 0:
        return None
    return f"{(curr - prev) / prev * 100:+.1f}%"


def _previous_period_range(
    date_start: date, date_end: date, data_min: date
) -> tuple[date, date] | None:
    n_days = (date_end - date_start).days + 1
    prev_end = date_start - timedelta(days=1)
    if prev_end < data_min:
        return None
    prev_start = prev_end - timedelta(days=n_days - 1)
    prev_start = max(prev_start, data_min)
    if prev_start > prev_end:
        return None
    return prev_start, prev_end


def _daily_trend_metrics(fr: pd.DataFrame) -> pd.DataFrame:
    d = fr.assign(_d=fr["date"].dt.date)
    g = d.groupby("_d", as_index=False).agg(
        cost=("cost", "sum"),
        revenue=("revenue", "sum"),
        conversions=("conversions", "sum"),
    )
    g["roas"] = g["revenue"] / g["cost"].replace(0, pd.NA)
    g["cpa"] = g["cost"] / g["conversions"].replace(0, pd.NA)
    g = g.rename(columns={"_d": "날짜"})
    return g


def render_tab_main_dashboard(
    filtered: pd.DataFrame,
    prev_filtered: pd.DataFrame,
) -> None:
    cur = _aggregate_kpis(filtered)
    prev = _aggregate_kpis(prev_filtered) if not prev_filtered.empty else None

    st.subheader("핵심 지표")
    c1, c2, c3, c4 = st.columns(4)
    if cur is None:
        st.info("표시할 데이터가 없습니다.")
        return

    d_cost = _pct_delta_str(cur["cost"], prev["cost"]) if prev else None
    d_rev = _pct_delta_str(cur["revenue"], prev["revenue"]) if prev else None
    d_roas = (
        _pct_delta_str(cur["roas"], prev["roas"])
        if prev and prev["cost"] > 0 and cur["cost"] > 0
        else None
    )
    d_cpa = None
    if (
        prev
        and pd.notna(cur["cpa"])
        and pd.notna(prev["cpa"])
        and prev["cpa"] != 0
    ):
        d_cpa = _pct_delta_str(cur["cpa"], prev["cpa"])

    c1.metric(
        "광고비",
        f"{int(cur['cost']):,}원",
        delta=d_cost,
        delta_color="inverse",
    )
    c2.metric(
        "매출",
        f"{int(cur['revenue']):,}원",
        delta=d_rev,
        delta_color="normal",
    )
    c3.metric(
        "ROAS",
        f"{cur['roas']:.2f}",
        delta=d_roas,
        delta_color="normal",
    )
    c4.metric(
        "CPA",
        f"{int(cur['cpa']):,}원" if pd.notna(cur["cpa"]) else "—",
        delta=d_cpa,
        delta_color="inverse",
    )

    st.subheader("추이")
    trend_choice = st.radio(
        "일별 추이 지표",
        ["광고비", "매출", "ROAS", "CPA"],
        horizontal=True,
        key="dash_trend_metric",
    )
    daily = _daily_trend_metrics(filtered)
    if daily.empty:
        st.info("추이를 그릴 일별 데이터가 없습니다.")
    else:
        col_y = {"광고비": "cost", "매출": "revenue", "ROAS": "roas", "CPA": "cpa"}[trend_choice]
        y_title = {"광고비": "광고비 (원)", "매출": "매출 (원)", "ROAS": "ROAS", "CPA": "CPA (원)"}[
            trend_choice
        ]
        fig_line = go.Figure()
        fig_line.add_trace(
            go.Scatter(
                x=daily["날짜"],
                y=daily[col_y],
                mode="lines+markers",
                name=trend_choice,
                connectgaps=False,
            )
        )
        if trend_choice == "ROAS":
            fig_line.add_hline(
                y=2.0,
                line_dash="dash",
                line_color="rgba(120,120,120,0.9)",
                annotation_text="손익분기 ROAS 2.0 (200%)",
                annotation_position="right",
            )
        fig_line.update_layout(
            margin=dict(l=8, r=8, t=32, b=8),
            yaxis_title=y_title,
            xaxis_title="날짜",
            showlegend=False,
            height=400,
        )
        st.plotly_chart(fig_line, use_container_width=True)

    st.subheader("채널 구성")
    col_d, col_b = st.columns(2)
    ch_cost = filtered.groupby("channel", as_index=False)["cost"].sum()
    ch_agg = filtered.groupby("channel", as_index=False).agg(
        cost=("cost", "sum"),
        revenue=("revenue", "sum"),
    )
    ch_agg["roas"] = ch_agg["revenue"] / ch_agg["cost"].replace(0, pd.NA)

    with col_d:
        if ch_cost["cost"].sum() > 0:
            fig_pie = go.Figure(
                data=[
                    go.Pie(
                        labels=ch_cost["channel"],
                        values=ch_cost["cost"],
                        hole=0.5,
                        textinfo="percent+label",
                        textposition="auto",
                    )
                ]
            )
            fig_pie.update_layout(
                title="채널별 광고비 비중",
                margin=dict(l=8, r=8, t=40, b=8),
                showlegend=False,
                height=380,
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.caption("도넛 차트를 그릴 광고비가 없습니다.")

    with col_b:
        fig_roas_bar = go.Figure(
            go.Bar(
                x=ch_agg["channel"],
                y=ch_agg["roas"],
                marker_color="#636EFA",
            )
        )
        fig_roas_bar.update_layout(
            title="채널별 ROAS",
            yaxis_title="ROAS",
            xaxis_title="채널",
            margin=dict(l=8, r=8, t=40, b=8),
            height=380,
        )
        fig_roas_bar.add_hline(y=2.0, line_dash="dash", line_color="rgba(120,120,120,0.8)")
        st.plotly_chart(fig_roas_bar, use_container_width=True)


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

        period_preset = st.selectbox("기간 프리셋", PERIOD_PRESETS, index=0)
        date_start, date_end = _preset_date_range(period_preset, min_d, max_d)
        st.caption(f"적용 기간: **{date_start}** ~ **{date_end}**")

        channel_sel = st.multiselect("채널", options=channels, default=channels)

        sub = df[df["channel"].isin(channel_sel)] if channel_sel else df.iloc[0:0]
        campaign_options = sorted(sub["campaign"].unique().tolist()) if not sub.empty else []
        campaign_sel = st.multiselect(
            "캠페인 (선택 채널 기준)",
            options=campaign_options,
            default=campaign_options,
        )

        roas_min = st.slider("ROAS 최소", min_value=0.0, max_value=800.0, value=0.0, step=0.5)
        cpa_max_input = st.number_input(
            "CPA 최대 (원)",
            min_value=0,
            value=0,
            step=1000,
            help="0이면 CPA 필터를 적용하지 않습니다.",
        )
        cpa_max = float(cpa_max_input) if cpa_max_input > 0 else None

    filtered = _apply_data_filters(
        df,
        date_start=date_start,
        date_end=date_end,
        channels=channel_sel,
        campaigns=campaign_sel,
        roas_min=roas_min,
        cpa_max=cpa_max,
    )

    prev_range = _previous_period_range(date_start, date_end, min_d)
    if prev_range is None:
        prev_filtered = df.iloc[0:0]
    else:
        ps, pe = prev_range
        prev_filtered = _apply_data_filters(
            df,
            date_start=ps,
            date_end=pe,
            channels=channel_sel,
            campaigns=campaign_sel,
            roas_min=roas_min,
            cpa_max=cpa_max,
        )

    if filtered.empty:
        st.info("선택한 필터에 맞는 데이터가 없습니다.")
        return

    tab_dash, tab_summary, tab_charts, tab_channel, tab_detail = st.tabs(
        ["대시보드", "요약", "차트", "채널 요약", "상세"]
    )

    total_cost = int(filtered["cost"].sum())
    total_rev = int(filtered["revenue"].sum())
    total_imp = int(filtered["impressions"].sum())
    total_clicks = int(filtered["clicks"].sum())
    total_conv = int(filtered["conversions"].sum())
    roas = (total_rev / total_cost) if total_cost else 0.0
    ctr = (total_clicks / total_imp * 100) if total_imp else 0.0
    cvr = (total_conv / total_clicks * 100) if total_clicks else 0.0
    cpc = (total_cost / total_clicks) if total_clicks else 0.0

    with tab_dash:
        st.caption(
            "KPI 증감률은 **동일한 일수**의 바로 이전 기간(전기간)과 비교합니다. 채널·캠페인·ROAS·CPA 필터를 동일하게 적용합니다."
        )
        render_tab_main_dashboard(filtered, prev_filtered)

    with tab_summary:
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

    with tab_charts:
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

    with tab_channel:
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

    with tab_detail:
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
