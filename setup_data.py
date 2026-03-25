"""
예시 마케팅 데이터를 SQLite DB에 생성하는 스크립트
실행: python setup_data.py
"""
import sqlite3
import random
from datetime import datetime, timedelta

DB_PATH = "marketing.db"

# 채널별 설정 (평균 CPC, 전환율 등)
CHANNELS = {
    "Google Ads": {"avg_cpc": 800, "cvr": 0.035, "roas_base": 3.5},
    "Meta Ads": {"avg_cpc": 500, "cvr": 0.025, "roas_base": 2.8},
    "Naver SA": {"avg_cpc": 600, "cvr": 0.040, "roas_base": 4.0},
    "Kakao Moment": {"avg_cpc": 400, "cvr": 0.020, "roas_base": 2.2},
    "TikTok Ads": {"avg_cpc": 300, "cvr": 0.015, "roas_base": 1.8},
}

CAMPAIGNS = {
    "Google Ads": ["브랜드_검색", "경쟁사_검색", "쇼핑_리타겟팅", "디스플레이_인지도"],
    "Meta Ads": ["LAL_전환", "리타겟팅_장바구니", "브로드_인지도", "릴스_영상"],
    "Naver SA": ["브랜드키워드", "카테고리키워드", "경쟁사키워드", "쇼핑검색"],
    "Kakao Moment": ["카카오톡_비즈보드", "디스플레이_리타겟팅", "동영상_인지도"],
    "TikTok Ads": ["인피드_전환", "탑뷰_인지도", "스파크애즈_리뷰어"],
}


def create_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS daily_report")
    cur.execute("""
        CREATE TABLE daily_report (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            channel TEXT NOT NULL,
            campaign TEXT NOT NULL,
            impressions INTEGER NOT NULL,
            clicks INTEGER NOT NULL,
            cost INTEGER NOT NULL,
            conversions INTEGER NOT NULL,
            revenue INTEGER NOT NULL
        )
    """)

    # 최근 30일 데이터 생성
    rows = []
    base_date = datetime(2026, 3, 1)

    for day_offset in range(25):
        date = base_date + timedelta(days=day_offset)
        date_str = date.strftime("%Y-%m-%d")

        # 주말 효과
        is_weekend = date.weekday() >= 5
        weekend_factor = 0.7 if is_weekend else 1.0

        for channel, config in CHANNELS.items():
            for campaign in CAMPAIGNS[channel]:
                # 랜덤 변동성 추가
                noise = random.uniform(0.6, 1.4)
                daily_budget = random.randint(50000, 300000)

                cost = int(daily_budget * noise * weekend_factor)
                clicks = max(1, int(cost / (config["avg_cpc"] * random.uniform(0.8, 1.2))))
                impressions = int(clicks * random.uniform(15, 40))
                conversions = max(0, int(clicks * config["cvr"] * random.uniform(0.5, 1.8)))
                revenue = int(conversions * random.randint(30000, 80000) * random.uniform(0.8, 1.3))

                rows.append((date_str, channel, campaign, impressions, clicks, cost, conversions, revenue))

    cur.executemany(
        "INSERT INTO daily_report (date, channel, campaign, impressions, clicks, cost, conversions, revenue) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )

    conn.commit()
    print(f"✅ {len(rows)}개 행 생성 완료 → {DB_PATH}")
    conn.close()


if __name__ == "__main__":
    create_db()
