"""패션 트렌드 분석기 — Streamlit 메인 진입점."""
from __future__ import annotations

import streamlit as st
from database.db import init_db

# DB가 없으면 빈 테이블 생성 (배포 환경 대응)
init_db()

st.set_page_config(
    page_title="패션 트렌드 분석기",
    page_icon="\U0001F4C8",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("패션 트렌드 분석기")
st.markdown(
    """
    **무신사**, **29CM**, **W컨셉**, **지그재그** 실시간 패션 트렌드 추적 대시보드입니다.

    사이드바에서 페이지를 선택하세요:

    | 페이지 | 설명 |
    |--------|------|
    | **개요** | 주요 지표, 스크래퍼 상태, 플랫폼별 현황 |
    | **키워드** | 트렌드 키워드 분석 및 순위 변동 |
    | **베스트셀러** | 인기 상품, 브랜드 빈도, 가격 분석 |
    | **인스타그램** | 해시태그 게시물 수 추적 |
    | **플랫폼 비교** | 브랜드 교차 분석 및 가격 비교 |

    데이터는 스크래핑 주기마다 갱신되며, 5분간 캐시됩니다.
    """
)
