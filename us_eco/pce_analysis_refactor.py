# %%
"""
PCE Analysis 데이터 분석 (리팩토링 버전)
- us_eco_utils를 사용한 통합 구조
- 시리즈 정의와 분석 로직만 포함
"""

import pandas as pd
import numpy as np
from datetime import datetime
import sys
import warnings
warnings.filterwarnings('ignore')

# 통합 유틸리티 함수 불러오기
from us_eco_utils import *

# %%
# === FRED API 키 설정 ===
api_config.FRED_API_KEY = 'f4bd434811e42e42287a0e5ccf400fff'

# %%
# === PCE 시리즈 정의 ===
PCE_MAIN_SERIES = {
    # 핵심 PCE 지표 (실질 소비 MoM 변화율) - 이미 MoM 처리됨
    'pce_total': 'DPCERAM1M225NBEA',  # Real PCE - total (MoM %)
    'pce_goods': 'DGDSRAM1M225NBEA',  # Goods (MoM %)
    'pce_durable': 'DDURRAM1M225NBEA',  # Durable goods (MoM %)
    'pce_nondurable': 'DNDGRAM1M225NBEA',  # Nondurable goods (MoM %)
    'pce_services': 'DSERRAM1M225NBEA',  # Services (MoM %)
    'pce_core': 'DPCCRAM1M225NBEA',  # PCE ex-food & energy (MoM %)
    'pce_food': 'DFXARAM1M225NBEA',  # Food (MoM %)
    'pce_energy': 'DNRGRAM1M225NBEA',  # Energy goods & services (MoM %)
    'pce_market_based': 'DPCMRAM1M225NBEA',  # Market-based PCE (MoM %)
    'pce_market_core': 'DPCXRAM1M225NBEA',  # Market-based PCE ex-food & energy (MoM %)
    
    # PCE 가격지수 (물가 MoM 변화율) - 이미 MoM 처리됨
    'pce_price_headline': 'DPCERGM1M225SBEA',  # PCE Price index (headline) (MoM %)
    'pce_price_goods': 'DGDSRGM1M225SBEA',  # Goods prices (MoM %)
    'pce_price_durable': 'DDURRGM1M225SBEA',  # Durable goods prices (MoM %)
    'pce_price_nondurable': 'DNDGRGM1M225SBEA',  # Nondurable goods prices (MoM %)
    'pce_price_services': 'DSERRGM1M225SBEA',  # Services prices (MoM %)
    'pce_price_core': 'DPCCRGM1M225SBEA',  # Core PCE (ex-food & energy) (MoM %)
    'pce_price_food': 'DFXARGM1M225SBEA',  # Food prices (MoM %)
    'pce_price_energy': 'DNRGRGM1M225SBEA',  # Energy prices (MoM %)
    'pce_price_market': 'DPCMRGM1M225SBEA',  # Market-based PCE prices (MoM %)
    'pce_price_market_core': 'DPCXRGM1M225SBEA',  # Market-based core PCE prices (MoM %)
    
    # 추가 경제지표 (레벨 데이터 - MoM 계산 필요)
    'personal_income': 'PI',  # Personal income (level data)
    'disposable_income': 'DSPI',  # Disposable personal income (level data)
    'saving_rate': 'PSAVERT',  # Personal saving rate (level data)
}

# PCE 한국어 이름 매핑
PCE_KOREAN_NAMES = {
    'pce_total': '전체 개인소비',
    'pce_goods': '상품소비',
    'pce_durable': '내구재소비',
    'pce_nondurable': '비내구재소비',
    'pce_services': '서비스소비',
    'pce_core': '근원 개인소비',
    'pce_food': '식품소비',
    'pce_energy': '에너지소비',
    'pce_market_based': '시장기반 개인소비',
    'pce_market_core': '시장기반 근원소비',
    'pce_price_headline': 'PCE 물가지수',
    'pce_price_goods': '상품물가',
    'pce_price_durable': '내구재물가',
    'pce_price_nondurable': '비내구재물가',
    'pce_price_services': '서비스물가',
    'pce_price_core': '근원 PCE물가',
    'pce_price_food': '식품물가',
    'pce_price_energy': '에너지물가',
    'pce_price_market': '시장기반 PCE물가',
    'pce_price_market_core': '시장기반 근원물가',
    'personal_income': '개인소득',
    'disposable_income': '가처분소득',
    'saving_rate': '저축률'
}

# PCE 카테고리 분류
PCE_CATEGORIES = {
    '실질소비': {
        '전체소비': ['pce_total', 'pce_core'],
        '상품소비': ['pce_goods', 'pce_durable', 'pce_nondurable'], 
        '서비스소비': ['pce_services'],
        '품목별소비': ['pce_food', 'pce_energy']
    },
    'PCE물가': {
        '전체물가': ['pce_price_headline', 'pce_price_core'],
        '상품물가': ['pce_price_goods', 'pce_price_durable', 'pce_price_nondurable'],
        '서비스물가': ['pce_price_services'],
        '품목별물가': ['pce_price_food', 'pce_price_energy']
    },
    '소득지표': {
        '소득': ['personal_income', 'disposable_income'],
        '저축': ['saving_rate']
    },
    '시장기반지표': {
        '시장기반소비': ['pce_market_based', 'pce_market_core'],
        '시장기반물가': ['pce_price_market', 'pce_price_market_core']
    }
}

# %%
# === 전역 변수 ===
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/pce_data_refactored.csv'
PCE_DATA = {}

# %%
# === 데이터 로드 함수 ===
def load_pce_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """통합 함수 사용한 PCE 데이터 로드"""
    global PCE_DATA

    # 시리즈 딕셔너리를 직접 전달 (2024.08.24 업데이트)
    # PCE_MAIN_SERIES는 이미 {'시리즈_이름': 'API_ID'} 형태
    result = load_economic_data(
        series_dict=PCE_MAIN_SERIES,
        data_source='FRED',
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
        tolerance=10.0  # PCE 데이터는 10.0
    )

    if result:
        PCE_DATA = result
        print_load_info()
        return True
    else:
        print("❌ PCE 데이터 로드 실패")
        return False

def print_load_info():
    """PCE 데이터 로드 정보 출력"""
    if not PCE_DATA or 'load_info' not in PCE_DATA:
        print("⚠️ 로드된 데이터가 없습니다.")
        return
    
    info = PCE_DATA['load_info']
    print(f"\n📊 PCE 데이터 로드 완료!")
    print(f"   📅 로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S') if info['load_time'] else 'N/A'}")
    print(f"   📈 시리즈 개수: {info['series_count']}개")
    print(f"   📊 데이터 포인트: {info['data_points']}개")
    print(f"   🎯 시작 날짜: {info['start_date']}")
    print(f"   🔗 데이터 소스: {info['source']}")
    
    if 'raw_data' in PCE_DATA and not PCE_DATA['raw_data'].empty:
        latest_date = PCE_DATA['raw_data'].index[-1].strftime('%Y-%m-%d')
        print(f"   📅 최신 데이터: {latest_date}")

# %%
# === 범용 시각화 함수 ===
def plot_pce_series_advanced(series_list, chart_type='multi_line', 
                            data_type='mom', periods=None, target_date=None,
                            left_ytitle=None, right_ytitle=None):
    """범용 PCE 시각화 함수 - plot_economic_series 활용"""
    if not PCE_DATA:
        print("⚠️ 먼저 load_pce_data()를 실행하세요.")
        return None

    return plot_economic_series(
        data_dict=PCE_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        left_ytitle=left_ytitle,
        right_ytitle=right_ytitle,
        korean_names=PCE_KOREAN_NAMES
    )

# %%
# === 데이터 Export 함수 ===
def export_pce_data(series_list, data_type='mom', periods=None, 
                   target_date=None, export_path=None, file_format='excel'):
    """PCE 데이터 export 함수 - export_economic_data 활용"""
    if not PCE_DATA:
        print("⚠️ 먼저 load_pce_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=PCE_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=PCE_KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )

# %%
# === 데이터 접근 함수들 ===

def clear_pce_data():
    """PCE 데이터 초기화"""
    global PCE_DATA
    PCE_DATA = {}
    print("🗑️ PCE 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not PCE_DATA or 'raw_data' not in PCE_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_pce_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return PCE_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in PCE_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return PCE_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not PCE_DATA or 'mom_data' not in PCE_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_pce_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return PCE_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in PCE_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return PCE_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not PCE_DATA or 'yoy_data' not in PCE_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_pce_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return PCE_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in PCE_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return PCE_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not PCE_DATA or 'raw_data' not in PCE_DATA:
        return []
    return list(PCE_DATA['raw_data'].columns)

# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 PCE 시리즈 표시"""
    print("=== 사용 가능한 PCE 시리즈 ===")
    
    # 2024.08.24 업데이트: 시리즈 이름이 key, API_ID가 value
    for series_name, series_id in PCE_MAIN_SERIES.items():
        korean_name = PCE_KOREAN_NAMES.get(series_name, series_name)
        print(f"  '{series_name}': {korean_name} ({series_id})")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, groups in PCE_CATEGORIES.items():
        print(f"\n{category}:")
        for group_name, series_list in groups.items():
            print(f"  {group_name}: {len(series_list)}개 시리즈")
            # 2024.08.24 업데이트: 카테고리도 시리즈 이름 기준으로 수정
            for series_name in series_list:
                korean_name = PCE_KOREAN_NAMES.get(series_name, series_name)
                api_id = PCE_MAIN_SERIES.get(series_name, series_name)
                print(f"    - {series_name}: {korean_name} ({api_id})")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not PCE_DATA or 'load_info' not in PCE_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': PCE_DATA['load_info']['loaded'],
        'series_count': PCE_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': PCE_DATA['load_info']
    }

# %%
# === 사용 예시 ===

print("=== 리팩토링된 PCE 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_pce_data()  # 스마트 업데이트")
print("   load_pce_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!):")
print("   plot_pce_series_advanced(['pce_total', 'pce_core'], 'multi_line', 'mom')")
print("   plot_pce_series_advanced(['pce_price_headline'], 'horizontal_bar', 'yoy', left_ytitle='%')")
print("   plot_pce_series_advanced(['pce_total'], 'single_line', 'mom', periods=24, left_ytitle='%')")
print("   plot_pce_series_advanced(['pce_total', 'pce_services'], 'dual_axis', 'raw', left_ytitle='%', right_ytitle='%')")
print()
print("3. 🔥 데이터 Export:")
print("   export_pce_data(['pce_total', 'pce_core'], 'mom')")
print("   export_pce_data(['pce_price_headline'], 'raw', periods=24, file_format='csv')")
print("   export_pce_data(['pce_total'], 'yoy', target_date='2024-06-01')")
print()
print("✅ plot_pce_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
print("✅ export_pce_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수 사용!")

# %%
load_pce_data()
plot_pce_series_advanced(['pce_total', 'pce_core'], 'multi_line', 'raw', left_ytitle='%')
# %%
