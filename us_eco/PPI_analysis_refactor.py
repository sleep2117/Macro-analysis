# %%
"""
PPI 데이터 분석 (리팩토링 버전)
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
# === BLS API 키 설정 ===
api_config.BLS_API_KEY = '56b193612b614cdc9416359fd1c73a74'
api_config.BLS_API_KEY2 = '0450ef37363c48b5bedd2ae6fc92dd6e'
api_config.BLS_API_KEY3 = 'daf1ca7970b74e81b6a5c7a80a8b8a7f'

# %%
# === PPI 시리즈 정의 ===

# PPI 시리즈 ID와 영어 이름 매핑
PPI_SERIES = {
    # Final Demand (최종 수요)
    'WPSFD4': 'Final demand',
    'WPSFD41': 'Final demand goods',
    'WPSFD411': 'Final demand foods',
    'WPSFD412': 'Final demand energy',
    'WPSFD49104': 'Final demand less foods and energy',
    'WPSFD49116': 'Final demand less foods, energy, & trade services',
    'WPSFD42': 'Final demand services',
    'WPSFD422': 'Final demand transportation and warehousing',
    'WPSFD423': 'Final demand trade services',
    'WPSFD421': 'Final demand services less trade, trans., wrhsg',
    'WPSFD43': 'Final demand construction',

    # Final Demand (최종 수요) - 계절미조정
    'WPUFD4': 'Final demand',
    'WPUFD41': 'Final demand goods',
    'WPUFD411': 'Final demand foods',
    'WPUFD412': 'Final demand energy',
    'WPUFD49104': 'Final demand less foods and energy',
    'WPUFD49116': 'Final demand less foods, energy, & trade services',
    'WPUFD42': 'Final demand services',
    'WPUFD422': 'Final demand transportation and warehousing',
    'WPUFD423': 'Final demand trade services',
    'WPUFD421': 'Final demand services less trade, trans., wrhsg',
    'WPUFD43': 'Final demand construction',
    
    # Intermediate Demand (중간 수요)
    'WPSID61': 'Processed goods for intermediate demand',
    'WPSID62': 'Unprocessed goods for intermediate demand',
    'WPSID63': 'Services for intermediate demand',
    'WPSID54': 'Stage 4 intermediate demand',
    'WPSID53': 'Stage 3 intermediate demand',
    'WPSID52': 'Stage 2 intermediate demand',
    'WPSID51': 'Stage 1 intermediate demand',
    
    # Specific Commodities (주요 품목)
    'WPS1411': 'Motor vehicles',
    'WPS0638': 'Pharmaceutical preparations',
    'WPS0571': 'Gasoline',
    'WPS0221': 'Meats',
    'WPS061': 'Industrial chemicals',
    'WPS081': 'Lumber',
    'WPS1017': 'Steel mill products',
    'WPS057303': 'Diesel fuel',
    'WPS029': 'Prepared animal feeds',
    'WPS0561': 'Crude petroleum',
    'WPS012': 'Grains',
    'WPS101211': 'Carbon steel scrap',
    
    # Services (서비스)
    'WPS5111': 'Outpatient healthcare',
    'WPS5121': 'Inpatient healthcare services',
    'WPS5811': 'Food and alcohol retailing',
    'WPS5831': 'Apparel and jewelry retailing',
    'WPS3022': 'Airline passenger services',
    'WPS4011': 'Securities brokerage, investment, and related',
    'WPS3911': 'Business loans (partial)',
    'WPS4511': 'Legal services',
    'WPS301': 'Truck transportation of freight',
    'WPS057': 'Machinery and equipment wholesaling',
    
    # All Commodities (전체 상품)
    'WPSSOP3000': 'All commodities',
    'WPS03THRU15': 'Industrial commodities'
}

# 한국어 이름 매핑
PPI_KOREAN_NAMES = {
    # Final Demand (최종수요) - 계절조정
    'WPSFD4': '최종수요 (계절조정)',
    'WPSFD41': '최종수요 재화 (계절조정)',
    'WPSFD411': '최종수요 식품 (계절조정)',
    'WPSFD412': '최종수요 에너지 (계절조정)',
    'WPSFD49104': '최종수요(식품·에너지 제외) (계절조정)',
    'WPSFD49116': '최종수요(식품·에너지·무역서비스 제외) (계절조정)',
    'WPSFD42': '최종수요 서비스 (계절조정)',
    'WPSFD422': '최종수요 운송·창고업 (계절조정)',
    'WPSFD423': '최종수요 무역서비스 (계절조정)',
    'WPSFD421': '최종수요 서비스(무역·운송·창고 제외) (계절조정)',
    'WPSFD43': '최종수요 건설업 (계절조정)',
    
    # Final Demand (최종수요) - 계절미조정
    'WPUFD4': '최종수요',
    'WPUFD41': '최종수요 재화',
    'WPUFD411': '최종수요 식품',
    'WPUFD412': '최종수요 에너지',
    'WPUFD49104': '최종수요(식품·에너지 제외)',
    'WPUFD49116': '최종수요(식품·에너지·무역서비스 제외)',
    'WPUFD42': '최종수요 서비스',
    'WPUFD422': '최종수요 운송·창고업',
    'WPUFD423': '최종수요 무역서비스',
    'WPUFD421': '최종수요 서비스(무역·운송·창고 제외)',
    'WPUFD43': '최종수요 건설업',
    
    # Intermediate Demand (중간수요) - 계절조정
    'WPSID61': '중간수요 가공재 (계절조정)',
    'WPSID62': '중간수요 미가공재 (계절조정)',
    'WPSID63': '중간수요 서비스 (계절조정)',
    'WPSID54': '4단계 중간수요 (계절조정)',
    'WPSID53': '3단계 중간수요 (계절조정)',
    'WPSID52': '2단계 중간수요 (계절조정)',
    'WPSID51': '1단계 중간수요 (계절조정)',
    
    # Specific Commodities (주요 품목) - 계절조정
    'WPS1411': '자동차 (계절조정)',
    'WPS0638': '의약품 (계절조정)',
    'WPS0571': '가솔린 (계절조정)',
    'WPS0221': '육류 (계절조정)',
    'WPS061': '산업화학 (계절조정)',
    'WPS081': '목재 (계절조정)',
    'WPS1017': '제철 제품 (계절조정)',
    'WPS057303': '디젤연료 (계절조정)',
    'WPS029': '사료 (계절조정)',
    'WPS0561': '원유 (계절조정)',
    'WPS012': '곡물 (계절조정)',
    'WPS101211': '탄소강 스크랩 (계절조정)',
    
    # Services (서비스) - 계절조정
    'WPS5111': '외래 의료서비스 (계절조정)',
    'WPS5121': '입원 의료서비스 (계절조정)',
    'WPS5811': '식품·주류 소매 (계절조정)',
    'WPS5831': '의류·보석 소매 (계절조정)',
    'WPS3022': '항공 승객 서비스 (계절조정)',
    'WPS4011': '증권중개·투자 관련 (계절조정)',
    'WPS3911': '기업 대출(부분) (계절조정)',
    'WPS4511': '법률 서비스 (계절조정)',
    'WPS301': '화물 트럭 운송 (계절조정)',
    'WPS057': '기계·장비 도매 (계절조정)',
    
    # All Commodities (전체 상품) - 계절조정
    'WPSSOP3000': '전체 상품',
    'WPS03THRU15': '산업 상품 (계절조정)'
}

# PPI 카테고리 분류
PPI_CATEGORIES = {
    '최종수요_계절조정': {
        '최종수요 전체': ['WPSFD4'],
        '최종수요 재화': ['WPSFD41', 'WPSFD411', 'WPSFD412'],
        '최종수요 서비스': ['WPSFD42', 'WPSFD422', 'WPSFD423', 'WPSFD421'],
        '최종수요 건설': ['WPSFD43'],
        '최종수요 코어': ['WPSFD49104', 'WPSFD49116']
    },
    '최종수요': {
        '최종수요 전체': ['WPUFD4'],
        '최종수요 재화': ['WPUFD41', 'WPUFD411', 'WPUFD412'],
        '최종수요 서비스': ['WPUFD42', 'WPUFD422', 'WPUFD423', 'WPUFD421'],
        '최종수요 건설': ['WPUFD43'],
        '최종수요 코어': ['WPUFD49104', 'WPUFD49116']
    },
    '중간수요_계절조정': {
        '중간수요 가공재': ['WPSID61'],
        '중간수요 미가공재': ['WPSID62'],
        '중간수요 서비스': ['WPSID63'],
        '중간수요 단계별': ['WPSID54', 'WPSID53', 'WPSID52', 'WPSID51']
    },
    '주요품목_계절조정': {
        '에너지 관련': ['WPS0571', 'WPS057303', 'WPS0561'],
        '제조업': ['WPS1411', 'WPS0638', 'WPS061', 'WPS081', 'WPS1017'],
        '식품 농업': ['WPS0221', 'WPS029', 'WPS012', 'WPS101211']
    },
    '서비스_계절조정': {
        '의료서비스': ['WPS5111', 'WPS5121'],
        '비즈니스서비스': ['WPS4011', 'WPS3911', 'WPS4511'],
        '운송서비스': ['WPS3022', 'WPS301'],
        '소매서비스': ['WPS5811', 'WPS5831', 'WPS057']
    }
}

# %%
# === 전역 변수 ===
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/ppi_data.csv'
PPI_DATA = {}

# %%
# === 데이터 로드 함수 ===
def load_ppi_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """통합 함수 사용한 PPI 데이터 로드"""
    global PPI_DATA

    # 시리즈 딕셔너리를 {id: id} 형태로 변환 (load_economic_data가 예상하는 형태)
    series_dict = {series_id: series_id for series_id in PPI_SERIES.keys()}

    result = load_economic_data(
        series_dict=series_dict,
        data_source='BLS',
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
        tolerance=10.0  # PPI 데이터 허용 오차
    )

    if result:
        PPI_DATA = result
        print_load_info()
        return True
    else:
        print("❌ PPI 데이터 로드 실패")
        return False

def print_load_info():
    """PPI 데이터 로드 정보 출력"""
    if not PPI_DATA or 'load_info' not in PPI_DATA:
        print("⚠️ 로드된 데이터가 없습니다.")
        return
    
    info = PPI_DATA['load_info']
    print(f"\n📊 PPI 데이터 로드 완료!")
    print(f"   📅 로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S') if info['load_time'] else 'N/A'}")
    print(f"   📈 시리즈 개수: {info['series_count']}개")
    print(f"   📊 데이터 포인트: {info['data_points']}개")
    print(f"   🎯 시작 날짜: {info['start_date']}")
    print(f"   🔗 데이터 소스: {info['source']}")
    
    if 'raw_data' in PPI_DATA and not PPI_DATA['raw_data'].empty:
        latest_date = PPI_DATA['raw_data'].index[-1].strftime('%Y-%m-%d')
        print(f"   📅 최신 데이터: {latest_date}")

# %%
# === 범용 시각화 함수 ===
def plot_ppi_series_advanced(series_list, chart_type='multi_line', 
                             data_type='mom', periods=None, target_date=None):
    """범용 PPI 시각화 함수 - plot_economic_series 활용"""
    if not PPI_DATA:
        print("⚠️ 먼저 load_ppi_data()를 실행하세요.")
        return None

    return plot_economic_series(
        data_dict=PPI_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=PPI_KOREAN_NAMES
    )

# %%
# === 데이터 Export 함수 ===
def export_ppi_data(series_list, data_type='mom', periods=None, 
                    target_date=None, export_path=None, file_format='excel'):
    """PPI 데이터 export 함수 - export_economic_data 활용"""
    if not PPI_DATA:
        print("⚠️ 먼저 load_ppi_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=PPI_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=PPI_KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )

# %%
# === 데이터 접근 함수들 ===

def clear_ppi_data():
    """PPI 데이터 초기화"""
    global PPI_DATA
    PPI_DATA = {}
    print("🗑️ PPI 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not PPI_DATA or 'raw_data' not in PPI_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_ppi_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return PPI_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in PPI_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return PPI_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not PPI_DATA or 'mom_data' not in PPI_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_ppi_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return PPI_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in PPI_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return PPI_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not PPI_DATA or 'yoy_data' not in PPI_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_ppi_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return PPI_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in PPI_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return PPI_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not PPI_DATA or 'raw_data' not in PPI_DATA:
        return []
    return list(PPI_DATA['raw_data'].columns)

# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 PPI 시리즈 표시"""
    print("=== 사용 가능한 PPI 시리즈 ===")
    
    for series_id, description in PPI_SERIES.items():
        korean_name = PPI_KOREAN_NAMES.get(series_id, description)
        print(f"  '{series_id}': {korean_name} ({description})")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, groups in PPI_CATEGORIES.items():
        print(f"\n{category}:")
        for group_name, series_list in groups.items():
            print(f"  {group_name}: {len(series_list)}개 시리즈")
            for series_id in series_list:
                korean_name = PPI_KOREAN_NAMES.get(series_id, series_id)
                print(f"    - {series_id}: {korean_name}")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not PPI_DATA or 'load_info' not in PPI_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': PPI_DATA['load_info']['loaded'],
        'series_count': PPI_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': PPI_DATA['load_info']
    }

# %%
# === 사용 예시 ===

print("=== 리팩토링된 PPI 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_ppi_data()  # 스마트 업데이트")
print("   load_ppi_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!):")
print("   plot_ppi_series_advanced(['WPSFD4', 'WPSFD49104'], 'multi_line', 'mom')")
print("   plot_ppi_series_advanced(['WPSFD4'], 'horizontal_bar', 'yoy')")
print("   plot_ppi_series_advanced(['WPSFD4'], 'single_line', 'mom', periods=24)")
print()
print("3. 🔥 데이터 Export:")
print("   export_ppi_data(['WPSFD4', 'WPSFD49104'], 'mom')")
print("   export_ppi_data(['WPSFD4'], 'raw', periods=24, file_format='csv')")
print("   export_ppi_data(['WPSFD4'], 'yoy', target_date='2024-06-01')")
print()
print("✅ plot_ppi_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
print("✅ export_ppi_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수 사용!")

# %%
load_ppi_data()
plot_ppi_series_advanced(['WPSFD4', 'WPSFD49104'], 'multi_line', 'mom')
plot_ppi_series_advanced(['WPSFD4'], 'horizontal_bar', 'yoy')