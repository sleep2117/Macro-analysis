# %%
"""
미국 주택 가격 및 판매 데이터 분석 (리팩토링 버전)
- us_eco_utils를 사용한 통합 구조
- 5개 카테고리별 스마트 업데이트 지원
- Case-Shiller, FHFA, Zillow, 기존주택판매, 신규주택판매 데이터
- KPDS 포맷 시각화 지원
"""

import pandas as pd
import warnings
warnings.filterwarnings('ignore')

# 통합 유틸리티 함수 불러오기
from us_eco_utils import *

# %%
# === FRED API 키 설정 ===
api_config.FRED_API_KEY = 'f4bd434811e42e42287a0e5ccf400fff'

print("✓ KPDS 시각화 포맷 로드됨")
print("✓ US Economic Data Utils 로드됨")

# %%
# === 주택 가격 및 판매 시리즈 정의 ===

# 기존 주택 판매 관련 시리즈 (NAR - Existing Home Sales)
EXISTING_HOME_SALES_SERIES = {
    # National Level - Sales Volume
    'ehs_sales_national_sa': 'EXHOSLUSM495S',        # 전체 기존 주택 판매량 (SA)
    'ehs_inventory_national': 'HOSINVUSM495N',       # 판매 가능 재고 (NSA)
    'ehs_months_supply': 'HOSSUPUSM673N',            # 재고 소진 개월수 (NSA)
    'ehs_sf_sales_national_sa': 'EXSFHSUSM495S',     # 단독주택 판매량 (SA)
    'ehs_sf_inventory_national': 'HSFINVUSM495N',    # 단독주택 재고 (SA)
    'ehs_sf_months_supply': 'HSFSUPUSM673N',         # 단독주택 재고 소진 개월수 (NSA)
    
    # Regional Level - Sales Volume
    'ehs_sales_northeast_sa': 'EXHOSLUSNEM495S',     # 동북부 판매량 (SA)
    'ehs_sales_midwest_sa': 'EXHOSLUSMWM495S',       # 중서부 판매량 (SA)
    'ehs_sales_south_sa': 'EXHOSLUSSOM495S',         # 남부 판매량 (SA)
    'ehs_sales_west_sa': 'EXHOSLUSWTM495S',          # 서부 판매량 (SA)
    
    'ehs_sf_sales_northeast_sa': 'EXSFHSUSNEM495S',  # 동북부 단독주택 판매량 (SA)
    'ehs_sf_sales_midwest_sa': 'EXSFHSUSMWM495S',    # 중서부 단독주택 판매량 (SA)
    'ehs_sf_sales_south_sa': 'EXSFHSUSSOM495S',      # 남부 단독주택 판매량 (SA)
    'ehs_sf_sales_west_sa': 'EXSFHSUSWTM495S',       # 서부 단독주택 판매량 (SA)
}

# 신규 주택 판매 관련 시리즈 (Census & HUD - New Residential Sales)
NEW_RESIDENTIAL_SALES_SERIES = {
    # National Level - Core Sales and Inventory
    'nrs_sales_national_sa': 'HSN1F',               # 신규 단독주택 판매량 (SAAR)
    'nrs_sales_national_nsa': 'HSN1FNSA',           # 신규 단독주택 판매량 (NSA)
    'nrs_inventory_national_sa': 'HNFSEPUSSA',      # 신규 주택 재고 (SA)
    'nrs_inventory_national_nsa': 'HNFSUSNSA',      # 신규 주택 재고 (NSA)
    'nrs_months_supply_sa': 'MSACSR',               # 재고 소진 개월수 (SA)
    'nrs_months_supply_nsa': 'MSACSRNSA',           # 재고 소진 개월수 (NSA)
    
    # Regional Level - Sales
    'nrs_sales_northeast_sa': 'HSN1FNE',            # 동북부 판매량 (SA)
    'nrs_sales_midwest_sa': 'HSN1FMW',              # 중서부 판매량 (SA)
    'nrs_sales_south_sa': 'HSN1FS',                 # 남부 판매량 (SA)
    'nrs_sales_west_sa': 'HSN1FW',                  # 서부 판매량 (SA)
    
    'nrs_sales_northeast_nsa': 'HSN1FNENSA',        # 동북부 판매량 (NSA)
    'nrs_sales_midwest_nsa': 'HSN1FMWNSA',          # 중서부 판매량 (NSA)
    'nrs_sales_south_nsa': 'HSN1FSNSA',             # 남부 판매량 (NSA)
    'nrs_sales_west_nsa': 'HSN1FWNSA',              # 서부 판매량 (NSA)
    
    # Regional Level - Inventory
    'nrs_inventory_northeast': 'HNFSNE',            # 동북부 재고
    'nrs_inventory_midwest': 'HNFSMW',              # 중서부 재고
    'nrs_inventory_south': 'HNFSS',                 # 남부 재고
    'nrs_inventory_west': 'HNFSW',                  # 서부 재고
    
    # Sales by Stage of Construction
    'nrs_sales_total_stage': 'NHSDPTS',             # 전체 (단계별)
    'nrs_sales_completed': 'NHSDPCS',               # 완공
    'nrs_sales_under_construction': 'NHSDPUCS',     # 건설중
    'nrs_sales_not_started': 'NHSDPNSS',            # 미착공
    
    # Inventory by Stage of Construction
    'nrs_inventory_total_stage': 'NHFSEPTS',        # 전체 재고 (단계별)
    'nrs_inventory_completed_stage': 'NHFSEPCS',    # 완공 재고
    'nrs_inventory_under_construction_stage': 'NHFSEPUCS', # 건설중 재고
    'nrs_inventory_not_started_stage': 'NHFSEPNTS', # 미착공 재고
    
    # Sales by Type of Financing (Quarterly)
    'nrs_sales_cash': 'HSTFC',                      # 현금 구매
    'nrs_sales_conventional': 'HSTFCM',             # 일반 융자
    'nrs_sales_fha': 'HSTFFHAI',                    # FHA 융자
    'nrs_sales_va': 'HSTFVAG',                      # VA 융자
    
    # Other Indicators
    'nrs_median_months_on_market': 'MNMFS',         # 시장 체류 기간 (중간값)
}

# 데이터 유형별로 분류
HOUSE_PRICE_DATA_CATEGORIES = {
    'existing_home_sales': EXISTING_HOME_SALES_SERIES,
    'new_residential_sales': NEW_RESIDENTIAL_SALES_SERIES
}

# 전체 시리즈 통합
ALL_HOUSE_PRICE_SERIES = {
    **EXISTING_HOME_SALES_SERIES,
    **NEW_RESIDENTIAL_SALES_SERIES
}

# 한국어 이름 매핑 (실제 컬럼명 기반 - category_indicator 형태)
HOUSE_PRICE_KOREAN_NAMES = {
    # Existing Home Sales (기존 주택 판매)
    'existing_home_sales_ehs_sales_national_sa': 'EHS 전국 판매량(SA)',
    'existing_home_sales_ehs_inventory_national': 'EHS 전국 재고',
    'existing_home_sales_ehs_months_supply': 'EHS 재고 소진율',
    'existing_home_sales_ehs_sf_sales_national_sa': 'EHS 단독주택 판매량(SA)',
    'existing_home_sales_ehs_sf_inventory_national': 'EHS 단독주택 재고',
    'existing_home_sales_ehs_sf_months_supply': 'EHS 단독주택 소진율',
    
    'existing_home_sales_ehs_sales_northeast_sa': 'EHS 동북부 판매량(SA)',
    'existing_home_sales_ehs_sales_midwest_sa': 'EHS 중서부 판매량(SA)',
    'existing_home_sales_ehs_sales_south_sa': 'EHS 남부 판매량(SA)',
    'existing_home_sales_ehs_sales_west_sa': 'EHS 서부 판매량(SA)',
    
    'existing_home_sales_ehs_sf_sales_northeast_sa': 'EHS 동북부 단독주택(SA)',
    'existing_home_sales_ehs_sf_sales_midwest_sa': 'EHS 중서부 단독주택(SA)',
    'existing_home_sales_ehs_sf_sales_south_sa': 'EHS 남부 단독주택(SA)',
    'existing_home_sales_ehs_sf_sales_west_sa': 'EHS 서부 단독주택(SA)',
    
    # New Residential Sales (신규 주택 판매)
    'new_residential_sales_nrs_sales_national_sa': 'NRS 전국 판매량(SA)',
    'new_residential_sales_nrs_sales_national_nsa': 'NRS 전국 판매량(NSA)',
    'new_residential_sales_nrs_inventory_national_sa': 'NRS 전국 재고(SA)',
    'new_residential_sales_nrs_inventory_national_nsa': 'NRS 전국 재고(NSA)',
    'new_residential_sales_nrs_months_supply_sa': 'NRS 재고 소진율(SA)',
    'new_residential_sales_nrs_months_supply_nsa': 'NRS 재고 소진율(NSA)',
    
    'new_residential_sales_nrs_sales_northeast_sa': 'NRS 동북부 판매량(SA)',
    'new_residential_sales_nrs_sales_midwest_sa': 'NRS 중서부 판매량(SA)',
    'new_residential_sales_nrs_sales_south_sa': 'NRS 남부 판매량(SA)',
    'new_residential_sales_nrs_sales_west_sa': 'NRS 서부 판매량(SA)',
    
    'new_residential_sales_nrs_sales_northeast_nsa': 'NRS 동북부 판매량(NSA)',
    'new_residential_sales_nrs_sales_midwest_nsa': 'NRS 중서부 판매량(NSA)',
    'new_residential_sales_nrs_sales_south_nsa': 'NRS 남부 판매량(NSA)',
    'new_residential_sales_nrs_sales_west_nsa': 'NRS 서부 판매량(NSA)',
    
    'new_residential_sales_nrs_inventory_northeast': 'NRS 동북부 재고',
    'new_residential_sales_nrs_inventory_midwest': 'NRS 중서부 재고',
    'new_residential_sales_nrs_inventory_south': 'NRS 남부 재고',
    'new_residential_sales_nrs_inventory_west': 'NRS 서부 재고',
    
    'new_residential_sales_nrs_sales_total_stage': 'NRS 전체 단계별',
    'new_residential_sales_nrs_sales_completed': 'NRS 완공',
    'new_residential_sales_nrs_sales_under_construction': 'NRS 건설중',
    'new_residential_sales_nrs_sales_not_started': 'NRS 미착공',
    
    'new_residential_sales_nrs_inventory_total_stage': 'NRS 전체 재고 단계별',
    'new_residential_sales_nrs_inventory_completed_stage': 'NRS 완공 재고',
    'new_residential_sales_nrs_inventory_under_construction_stage': 'NRS 건설중 재고',
    'new_residential_sales_nrs_inventory_not_started_stage': 'NRS 미착공 재고',
    
    'new_residential_sales_nrs_sales_cash': 'NRS 현금구매',
    'new_residential_sales_nrs_sales_conventional': 'NRS 일반융자',
    'new_residential_sales_nrs_sales_fha': 'NRS FHA융자',
    'new_residential_sales_nrs_sales_va': 'NRS VA융자',
    
    'new_residential_sales_nrs_median_months_on_market': 'NRS 시장체류기간'
}

# %%
# === 전역 변수 ===
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/house_sales_data_refactored.csv'
HOUSE_PRICE_DATA = {
    'raw_data': pd.DataFrame(),          # 원본 데이터
    'mom_data': pd.DataFrame(),          # 전월대비 변화
    'yoy_data': pd.DataFrame(),          # 전년동월대비 변화
    'load_info': {
        'loaded': False,
        'load_time': None,
        'start_date': None,
        'series_count': 0,
        'data_points': 0,
        'categories_loaded': []
    }
}

# %%
# === 그룹별 스마트 업데이트를 위한 시리즈 그룹 정의 ===

def build_house_price_series_groups(enabled_categories=None):
    """
    주택 가격/판매 데이터 그룹화된 시리즈 딕셔너리 생성 (us_eco_utils 호환)
    
    Args:
        enabled_categories: 사용할 카테고리 리스트 (None이면 모든 카테고리)
    
    Returns:
        dict: {group_name: {series_name: series_id}} 형태의 그룹 딕셔너리
    """
    if enabled_categories is None:
        enabled_categories = list(HOUSE_PRICE_DATA_CATEGORIES.keys())
    
    series_groups = {}
    
    for category_name in enabled_categories:
        if category_name not in HOUSE_PRICE_DATA_CATEGORIES:
            continue
            
        category_series = HOUSE_PRICE_DATA_CATEGORIES[category_name]
        
        # 각 카테고리를 그룹으로 생성
        group_name = category_name
        
        # 시리즈명을 카테고리_지표명 형태로 변환
        group_series = {}
        for indicator_name, fred_id in category_series.items():
            series_name = f"{category_name}_{indicator_name}"
            group_series[series_name] = fred_id
        
        series_groups[group_name] = group_series
    
    return series_groups

# %%
# === 데이터 로드 함수 ===

def load_house_price_data(start_date='2020-01-01', force_reload=False, smart_update=True, enabled_categories=None):
    """
    모든 주택 가격/판매 데이터 로드 (그룹별 스마트 업데이트 지원)
    
    Args:
        start_date: 시작 날짜
        force_reload: 강제 재로드 여부
        smart_update: 스마트 업데이트 사용 여부 (기본값: True)
        enabled_categories: 수집할 카테고리 리스트
    
    Returns:
        bool: 로드 성공 여부
    """
    global HOUSE_PRICE_DATA
    
    print("🚀 주택 가격/판매 데이터 로딩 시작 (그룹별 스마트 업데이트)")
    print("="*60)
    
    # 이미 로드된 경우 스킵 (강제 재로드가 아닌 경우)
    if HOUSE_PRICE_DATA['load_info']['loaded'] and not force_reload and not smart_update:
        print("💾 이미 로드된 데이터 사용 중")
        print_load_info()
        return True
    
    try:
        # 카테고리별 시리즈 그룹 생성
        series_groups = build_house_price_series_groups(enabled_categories)
        
        print(f"📋 생성된 그룹:")
        for group_name, group_series in series_groups.items():
            print(f"   {group_name}: {len(group_series)}개 시리즈")
        
        # us_eco_utils의 그룹별 로드 함수 사용
        result = load_economic_data_grouped(
            series_groups=series_groups,
            data_source='FRED',
            csv_file_path=CSV_FILE_PATH,
            start_date=start_date,
            smart_update=smart_update,
            force_reload=force_reload,
            tolerance=10.0  # 주택 가격 지수용 허용 오차
        )
        
        if result is None:
            print("❌ 데이터 로딩 실패")
            return False
        
        # 전역 저장소에 결과 저장
        raw_data = result['raw_data']
        
        if raw_data.empty or len(raw_data.columns) < 3:
            print(f"❌ 로드된 시리즈가 너무 적습니다: {len(raw_data.columns)}개")
            return False
        
        # 전역 저장소 업데이트 (fed_pmi와 동일한 구조)
        HOUSE_PRICE_DATA['raw_data'] = raw_data
        HOUSE_PRICE_DATA['mom_data'] = result['mom_data']
        HOUSE_PRICE_DATA['yoy_data'] = result['yoy_data']
        
        # 로드 정보 업데이트 (그룹별 정보 추가)
        load_info = result['load_info']
        
        # 카테고리 이름으로 변환
        categories_loaded = []
        groups_checked = load_info.get('groups_checked', [])
        for group_name in groups_checked:
            if group_name not in categories_loaded:
                categories_loaded.append(group_name)
        
        HOUSE_PRICE_DATA['load_info'] = load_info
        HOUSE_PRICE_DATA['load_info']['categories_loaded'] = categories_loaded
        
        # CSV 저장 (그룹별 업데이트인 경우 이미 저장됨)
        if 'CSV' not in load_info.get('source', ''):
            # us_eco_utils의 save_data_to_csv 함수 사용
            save_data_to_csv(raw_data, CSV_FILE_PATH)
        
        print("\\n✅ 주택 가격/판매 데이터 로딩 완료!")
        print_load_info()
        
        # 그룹별 업데이트 결과 요약
        if 'groups_updated' in load_info and load_info['groups_updated']:
            print(f"\\n📝 업데이트된 그룹:")
            for group in load_info['groups_updated']:
                category_display = group.replace('_', ' ').title()
                print(f"   {category_display}")
        elif 'groups_checked' in load_info:
            print(f"\\n✅ 모든 그룹 데이터 일치 (업데이트 불필요)")
        
        return True
        
    except Exception as e:
        print(f"❌ 데이터 로딩 실패: {e}")
        import traceback
        print("상세 오류:")
        print(traceback.format_exc())
        return False

def print_load_info():
    """로드 정보 출력"""
    if not HOUSE_PRICE_DATA or 'load_info' not in HOUSE_PRICE_DATA:
        print("❌ 데이터가 로드되지 않음")
        return
        
    info = HOUSE_PRICE_DATA['load_info']
    print(f"📊 로드된 데이터 정보:")
    print(f"   시리즈 개수: {info['series_count']}")
    print(f"   데이터 포인트: {info['data_points']}")
    print(f"   시작 날짜: {info['start_date']}")
    print(f"   로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   데이터 소스: {info.get('source', 'API')}")
    
    if info.get('categories_loaded'):
        categories_display = [cat.replace('_', ' ').title() for cat in info['categories_loaded']]
        print(f"   포함된 카테고리: {', '.join(categories_display)}")
    
    if not HOUSE_PRICE_DATA['raw_data'].empty:
        date_range = f"{HOUSE_PRICE_DATA['raw_data'].index[0].strftime('%Y-%m')} ~ {HOUSE_PRICE_DATA['raw_data'].index[-1].strftime('%Y-%m')}"
        print(f"   데이터 기간: {date_range}")

# %%
# === 범용 시각화 함수 ===
def plot_house_price_series_advanced(series_list, chart_type='multi_line', 
                                      data_type='raw', periods=None, target_date=None):
    """범용 주택 가격/판매 시각화 함수 - plot_economic_series 활용"""
    if not HOUSE_PRICE_DATA:
        print("⚠️ 먼저 load_house_price_data()를 실행하세요.")
        return None

    return plot_economic_series(
        data_dict=HOUSE_PRICE_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=HOUSE_PRICE_KOREAN_NAMES
    )

# %%
# === 데이터 Export 함수 ===
def export_house_price_data(series_list, data_type='raw', periods=None, 
                           target_date=None, export_path=None, file_format='excel'):
    """주택 가격/판매 데이터 export 함수 - export_economic_data 활용"""
    if not HOUSE_PRICE_DATA:
        print("⚠️ 먼저 load_house_price_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=HOUSE_PRICE_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=HOUSE_PRICE_KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )

# %%
# === 데이터 접근 함수들 ===

def clear_house_price_data():
    """주택 가격/판매 데이터 초기화"""
    global HOUSE_PRICE_DATA
    HOUSE_PRICE_DATA = {}
    print("🗑️ 주택 가격/판매 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not HOUSE_PRICE_DATA or 'raw_data' not in HOUSE_PRICE_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_house_price_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return HOUSE_PRICE_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in HOUSE_PRICE_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return HOUSE_PRICE_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not HOUSE_PRICE_DATA or 'mom_data' not in HOUSE_PRICE_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_house_price_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return HOUSE_PRICE_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in HOUSE_PRICE_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return HOUSE_PRICE_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not HOUSE_PRICE_DATA or 'yoy_data' not in HOUSE_PRICE_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_house_price_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return HOUSE_PRICE_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in HOUSE_PRICE_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return HOUSE_PRICE_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not HOUSE_PRICE_DATA or 'raw_data' not in HOUSE_PRICE_DATA:
        return []
    return list(HOUSE_PRICE_DATA['raw_data'].columns)

# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 주택 가격/판매 시리즈 표시"""
    if not HOUSE_PRICE_DATA or 'raw_data' not in HOUSE_PRICE_DATA:
        print("⚠️ 먼저 load_house_price_data()를 실행하여 데이터를 로드하세요.")
        return
    
    print("=== 사용 가능한 주택 가격/판매 시리즈 ===")
    print("="*60)
    
    all_columns = HOUSE_PRICE_DATA['raw_data'].columns.tolist()
    
    # 카테고리별로 그룹화
    category_groups = {
        'case_shiller': [],
        'fhfa': [],
        'zillow': [],
        'existing_home_sales': [],
        'new_residential_sales': []
    }
    
    # FRED 시리즈 ID를 카테고리별로 분류
    for col in all_columns:
        if col in CASE_SHILLER_SERIES.values():
            category_groups['case_shiller'].append(col)
        elif col in FHFA_SERIES.values():
            category_groups['fhfa'].append(col)
        elif col in ZILLOW_SERIES.values():
            category_groups['zillow'].append(col)
        elif col in EXISTING_HOME_SALES_SERIES.values():
            category_groups['existing_home_sales'].append(col)
        elif col in NEW_RESIDENTIAL_SALES_SERIES.values():
            category_groups['new_residential_sales'].append(col)
    
    # 카테고리별 출력
    category_names = {
        'case_shiller': 'Case-Shiller 지수',
        'fhfa': 'FHFA 지수',
        'zillow': 'Zillow 지수',
        'existing_home_sales': '기존주택 판매',
        'new_residential_sales': '신규주택 판매'
    }
    
    for category_key, category_name in category_names.items():
        if category_groups[category_key]:
            print(f"\\n🏠 {category_name} ({len(category_groups[category_key])}개 시리즈)")
            print("-" * 40)
            for series in category_groups[category_key][:5]:  # 처음 5개만 표시
                korean_name = HOUSE_PRICE_KOREAN_NAMES.get(series, series)
                print(f"  • {series}")
                print(f"    → {korean_name}")
            if len(category_groups[category_key]) > 5:
                print(f"  ... 외 {len(category_groups[category_key])-5}개 더")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, series_dict in HOUSE_PRICE_DATA_CATEGORIES.items():
        category_display = category.replace('_', ' ').title()
        print(f"\\n{category_display}:")
        print(f"  시리즈 개수: {len(series_dict)}개")
        # 샘플 시리즈 몇 개 표시
        sample_series = list(series_dict.values())[:3]
        for fred_id in sample_series:
            korean_name = HOUSE_PRICE_KOREAN_NAMES.get(fred_id, fred_id)
            print(f"    - {fred_id}: {korean_name}")
        if len(series_dict) > 3:
            print(f"    ... 외 {len(series_dict)-3}개 더")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not HOUSE_PRICE_DATA or 'load_info' not in HOUSE_PRICE_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': HOUSE_PRICE_DATA['load_info']['loaded'],
        'series_count': HOUSE_PRICE_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': HOUSE_PRICE_DATA['load_info']
    }
# %%
# === 사용 예시 ===

print("=== 리팩토링된 주택 가격/판매 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_house_price_data()  # 그룹별 스마트 업데이트")
print("   load_house_price_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!):")
print("   plot_house_price_series_advanced(['CSUSHPISA', 'HPIPONM226S'], 'multi_line', 'raw')")
print("   plot_house_price_series_advanced(['EXHOSLUSM495S'], 'horizontal_bar', 'mom')")
print("   plot_house_price_series_advanced(['SPCS20RSA'], 'single_line', 'yoy', periods=24)")
print()
print("3. 🔥 데이터 Export:")
print("   export_house_price_data(['CSUSHPISA', 'HPIPONM226S'], 'raw')")
print("   export_house_price_data(['EXHOSLUSM495S'], 'mom', periods=24, file_format='csv')")
print("   export_house_price_data(['SPCS20RSA'], 'yoy', target_date='2024-06-01')")
print()
print("4. 📋 데이터 확인:")
print("   show_available_series()  # 사용 가능한 모든 시리즈 목록")
print("   show_category_options()  # 카테고리별 옵션")
print("   get_raw_data()  # 원본 지수 데이터")
print("   get_mom_data()  # 전월대비 변화 데이터")
print("   get_yoy_data()  # 전년동월대비 변화 데이터")
print("   get_data_status()  # 현재 데이터 상태")
print()
print("✅ plot_house_price_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
print("✅ export_house_price_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수 사용!")
print()
print("🔥 그룹별 스마트 업데이트 특징:")
print("   • Case-Shiller만 새 데이터가 있으면 Case-Shiller만 업데이트")
print("   • FHFA만 새 데이터가 있으면 FHFA만 업데이트")
print("   • 전체 재로드 없이 부분 업데이트로 효율성 극대화")
print("   • 각 데이터 소스의 발표 일정에 맞춰 개별 스마트 업데이트")
print()
print("🏠 지원되는 카테고리:")
for category, series_dict in HOUSE_PRICE_DATA_CATEGORIES.items():
    category_display = category.replace('_', ' ').title()
    print(f"   • {category_display}: {len(series_dict)}개 시리즈")
print()
print("📅 데이터 발표 일정:")
print("   • Case-Shiller: 매월 마지막 화요일")
print("   • FHFA: 매월 25일경")
print("   • Zillow: 매월 중순")
print("   • 기존주택 판매: 매월 20일경")
print("   • 신규주택 판매: 매월 17일경")
print()
print("🎯 최적화된 워크플로:")
print("   1. 매일 load_house_price_data() 실행")
print("   2. 새로운 데이터가 있는 카테고리만 자동 업데이트")
print("   3. plot_house_price_series_advanced()로 시각화")
print("   4. export_house_price_data()로 데이터 내보내기")
print("   5. 효율적이고 빠른 데이터 관리!")

# %%
# 테스트 실행
print("테스트: 주택 가격 데이터 로딩...")
result = load_house_price_data()
if result:
    print("\\n테스트: 기본 시각화...")
    plot_house_price_series_advanced(['fhfa_national_sa', 'fhfa_national_nsa'], 'multi_line', 'raw')