# %%
"""
FRED API 전용 US GDP 분석 및 시각화 도구
- FRED API를 사용하여 US GDP 관련 데이터 수집
- GDP 구성 요소별 데이터 분류 및 분석
- QoQ/YoY 기준 시각화 지원
- 스마트 업데이트 기능 (실행 시마다 최신 데이터 확인 및 업데이트)
- CSV 파일 자동 저장 및 업데이트
"""
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import sys
import warnings
import os
warnings.filterwarnings('ignore')

# 필수 라이브러리들
try:
    import requests
    import json
    FRED_API_AVAILABLE = True
    print("✓ FRED API 연동 가능 (requests 라이브러리)")
except ImportError:
    print("⚠️ requests 라이브러리가 없습니다. 설치하세요: pip install requests")
    FRED_API_AVAILABLE = False

# FRED API 키 설정 (여기에 실제 API 키를 입력하세요)
FRED_API_KEY = 'f4bd434811e42e42287a0e5ccf400fff'  # https://fred.stlouisfed.org/docs/api/api_key.html 에서 발급

# KPDS 시각화 라이브러리 불러오기 (필수)
sys.path.append('/home/jyp0615')
from kpds_fig_format_enhanced import *

print("✓ KPDS 시각화 포맷 로드됨")

# %%
# === GDP 데이터 계층 구조 정의 ===

# GDP 주요 구성 요소 시리즈 맵 - QoQ 변화율 (RL1Q225SBEA: 전분기대비 연율화 변화율)
GDP_MAIN_QOQ_SERIES = {
    'gdp': 'A191RL1Q225SBEA',  # Gross domestic product (quarterly change rate)
    'consumption': 'DPCERL1Q225SBEA',  # Personal consumption expenditures (change rate)
    'investment': 'A006RL1Q225SBEA',  # Gross private domestic investment (change rate)
    'government': 'A822RL1Q225SBEA',  # Government consumption & investment (change rate)
    'exports': 'A020RL1Q158SBEA',  # Exports (change rate)
    'imports': 'A021RL1Q158SBEA'   # Imports (change rate)
}

# GDP 주요 구성 요소 시리즈 맵 - YoY 변화율 (직접 FRED에서 제공하는 YoY 시리즈)
GDP_MAIN_YOY_SERIES = {
    'gdp': 'A191RL1A225NBEA',  # Gross domestic product (YoY)
    'consumption': 'DPCERL1A225NBEA',  # Personal consumption expenditures (YoY)
    'investment': 'A006RL1A225NBEA',  # Gross private domestic investment (YoY)
    'government': 'A822RL1A225NBEA',  # Government consumption & investment (YoY)
    'exports': 'A020RL1A225NBEA',  # Exports (YoY)
    'imports': 'A021RL1A225NBEA'   # Imports (YoY)
}

# GDP 주요 구성 요소 시리즈 맵 - 기여도 (RY2Q224SBEA: GDP 성장 기여도 포인트)
# 주의: 수입은 GDP에서 차감되므로 기여도가 음수로 나오는 경우가 많음
GDP_MAIN_CONTRIB_SERIES = {
    'consumption': 'DPCERY2Q224SBEA',  # Personal consumption expenditures (contribution)
    'investment': 'A006RY2Q224SBEA',  # Gross private domestic investment (contribution) 
    'government': 'A822RY2Q224SBEA',  # Government consumption & investment (contribution)
    'net_exports': 'A019RY2Q224SBEA',  # Net exports (contribution) - 수출-수입 순기여도
    'exports': 'A020RY2Q224SBEA',  # Exports (contribution)
    'imports': 'A021RY2Q224SBEA'   # Imports (contribution) - 보통 음수
}

# GDP 소비 세부 구성 요소 - 변화율
GDP_CONSUMPTION_CHANGE_SERIES = {
    'goods': 'DGDSRL1Q225SBEA',  # Goods (change rate)
    'durable_goods': 'DDURRL1Q225SBEA',  # Durable goods (change rate)
    'nondurable_goods': 'DNDGRL1Q225SBEA',  # Nondurable goods (change rate)
    'services': 'DSERRL1Q225SBEA',  # Services (change rate)
    'motor_vehicles': 'DMOTRL1Q225SBEA',  # Motor vehicles & parts (change rate)
    'housing_utilities': 'DHUTRL1Q225SBEA',  # Housing & utilities (change rate)
    'health_care': 'DHLCRL1Q225SBEA',  # Health care (change rate)
    'food_services': 'DFSARL1Q225SBEA'  # Food services & accommodation (change rate)
}

# GDP 소비 세부 구성 요소 - 기여도
GDP_CONSUMPTION_CONTRIB_SERIES = {
    'goods': 'DGDSRY2Q224SBEA',  # Goods (contribution)
    'durable_goods': 'DDURRY2Q224SBEA',  # Durable goods (contribution)
    'nondurable_goods': 'DNDGRY2Q224SBEA',  # Nondurable goods (contribution)
    'services': 'DSERRY2Q224SBEA',  # Services (contribution)
    'motor_vehicles': 'DMOTRY2Q224SBEA',  # Motor vehicles & parts (contribution)
    'housing_utilities': 'DHUTRY2Q224SBEA',  # Housing & utilities (contribution)
    'health_care': 'DHLCRY2Q224SBEA',  # Health care (contribution)
    'food_services': 'DFSARY2Q224SBEA'  # Food services & accommodation (contribution)
}

# GDP 투자 세부 구성 요소 - 변화율
GDP_INVESTMENT_CHANGE_SERIES = {
    'fixed_investment': 'A007RL1Q225SBEA',  # Fixed investment (change rate)
    'nonresidential': 'A008RL1Q225SBEA',  # Non-residential (change rate)
    'residential': 'A011RL1Q225SBEA',  # Residential (change rate)
    'structures': 'A009RL1Q225SBEA',  # Structures (change rate)
    'equipment': 'Y033RL1Q225SBEA',  # Equipment (change rate)
    'intellectual_property': 'Y001RL1Q225SBEA',  # Intellectual property products (change rate)
    'software': 'B985RL1Q225SBEA',  # Software (change rate)
}

# GDP 투자 세부 구성 요소 - 기여도
GDP_INVESTMENT_CONTRIB_SERIES = {
    'fixed_investment': 'A007RY2Q224SBEA',  # Fixed investment (contribution)
    'nonresidential': 'A008RY2Q224SBEA',  # Non-residential (contribution)
    'residential': 'A011RY2Q224SBEA',  # Residential (contribution)
    'structures': 'A009RY2Q224SBEA',  # Structures (contribution)
    'equipment': 'Y033RY2Q224SBEA',  # Equipment (contribution)
    'intellectual_property': 'Y001RY2Q224SBEA',  # Intellectual property products (contribution)
    'software': 'B985RY2Q224SBEA',  # Software (contribution)
}

# GDP 정부 지출 세부 구성 요소 - 변화율
GDP_GOVERNMENT_CHANGE_SERIES = {
    'federal': 'A823RL1Q225SBEA',  # Federal (change rate)
    'state_local': 'A829RL1Q225SBEA',  # State & local (change rate)
    'defense': 'A824RL1Q225SBEA',  # National defense (change rate)
    'nondefense': 'A825RL1Q225SBEA',  # Non-defense (change rate)
    'defense_consumption': 'A997RL1Q225SBEA',  # Defense consumption (change rate)
    'defense_investment': 'A788RL1Q225SBEA'  # Defense investment (change rate)
}

# GDP 정부 지출 세부 구성 요소 - 기여도
GDP_GOVERNMENT_CONTRIB_SERIES = {
    'federal': 'A823RY2Q224SBEA',  # Federal (contribution)
    'state_local': 'A829RY2Q224SBEA',  # State & local (contribution)
    'defense': 'A824RY2Q224SBEA',  # National defense (contribution)
    'nondefense': 'A825RY2Q224SBEA',  # Non-defense (contribution)
    'defense_consumption': 'A997RY2Q224SBEA',  # Defense consumption (contribution)
    'defense_investment': 'A788RY2Q224SBEA'  # Defense investment (contribution)
}

# GDP 무역 세부 구성 요소 - 변화율
GDP_TRADE_CHANGE_SERIES = {
    'exports_goods': 'A253RL1Q225SBEA',  # Exports goods (change rate)
    'exports_services': 'A646RL1Q225SBEA',  # Exports services (change rate)
    'imports_goods': 'A255RL1Q225SBEA',  # Imports goods (change rate)
    'imports_services': 'A656RL1Q225SBEA'  # Imports services (change rate)
}

# GDP 무역 세부 구성 요소 - 기여도
GDP_TRADE_CONTRIB_SERIES = {
    'exports_goods': 'A253RY2Q224SBEA',  # Exports goods (contribution)
    'exports_services': 'A646RY2Q224SBEA',  # Exports services (contribution)
    'imports_goods': 'A255RY2Q224SBEA',  # Imports goods (contribution)
    'imports_services': 'A656RY2Q224SBEA'  # Imports services (contribution)
}

# 모든 GDP 시리즈 통합
ALL_GDP_QOQ_SERIES = {**GDP_MAIN_QOQ_SERIES, **GDP_CONSUMPTION_CHANGE_SERIES, **GDP_INVESTMENT_CHANGE_SERIES, **GDP_GOVERNMENT_CHANGE_SERIES, **GDP_TRADE_CHANGE_SERIES}
ALL_GDP_YOY_SERIES = {**GDP_MAIN_YOY_SERIES}  # 주요 구성요소만 YoY 제공
ALL_GDP_CONTRIB_SERIES = {**GDP_MAIN_CONTRIB_SERIES, **GDP_CONSUMPTION_CONTRIB_SERIES, **GDP_INVESTMENT_CONTRIB_SERIES, **GDP_GOVERNMENT_CONTRIB_SERIES, **GDP_TRADE_CONTRIB_SERIES}

# 한국어 이름 매핑
GDP_KOREAN_NAMES = {
    # 주요 구성 요소
    'gdp': 'GDP (실질)',
    'consumption': '개인소비',
    'investment': '민간투자',
    'government': '정부지출',
    'exports': '수출',
    'imports': '수입',
    'net_exports': '순수출(수출-수입)',
    
    # 소비 세부 항목
    'goods': '재화',
    'durable_goods': '내구재',
    'nondurable_goods': '비내구재',
    'services': '서비스',
    'motor_vehicles': '자동차 및 부품',
    'housing_utilities': '주택 및 유틸리티',
    'health_care': '의료',
    'food_services': '외식 및 숙박',
    
    # 투자 세부 항목
    'fixed_investment': '고정투자',
    'nonresidential': '비주거용',
    'residential': '주거용',
    'structures': '구조물',
    'equipment': '장비',
    'intellectual_property': '지적재산',
    'software': '소프트웨어',
    'rd': '연구개발',
    
    # 정부 지출 세부 항목
    'federal': '연방정부',
    'state_local': '주·지방정부',
    'defense': '국방',
    'nondefense': '비국방',
    'defense_consumption': '국방소비',
    'defense_investment': '국방투자',
    
    # 무역 세부 항목
    'exports_goods': '수출 재화',
    'exports_services': '수출 서비스',
    'imports_goods': '수입 재화',
    'imports_services': '수입 서비스'
}

# %%
# === 전역 데이터 저장소 ===

# FRED 세션
FRED_SESSION = None

# 전역 데이터 저장소
GDP_DATA = {
    'change_data': pd.DataFrame(),   # 변화율 데이터 (이미 계산된 QoQ 연율화 변화율, %)
    'contrib_data': pd.DataFrame(),  # 기여도 데이터 (GDP 성장 기여도, 포인트)
    'yoy_data': pd.DataFrame(),      # 전년동기대비 변화율 (변화율 데이터로부터 계산)
    'latest_values': {},             # 최신 값들
    'csv_data': pd.DataFrame(),      # CSV 저장용 통합 데이터
    'load_info': {
        'loaded': False,
        'load_time': None,
        'start_date': None,
        'series_count': 0,
        'data_points': 0,
        'csv_updated': False,
        'csv_file_path': '/home/jyp0615/us_eco/gdp_data.csv'
    }
}

# %%
# === FRED API 초기화 및 데이터 가져오기 함수 ===

def initialize_fred_api():
    """FRED API 세션 초기화"""
    global FRED_SESSION
    
    if not FRED_API_AVAILABLE:
        print("⚠️ FRED API 사용 불가 (requests 라이브러리 없음)")
        return False
    
    if not FRED_API_KEY or FRED_API_KEY == 'YOUR_FRED_API_KEY_HERE':
        print("⚠️ FRED API 키가 설정되지 않았습니다. FRED_API_KEY를 설정하세요.")
        print("  https://fred.stlouisfed.org/docs/api/api_key.html 에서 무료로 발급 가능합니다.")
        return False
    
    try:
        FRED_SESSION = requests.Session()
        print("✓ FRED API 세션 초기화 성공")
        return True
    except Exception as e:
        print(f"⚠️ FRED API 초기화 실패: {e}")
        return False

def get_fred_data(series_id, start_date='2000-01-01', end_date=None):
    """
    FRED API에서 데이터 가져오기
    
    Args:
        series_id: FRED 시리즈 ID
        start_date: 시작 날짜
        end_date: 종료 날짜 (None이면 현재)
    
    Returns:
        pandas.Series: 날짜를 인덱스로 하는 시리즈 데이터
    """
    if not FRED_API_AVAILABLE or FRED_SESSION is None:
        print(f"❌ FRED API 사용 불가 - {series_id}")
        return None
    
    if end_date is None:
        end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    
    # FRED API URL 구성
    url = f'https://api.stlouisfed.org/fred/series/observations'
    params = {
        'series_id': series_id,
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'observation_start': start_date,
        'observation_end': end_date,
        'sort_order': 'asc'
    }
    
    try:
        print(f"📊 FRED에서 로딩: {series_id}")
        response = FRED_SESSION.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if 'observations' in data:
            observations = data['observations']
            
            # 데이터 정리
            dates = []
            values = []
            
            for obs in observations:
                try:
                    date = pd.to_datetime(obs['date'])
                    value = float(obs['value'])
                    
                    dates.append(date)
                    values.append(value)
                except (ValueError, KeyError):
                    continue
            
            if dates and values:
                series = pd.Series(values, index=dates, name=series_id)
                series = series.sort_index()
                
                print(f"✓ FRED 성공: {series_id} ({len(series)}개 포인트)")
                return series
            else:
                print(f"❌ FRED 데이터 없음: {series_id}")
                return None
        else:
            print(f"❌ FRED 응답에 데이터 없음: {series_id}")
            return None
            
    except Exception as e:
        print(f"❌ FRED 요청 실패: {series_id} - {e}")
        return None

# %%
# === 데이터 계산 함수들 ===

def load_yoy_data_from_fred(start_date='2000-01-01'):
    """FRED에서 직접 YoY 데이터 로드"""
    yoy_data_dict = {}
    
    print("\n📊 YoY 데이터 로딩...")
    for series_name, series_id in GDP_MAIN_YOY_SERIES.items():
        series_data = get_fred_data(series_id, start_date)
        if series_data is not None and len(series_data) > 0:
            yoy_data_dict[series_name] = series_data
        else:
            print(f"❌ YoY 데이터 로드 실패: {series_name}")
    
    return pd.DataFrame(yoy_data_dict) if yoy_data_dict else pd.DataFrame()

def prepare_change_data(raw_data):
    """변화율 데이터 준비 (이미 QoQ 연율화 변화율로 제공됨)"""
    return raw_data  # 그대로 사용

def validate_contribution_data(contrib_data, gdp_growth_data):
    """기여도 데이터 검증 및 조정"""
    if contrib_data.empty or gdp_growth_data.empty:
        return contrib_data
    
    # 기여도 합계와 GDP 성장률 비교
    main_components = ['consumption', 'investment', 'government', 'net_exports']
    available_components = [comp for comp in main_components if comp in contrib_data.columns]
    
    if available_components:
        contrib_sum = contrib_data[available_components].sum(axis=1)
        gdp_growth = gdp_growth_data['gdp'] if 'gdp' in gdp_growth_data.columns else None
        
        if gdp_growth is not None:
            # 최근 8분기 비교
            recent_data = pd.DataFrame({
                '기여도_합계': contrib_sum.tail(8),
                'GDP_성장률': gdp_growth.tail(8)
            }).dropna()
            
            print("\n🔍 기여도 검증 결과:")
            for date, row in recent_data.iterrows():
                quarter = f"{date.year}Q{date.quarter}"
                contrib_total = row['기여도_합계']
                gdp_rate = row['GDP_성장률']
                diff = abs(contrib_total - gdp_rate)
                
                status = "✅" if diff < 0.5 else "⚠️" if diff < 1.0 else "❌"
                print(f"  {quarter}: 기여도합 {contrib_total:+.1f} vs GDP성장 {gdp_rate:+.1f} {status}")
    
    return contrib_data

# %%
# === CSV 데이터 처리 함수들 ===

def prepare_csv_data():
    """CSV 저장을 위한 데이터 준비"""
    if GDP_DATA['change_data'].empty:
        return pd.DataFrame()
    
    # 변화율 데이터 준비
    csv_data = GDP_DATA['change_data'].copy()
    
    # 컬럼명을 한국어로 변경 (변화율)
    csv_data.columns = [f"{GDP_KOREAN_NAMES.get(col, col)}_QoQ연율화(%)" for col in csv_data.columns]
    
    # 기여도 데이터 추가
    if not GDP_DATA['contrib_data'].empty:
        for col in GDP_DATA['contrib_data'].columns:
            korean_name = GDP_KOREAN_NAMES.get(col, col)
            csv_data[f'{korean_name}_기여도(포인트)'] = GDP_DATA['contrib_data'][col]
    
    # YoY 데이터 추가
    if not GDP_DATA['yoy_data'].empty:
        for col in GDP_DATA['yoy_data'].columns:
            korean_name = GDP_KOREAN_NAMES.get(col, col)
            csv_data[f'{korean_name}_YoY(%)'] = GDP_DATA['yoy_data'][col]
    
    # 날짜 인덱스를 컬럼으로 변경
    csv_data = csv_data.reset_index()
    csv_data.rename(columns={'index': '날짜'}, inplace=True)
    
    # 날짜 포맷 조정
    csv_data['날짜'] = csv_data['날짜'].dt.strftime('%Y-Q%q')
    
    return csv_data

def save_to_csv(csv_data):
    """CSV 파일 저장"""
    if csv_data.empty:
        print("⚠️ 저장할 CSV 데이터가 없습니다.")
        return False
    
    csv_path = GDP_DATA['load_info']['csv_file_path']
    
    try:
        # 디렉토리가 없으면 생성
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        
        # CSV 저장
        csv_data.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"✓ CSV 저장 완료: {csv_path}")
        print(f"   - 데이터 수: {len(csv_data)}행 x {len(csv_data.columns)}열")
        
        GDP_DATA['csv_data'] = csv_data
        return True
        
    except Exception as e:
        print(f"❌ CSV 저장 실패: {e}")
        return False

def load_existing_csv():
    """기존 CSV 파일 로드"""
    csv_path = GDP_DATA['load_info']['csv_file_path']
    
    if not os.path.exists(csv_path):
        print("📝 기존 CSV 파일이 없습니다. 새로 생성됩니다.")
        return None
    
    try:
        existing_data = pd.read_csv(csv_path, encoding='utf-8-sig')
        print(f"✓ 기존 CSV 파일 로드: {csv_path}")
        print(f"   - 기존 데이터 수: {len(existing_data)}행 x {len(existing_data.columns)}열")
        return existing_data
    except Exception as e:
        print(f"⚠️ 기존 CSV 파일 로드 실패: {e}")
        return None

# %%
# === 데이터 로드 함수들 ===

def check_recent_gdp_data_consistency(series_list=None, check_count=3):
    """
    최근 N개 GDP 데이터의 일치성을 확인하는 함수 (CES 스타일 스마트 업데이트)
    
    Args:
        series_list: 확인할 시리즈 리스트 (None이면 주요 시리즈만)
        check_count: 확인할 최근 데이터 개수
    
    Returns:
        dict: 일치성 확인 결과 (need_update, reason, mismatches)
    """
    if not GDP_DATA['load_info']['loaded']:
        return {'need_update': True, 'reason': '기존 데이터 없음'}
    
    if series_list is None:
        # 주요 시리즈만 체크 (속도 향상)
        series_list = ['gdp', 'consumption', 'investment', 'government']
    
    # FRED API 초기화
    if not initialize_fred_api():
        return {'need_update': True, 'reason': 'API 초기화 실패'}
    
    print(f"🔍 GDP 스마트 업데이트 디버깅: 최근 {check_count}개 GDP 데이터 일치성 확인 중...")
    print(f"📊 GDP_DATA 로드 상태: {GDP_DATA['load_info']['loaded']}")
    if GDP_DATA['load_info']['loaded']:
        print(f"📅 마지막 로드 시간: {GDP_DATA['load_info'].get('load_time', 'None')}")
    
    mismatches = []
    new_data_available = False
    all_data_identical = True
    
    for series_name in series_list:
        if series_name not in GDP_MAIN_QOQ_SERIES or series_name not in GDP_DATA['change_data'].columns:
            continue
        
        series_id = GDP_MAIN_QOQ_SERIES[series_name]
        existing_data = GDP_DATA['change_data'][series_name].dropna()
        if len(existing_data) == 0:
            continue
        
        # 최근 데이터 가져오기 (최근 2년)
        current_date = datetime.datetime.now()
        start_date = f"{current_date.year - 2}-01-01"
        api_data = get_fred_data(series_id, start_date)
        
        if api_data is None or len(api_data) == 0:
            mismatches.append({
                'series': series_name,
                'reason': 'API 데이터 없음'
            })
            all_data_identical = False
            continue
        
        # 먼저 새로운 데이터가 있는지 확인
        latest_existing = existing_data.index[-1]
        latest_api = api_data.index[-1]
        
        if latest_api > latest_existing:
            new_data_available = True
            mismatches.append({
                'series': series_name,
                'reason': '새로운 데이터 존재',
                'existing_latest': latest_existing,
                'api_latest': latest_api
            })
            all_data_identical = False
            continue  # 새 데이터가 있으면 다른 체크는 건너뛀
        
        # 최근 N개 데이터 비교 (날짜 정규화 - 분기별 데이터용)
        existing_recent = existing_data.tail(check_count)
        api_recent = api_data.tail(check_count)
        
        # 날짜를 년-분기 형식으로 정규화하여 비교 (GDP는 분기별 데이터)
        existing_normalized = {}
        for date, value in existing_recent.items():
            quarter = (date.month - 1) // 3 + 1  # 1, 2, 3, 4분기 계산
            key = (date.year, quarter)
            existing_normalized[key] = value
            
        api_normalized = {}
        for date, value in api_recent.items():
            quarter = (date.month - 1) // 3 + 1
            key = (date.year, quarter)
            api_normalized[key] = value
        
        # 정규화된 날짜로 비교
        for key in existing_normalized.keys():
            if key in api_normalized:
                existing_val = existing_normalized[key]
                api_val = api_normalized[key]
                
                # 값이 다르면 불일치 (0.1 이상 차이 - GDP 데이터에 적합한 임계값)
                if abs(existing_val - api_val) > 0.1:
                    mismatches.append({
                        'series': series_name,
                        'date': pd.Timestamp(key[0], key[1]*3, 1),  # 비교용 분기 시작 날짜
                        'existing': existing_val,
                        'api': api_val,
                        'diff': abs(existing_val - api_val)
                    })
                    all_data_identical = False
            else:
                mismatches.append({
                    'series': series_name,
                    'date': pd.Timestamp(key[0], key[1]*3, 1),
                    'existing': existing_normalized[key],
                    'api': None,
                    'reason': '날짜 없음'
                })
                all_data_identical = False
    
    # 결과 판정 로직 (디버깅 강화)
    print(f"🎯 GDP 스마트 업데이트 판정:")
    print(f"   - 새로운 데이터 감지: {new_data_available}")
    print(f"   - 전체 데이터 일치: {all_data_identical}")
    print(f"   - 총 불일치 수: {len(mismatches)}")
    
    if new_data_available:
        print(f"🆕 새로운 데이터 감지: 업데이트 필요")
        for mismatch in mismatches[:3]:
            if 'reason' in mismatch and mismatch['reason'] == '새로운 데이터 존재':
                existing_q = f"{mismatch['existing_latest'].year}Q{mismatch['existing_latest'].quarter}"
                api_q = f"{mismatch['api_latest'].year}Q{mismatch['api_latest'].quarter}"
                print(f"   - {mismatch['series']}: {existing_q} → {api_q}")
        print(f"📤 GDP 스마트 업데이트 결과: API 호출 필요 (새로운 데이터)")
        return {'need_update': True, 'reason': '새로운 데이터', 'mismatches': mismatches}
    elif not all_data_identical:
        # 값 불일치가 있는 경우
        value_mismatches = [m for m in mismatches if m.get('reason') != '새로운 데이터 존재']
        print(f"⚠️ 데이터 불일치 발견: {len(value_mismatches)}개")
        
        # 디버깅: 실제 불일치 내용 출력
        print("🔍 불일치 세부 내용 (최대 3개):")
        for i, mismatch in enumerate(value_mismatches[:3]):
            if 'existing' in mismatch and 'api' in mismatch:
                quarter = (mismatch['date'].month - 1) // 3 + 1
                date_str = f"{mismatch['date'].year}Q{quarter}"
                print(f"   {i+1}. {mismatch['series']} ({date_str}): CSV={mismatch['existing']:.4f}, API={mismatch['api']:.4f}, 차이={mismatch['diff']:.4f}")
            else:
                print(f"   {i+1}. {mismatch}")
        
        # 큰 차이만 실제 불일치로 간주 (0.1 이상)
        significant_mismatches = [m for m in value_mismatches if m.get('diff', 0) > 0.1]
        if len(significant_mismatches) == 0:
            print("📝 모든 차이가 0.1 미만입니다. 저장 정밀도 차이로 간주하여 업데이트를 건너뜁니다.")
            print(f"📤 GDP 스마트 업데이트 결과: API 호출 스킵 (정밀도 차이)")
            return {'need_update': False, 'reason': '미세한 정밀도 차이', 'mismatches': mismatches}
        else:
            print(f"🚨 유의미한 차이 발견: {len(significant_mismatches)}개 (0.1 이상)")
            print(f"📤 GDP 스마트 업데이트 결과: API 호출 필요 (데이터 불일치)")
            return {'need_update': True, 'reason': '데이터 불일치', 'mismatches': mismatches}
    else:
        print("✅ 최근 데이터가 완전히 일치합니다. 업데이트를 건너뜁니다.")
        print(f"📤 GDP 스마트 업데이트 결과: API 호출 스킵 (데이터 일치)")
        return {'need_update': False, 'reason': '데이터 일치', 'mismatches': []}

def load_all_gdp_data(start_date='2000-01-01', force_reload=False, smart_update=True):
    """
    모든 GDP 데이터 로드 및 스마트 업데이트
    
    Args:
        start_date: 시작 날짜
        force_reload: 강제 재로드 여부
        smart_update: 스마트 업데이트 사용 여부
    
    Returns:
        bool: 로드 성공 여부
    """
    global GDP_DATA
    
    # 스마트 업데이트 체크
    if GDP_DATA['load_info']['loaded'] and not force_reload and smart_update:
        # 1시간 이내 로드된 데이터는 스킵
        if GDP_DATA['load_info']['load_time'] and \
           (datetime.datetime.now() - GDP_DATA['load_info']['load_time']).seconds < 3600:
            print("💾 최신 데이터 사용 중 (1시간 이내 로드됨)")
            print_load_info()
            return True
        
        # 데이터 일치성 확인
        consistency_check = check_recent_gdp_data_consistency()
        if not consistency_check['need_update']:
            print("💾 스마트 업데이트: 데이터가 일치하여 업데이트 스킵")
            return True
        else:
            print(f"🔄 스마트 업데이트: {consistency_check['reason']} - 데이터 업데이트 실행")
    
    print("🚀 GDP 데이터 로딩 시작... (FRED API)")
    print("="*50)
    
    # FRED API 초기화
    if not initialize_fred_api():
        print("❌ FRED API 초기화 실패")
        return False
    
    # 기존 CSV 파일 확인
    existing_csv = load_existing_csv()
    
    # 데이터 수집
    raw_data_dict = {}
    
    # 변화율 데이터 수집
    change_data_dict = {}
    contrib_data_dict = {}
    
    # GDP 주요 구성 요소 데이터 로드 (QoQ 변화율 + 기여도)
    print("\n📊 GDP 주요 구성 요소 로딩...")
    for series_name, series_id in GDP_MAIN_QOQ_SERIES.items():
        series_data = get_fred_data(series_id, start_date)
        if series_data is not None and len(series_data) > 0:
            change_data_dict[series_name] = series_data
        else:
            print(f"❌ 변화율 데이터 로드 실패: {series_name}")
    
    for series_name, series_id in GDP_MAIN_CONTRIB_SERIES.items():
        series_data = get_fred_data(series_id, start_date)
        if series_data is not None and len(series_data) > 0:
            contrib_data_dict[series_name] = series_data
        else:
            print(f"❌ 기여도 데이터 로드 실패: {series_name}")
    
    # GDP 소비 세부 구성 요소 로드
    print("\n🛍️ 소비 세부 구성 요소 로딩...")
    for series_name, series_id in GDP_CONSUMPTION_CHANGE_SERIES.items():
        series_data = get_fred_data(series_id, start_date)
        if series_data is not None and len(series_data) > 0:
            change_data_dict[series_name] = series_data
        else:
            print(f"❌ 변화율 데이터 로드 실패: {series_name}")
    
    for series_name, series_id in GDP_CONSUMPTION_CONTRIB_SERIES.items():
        series_data = get_fred_data(series_id, start_date)
        if series_data is not None and len(series_data) > 0:
            contrib_data_dict[series_name] = series_data
        else:
            print(f"❌ 기여도 데이터 로드 실패: {series_name}")
    
    # GDP 투자 세부 구성 요소 로드
    print("\n🏗️ 투자 세부 구성 요소 로딩...")
    for series_name, series_id in GDP_INVESTMENT_CHANGE_SERIES.items():
        series_data = get_fred_data(series_id, start_date)
        if series_data is not None and len(series_data) > 0:
            change_data_dict[series_name] = series_data
        else:
            print(f"❌ 변화율 데이터 로드 실패: {series_name}")
    
    for series_name, series_id in GDP_INVESTMENT_CONTRIB_SERIES.items():
        series_data = get_fred_data(series_id, start_date)
        if series_data is not None and len(series_data) > 0:
            contrib_data_dict[series_name] = series_data
        else:
            print(f"❌ 기여도 데이터 로드 실패: {series_name}")
    
    # GDP 정부 지출 세부 구성 요소 로드
    print("\n🏛️ 정부 지출 세부 구성 요소 로딩...")
    for series_name, series_id in GDP_GOVERNMENT_CHANGE_SERIES.items():
        series_data = get_fred_data(series_id, start_date)
        if series_data is not None and len(series_data) > 0:
            change_data_dict[series_name] = series_data
        else:
            print(f"❌ 변화율 데이터 로드 실패: {series_name}")
    
    for series_name, series_id in GDP_GOVERNMENT_CONTRIB_SERIES.items():
        series_data = get_fred_data(series_id, start_date)
        if series_data is not None and len(series_data) > 0:
            contrib_data_dict[series_name] = series_data
        else:
            print(f"❌ 기여도 데이터 로드 실패: {series_name}")
    
    # GDP 무역 세부 구성 요소 로드
    print("\n🌍 무역 세부 구성 요소 로딩...")
    for series_name, series_id in GDP_TRADE_CHANGE_SERIES.items():
        series_data = get_fred_data(series_id, start_date)
        if series_data is not None and len(series_data) > 0:
            change_data_dict[series_name] = series_data
        else:
            print(f"❌ 변화율 데이터 로드 실패: {series_name}")
    
    for series_name, series_id in GDP_TRADE_CONTRIB_SERIES.items():
        series_data = get_fred_data(series_id, start_date)
        if series_data is not None and len(series_data) > 0:
            contrib_data_dict[series_name] = series_data
        else:
            print(f"❌ 기여도 데이터 로드 실패: {series_name}")
    
    if len(change_data_dict) < 6:  # 최소 6개 주요 시리즈는 있어야 함
        error_msg = f"❌ 로드된 변화율 시리즈가 너무 적습니다: {len(change_data_dict)}개"
        print(error_msg)
        return False
    
    # 전역 저장소에 저장
    GDP_DATA['change_data'] = pd.DataFrame(change_data_dict)  # 이미 QoQ 연율화 변화율
    GDP_DATA['contrib_data'] = pd.DataFrame(contrib_data_dict)  # 기여도 데이터
    
    # 기여도 데이터 검증
    GDP_DATA['contrib_data'] = validate_contribution_data(GDP_DATA['contrib_data'], GDP_DATA['change_data'])
    
    # YoY 변화율 로드 (FRED에서 직접)
    GDP_DATA['yoy_data'] = load_yoy_data_from_fred(start_date)
    
    # 최신 값 저장
    latest_values = {}
    for col in GDP_DATA['change_data'].columns:
        latest_values[col] = {
            'qoq_change_rate': GDP_DATA['change_data'][col].iloc[-1],  # QoQ 연율화 변화율
            'yoy_change_rate': GDP_DATA['yoy_data'][col].iloc[-1] if not GDP_DATA['yoy_data'].empty and col in GDP_DATA['yoy_data'].columns else None,
            'contrib_points': GDP_DATA['contrib_data'][col].iloc[-1] if col in GDP_DATA['contrib_data'].columns else None
        }
    
    GDP_DATA['latest_values'] = latest_values
    
    # CSV 데이터 준비 및 저장
    print("\n💾 CSV 파일 저장 중...")
    csv_data = prepare_csv_data()
    save_to_csv(csv_data)
    
    GDP_DATA['load_info'] = {
        'loaded': True,
        'load_time': datetime.datetime.now(),
        'start_date': start_date,
        'series_count': len(change_data_dict) + len(contrib_data_dict),
        'data_points': len(GDP_DATA['change_data']),
        'csv_updated': True,
        'csv_file_path': '/home/jyp0615/us_eco/gdp_data.csv'
    }
    
    print("\n✅ 데이터 로딩 완료!")
    print_load_info()
    
    return True

def print_load_info():
    """로드 정보 출력"""
    info = GDP_DATA['load_info']
    if not info['loaded']:
        print("❌ 데이터가 로드되지 않음")
        return
    
    print(f"📊 로드된 데이터 정보:")
    print(f"   시리즈 개수: {info['series_count']}")
    print(f"   데이터 포인트: {info['data_points']}")
    print(f"   시작 날짜: {info['start_date']}")
    print(f"   로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   CSV 파일: {info['csv_file_path']}")
    print(f"   CSV 업데이트: {'✅' if info['csv_updated'] else '❌'}")
    
    if not GDP_DATA['change_data'].empty:
        start_date = GDP_DATA['change_data'].index[0]
        end_date = GDP_DATA['change_data'].index[-1]
        start_quarter = f"{start_date.year} Q{start_date.quarter}"
        end_quarter = f"{end_date.year} Q{end_date.quarter}"
        print(f"   데이터 기간: {start_quarter} ~ {end_quarter}")

# %%
# === 데이터 접근 함수들 ===

def get_change_data(series_names=None):
    """변화율 데이터 반환 (QoQ 연율화 변화율)"""
    if not GDP_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다. load_all_gdp_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return GDP_DATA['change_data'].copy()
    
    available_series = [s for s in series_names if s in GDP_DATA['change_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return GDP_DATA['change_data'][available_series].copy()

def get_contrib_data(series_names=None):
    """기여도 데이터 반환 (포인트 단위)"""
    if not GDP_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다.")
        return pd.DataFrame()
    
    if series_names is None:
        return GDP_DATA['contrib_data'].copy()
    
    available_series = [s for s in series_names if s in GDP_DATA['contrib_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return GDP_DATA['contrib_data'][available_series].copy()

# 이전 함수명과의 호환성을 위해 별칭 제공
def get_qoq_data(series_names=None):
    """전분기대비 변화율 데이터 반환 (get_change_data와 동일)"""
    return get_change_data(series_names)

def get_raw_data(series_names=None):
    """이전 호환성을 위한 별칭 (get_change_data와 동일)"""
    return get_change_data(series_names)

def get_yoy_data(series_names=None):
    """전년동기대비 변화율 데이터 반환"""
    if not GDP_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다.")
        return pd.DataFrame()
    
    if series_names is None:
        return GDP_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in GDP_DATA['yoy_data'].columns]
    return GDP_DATA['yoy_data'][available_series].copy()

def get_latest_values(series_names=None):
    """최신 값들 반환"""
    if not GDP_DATA['load_info']['loaded']:
        print("⚠️ 데이터가 로드되지 않았습니다.")
        return {}
    
    if series_names is None:
        return GDP_DATA['latest_values'].copy()
    
    return {name: GDP_DATA['latest_values'].get(name) for name in series_names if name in GDP_DATA['latest_values']}

def create_gdp_contribution_chart():
    """
    GDP 기여도 차트 생성 (KPDS 포맷)
    
    Returns:
        plotly figure
    """
    print("GDP 구성 요소별 기여도 (전분기대비)")
    if not GDP_DATA['load_info']['loaded']:
        print("⚠️ 먼저 load_all_gdp_data()를 실행하세요.")
        return None
    
    # 주요 구성 요소들의 기여도 계산
    main_components = ['consumption', 'investment', 'government', 'net_exports']
    available_components = [comp for comp in main_components if comp in GDP_DATA['contrib_data'].columns]
    
    if not available_components:
        print("⚠️ 기여도 계산을 위한 데이터가 없습니다.")
        return None
    
    # 최근 8분기 데이터
    contribution_data = GDP_DATA['contrib_data'][available_components].tail(8)
    
    # 날짜 라벨
    x_labels = [f"{date.year} Q{date.quarter}" for date in contribution_data.index]
    
    # KPDS 폰트 설정
    font_family = 'NanumGothic'
    
    # 누적 막대 차트 생성
    fig = go.Figure()
    
    colors = [deepblue_pds, deepred_pds, beige_pds, blue_pds, grey_pds]
    
    for i, component in enumerate(available_components):
        korean_name = GDP_KOREAN_NAMES.get(component, component)
        
        # 수입은 음수로 표시 (GDP에서 차감되므로)
        values = contribution_data[component].values
        if component == 'imports':
            values = -values
        
        fig.add_trace(go.Bar(
            x=x_labels,
            y=values,
            name=korean_name,
            marker_color=colors[i % len(colors)],
            text=[f'{v:+.1f}' for v in values],
            textposition='inside',
            textfont=dict(size=10, color='white')
        ))
    
    # GDP 총 변화 라인 추가
    if 'gdp' in GDP_DATA['change_data'].columns:
        gdp_change = GDP_DATA['change_data']['gdp'].tail(8).values
        fig.add_trace(go.Scatter(
            x=x_labels,
            y=gdp_change,
            mode='lines+markers',
            name='GDP 총 변화',
            line=dict(color='black', width=3),
            marker=dict(size=8, color='black')
        ))
    
    fig.update_layout(
        xaxis=dict(
            title="분기",
            title_font=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE),
            tickfont=dict(family=font_family, size=FONT_SIZE_GENERAL)
        ),
        yaxis=dict(
            title="십억 달러",
            title_font=dict(family=font_family, size=FONT_SIZE_AXIS_TITLE),
            tickfont=dict(family=font_family, size=FONT_SIZE_GENERAL)
        ),
        barmode='relative',
        width=1000,
        height=600,
        paper_bgcolor='white',
        plot_bgcolor='white',
        font=dict(family=font_family, size=FONT_SIZE_GENERAL, color="black"),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    # 0선 추가
    fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)
    
    return fig
