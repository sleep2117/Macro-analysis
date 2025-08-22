# %%
"""
CPS(Current Population Survey) 데이터 분석 (리팩토링 버전)
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
# === CPS 시리즈 정의 ===

CPS_SERIES = {
    # 주요 노동시장 지표
    'LNS11000000': 'Civilian Labor Force Level',
    'LNS11300000': 'Civilian Labor Force Participation Rate',
    'LNS12000000': 'Employment Level',
    'LNS12300000': 'Employment-Population Ratio',
    'LNS12500000': 'Employed, Usually Work Full Time',
    'LNS12600000': 'Employed, Usually Work Part Time',
    'LNS13000000': 'Unemployment Level',
    'LNS14000000': 'Unemployment Rate',
    
    # 연령별 실업률
    'LNS14000012': 'Unemployment Rate - 16-19 Years',
    'LNS14000025': 'Unemployment Rate - 20 Years & Over Men',
    'LNS14000026': 'Unemployment Rate - 20 Years & Over Women',
    
    # 인종별 실업률
    'LNS14000003': 'Unemployment Rate - White',
    'LNS14000006': 'Unemployment Rate - Black or African American',
    'LNS14032183': 'Unemployment Rate - Asian',
    'LNS14000009': 'Unemployment Rate - Hispanic or Latino',
    
    # 교육수준별 실업률
    'LNS14027659': 'Unemployment Rate - Less than High School',
    'LNS14027660': 'Unemployment Rate - High School Graduates',
    'LNS14027689': 'Unemployment Rate - Some College',
    'LNS14027662': 'Unemployment Rate - Bachelor\'s Degree and Higher',
    
    # 실업 기간
    'LNS13008396': 'Unemployed Less Than 5 weeks',
    'LNS13008756': 'Unemployed 5-14 Weeks',
    'LNS13008516': 'Unemployed 15 Weeks & Over',
    'LNS13008636': 'Unemployed 27 Weeks & Over',
    'LNS13008275': 'Average Weeks Unemployed',
    'LNS13008276': 'Median Weeks Unemployed',
    
    # 실업 유형
    'LNS13023621': 'Unemployment - Job Losers',
    'LNS13023653': 'Unemployment - Job Losers On Layoff',
    'LNS13025699': 'Unemployment - Job Losers Not on Layoff',
    'LNS13023705': 'Unemployment - Job Leavers',
    'LNS13023557': 'Unemployment - Reentrants',
    'LNS13023569': 'Unemployment - New Entrants',
    
    # 기타 노동시장 지표
    'LNS12032194': 'Part Time for Economic Reasons',
    'LNS15000000': 'Not in Labor Force',
    'LNS15026642': 'Marginally Attached to Labor Force',
    'LNS15026645': 'Discouraged Workers',
    'LNS13327709': 'U-6 Unemployment Rate',
    'LNS12026619': 'Multiple Jobholders Level',
    'LNS12026620': 'Multiple Jobholders as Percent of Employed',
    
    # 날씨 영향
    'LNU02036012': 'Not at Work - Bad Weather',
    'LNU02033224': 'At Work 1-34 Hrs - Bad Weather',
    
    # 20세 이상 주요 지표
    'LNS11000024': 'Labor Force - 20 Years & Over',
    'LNS11300024': 'Participation Rate - 20 Years & Over',
    'LNS12000024': 'Employment - 20 Years & Over',
    'LNS13000024': 'Unemployment - 20 Years & Over',
    'LNS14000024': 'Unemployment Rate - 20 Years & Over',
    
    # 25세 이상 주요 지표
    'LNS11000048': 'Labor Force - 25 Years & Over',
    'LNS11300048': 'Participation Rate - 25 Years & Over',
    'LNS12000048': 'Employment - 25 Years & Over',
    'LNS13000048': 'Unemployment - 25 Years & Over',
    'LNS14000048': 'Unemployment Rate - 25 Years & Over',
    
    # 55세 이상 주요 지표
    'LNS11024230': 'Labor Force - 55 Years & Over',
    'LNS11324230': 'Participation Rate - 55 Years & Over',
    'LNS12024230': 'Employment - 55 Years & Over',
    'LNS13024230': 'Unemployment - 55 Years & Over',
    'LNS14024230': 'Unemployment Rate - 55 Years & Over',
    
    # 히스패닉/라티노 전체 노동시장 지표
    'LNS11000009': 'Labor Force - Hispanic or Latino',
    'LNS11300009': 'Participation Rate - Hispanic or Latino',
    'LNS12000009': 'Employment - Hispanic or Latino',
    'LNS13000009': 'Unemployment - Hispanic or Latino',
    'LNS15000009': 'Not in Labor Force - Hispanic or Latino',
    
    # === 추가 연령별 노동시장 지표 (Not Seasonally Adjusted) ===
    'LNU01076975': 'Civilian Labor Force - 18 Years & Over (NSA)',
    'LNU01376975': 'Participation Rate - 18 Years & Over (NSA)',
    'LNU02076975': 'Employment - 18 Years & Over (NSA)',
    'LNU03076975': 'Unemployment - 18 Years & Over (NSA)',
    'LNU04076975': 'Unemployment Rate - 18 Years & Over (NSA)',
    'LNU05076975': 'Not in Labor Force - 18 Years & Over (NSA)',
    
    # 25세 이상 (Not Seasonally Adjusted)
    'LNU01000048': 'Civilian Labor Force - 25 Years & Over (NSA)',
    'LNU01300048': 'Participation Rate - 25 Years & Over (NSA)',
    'LNU02000048': 'Employment - 25 Years & Over (NSA)',
    'LNU02500048': 'Employed Full Time - 25 Years & Over (NSA)',
    'LNU02600048': 'Employed Part Time - 25 Years & Over (NSA)',
    'LNU03000048': 'Unemployment - 25 Years & Over (NSA)',
    'LNU03100048': 'Unemployed Looking for Full-time - 25 Years & Over (NSA)',
    'LNU03200048': 'Unemployed Looking for Part-time - 25 Years & Over (NSA)',
    'LNU04000048': 'Unemployment Rate - 25 Years & Over (NSA)',
    
    # 45세 이상 (Not Seasonally Adjusted)
    'LNU01000092': 'Civilian Labor Force - 45 Years & Over (NSA)',
    'LNU01300092': 'Participation Rate - 45 Years & Over (NSA)',
    'LNU02000092': 'Employment - 45 Years & Over (NSA)',
    'LNU03000092': 'Unemployment - 45 Years & Over (NSA)',
    'LNU04000092': 'Unemployment Rate - 45 Years & Over (NSA)',
    'LNU05000092': 'Not in Labor Force - 45 Years & Over (NSA)',
    
    # 55세 이상 (Not Seasonally Adjusted)
    'LNU01024230': 'Civilian Labor Force - 55 Years & Over (NSA)',
    'LNU01324230': 'Participation Rate - 55 Years & Over (NSA)',
    'LNU02024230': 'Employment - 55 Years & Over (NSA)',
    'LNU02524230': 'Employed Full Time - 55 Years & Over (NSA)',
    'LNU02624230': 'Employed Part Time - 55 Years & Over (NSA)',
    'LNU03024230': 'Unemployment - 55 Years & Over (NSA)',
    'LNU03124230': 'Unemployed Looking for Full-time - 55 Years & Over (NSA)',
    'LNU03224230': 'Unemployed Looking for Part-time - 55 Years & Over (NSA)',
    'LNU04024230': 'Unemployment Rate - 55 Years & Over (NSA)',
    'LNU05024230': 'Not in Labor Force - 55 Years & Over (NSA)',
    
    # 65세 이상 (Not Seasonally Adjusted)
    'LNU01000097': 'Civilian Labor Force - 65 Years & Over (NSA)',
    'LNU01300097': 'Participation Rate - 65 Years & Over (NSA)',
    'LNU02000097': 'Employment - 65 Years & Over (NSA)',
    'LNU03000097': 'Unemployment - 65 Years & Over (NSA)',
    'LNU04000097': 'Unemployment Rate - 65 Years & Over (NSA)',
    'LNU05000097': 'Not in Labor Force - 65 Years & Over (NSA)',
    
    # === 추가 인종별 노동시장 지표 (Not Seasonally Adjusted) ===
    # 백인 (White)
    'LNU01000003': 'Civilian Labor Force - White (NSA)',
    'LNU01300003': 'Participation Rate - White (NSA)',
    'LNU02000003': 'Employment - White (NSA)',
    'LNU02500003': 'Employed Full Time - White (NSA)',
    'LNU02600003': 'Employed Part Time - White (NSA)',
    'LNU03000003': 'Unemployment - White (NSA)',
    'LNU04000003': 'Unemployment Rate - White (NSA)',
    'LNU05000003': 'Not in Labor Force - White (NSA)',
    
    # 흑인 (Black or African American)
    'LNU01000006': 'Civilian Labor Force - Black or African American (NSA)',
    'LNU01300006': 'Participation Rate - Black or African American (NSA)',
    'LNU02000006': 'Employment - Black or African American (NSA)',
    'LNU02500006': 'Employed Full Time - Black or African American (NSA)',
    'LNU02600006': 'Employed Part Time - Black or African American (NSA)',
    'LNU03000006': 'Unemployment - Black or African American (NSA)',
    'LNU04000006': 'Unemployment Rate - Black or African American (NSA)',
    'LNU05000006': 'Not in Labor Force - Black or African American (NSA)',
    
    # 아시아계 (Asian)
    'LNU01032183': 'Civilian Labor Force - Asian (NSA)',
    'LNU01332183': 'Participation Rate - Asian (NSA)',
    'LNU02032183': 'Employment - Asian (NSA)',
    'LNU02532183': 'Employed Full Time - Asian (NSA)',
    'LNU02632183': 'Employed Part Time - Asian (NSA)',
    'LNU03032183': 'Unemployment - Asian (NSA)',
    'LNU04032183': 'Unemployment Rate - Asian (NSA)',
    'LNU05032183': 'Not in Labor Force - Asian (NSA)',
    
    # 아메리카 원주민 (American Indian or Alaska Native)
    'LNU01035243': 'Civilian Labor Force - American Indian or Alaska Native (NSA)',
    'LNU01335243': 'Participation Rate - American Indian or Alaska Native (NSA)',
    'LNU02035243': 'Employment - American Indian or Alaska Native (NSA)',
    'LNU03035243': 'Unemployment - American Indian or Alaska Native (NSA)',
    'LNU04035243': 'Unemployment Rate - American Indian or Alaska Native (NSA)',
    'LNU05035243': 'Not in Labor Force - American Indian or Alaska Native (NSA)',
    
    # 하와이 원주민 및 태평양 섬 주민 (Native Hawaiian or Other Pacific Islander)
    'LNU01035553': 'Civilian Labor Force - Native Hawaiian or Other Pacific Islander (NSA)',
    'LNU01335553': 'Participation Rate - Native Hawaiian or Other Pacific Islander (NSA)',
    'LNU02035553': 'Employment - Native Hawaiian or Other Pacific Islander (NSA)',
    'LNU03035553': 'Unemployment - Native Hawaiian or Other Pacific Islander (NSA)',
    'LNU04035553': 'Unemployment Rate - Native Hawaiian or Other Pacific Islander (NSA)',
    'LNU05035553': 'Not in Labor Force - Native Hawaiian or Other Pacific Islander (NSA)',
    
    # === 기본 지표들의 Not Seasonally Adjusted 버전 ===
    'LNU01000000': 'Civilian Labor Force (NSA)',
    'LNU01300000': 'Civilian Labor Force Participation Rate (NSA)',
    'LNU02000000': 'Employment Level (NSA)',
    'LNU02500000': 'Employed, Usually Work Full Time (NSA)',
    'LNU02600000': 'Employed, Usually Work Part Time (NSA)',
    'LNU03000000': 'Unemployment Level (NSA)',
    'LNU03100000': 'Unemployed Looking for Full-time Work (NSA)',
    'LNU03200000': 'Unemployed Looking for Part-time Work (NSA)',
    'LNU04000000': 'Unemployment Rate (NSA)',
    'LNU04100000': 'Unemployment Rate of the Full-time Labor Force (NSA)',
    'LNU04200000': 'Unemployment Rate of the Part-time Labor Force (NSA)',
    'LNU05000000': 'Not in Labor Force (NSA)'
}

# 한국어 이름 매핑
CPS_KOREAN_NAMES = {
    # 주요 노동시장 지표
    'LNS11000000': '경제활동인구',
    'LNS11300000': '경제활동참가율',
    'LNS12000000': '취업자수',
    'LNS12300000': '고용률',
    'LNS12500000': '풀타임 취업자',
    'LNS12600000': '파트타임 취업자',
    'LNS13000000': '실업자수',
    'LNS14000000': '실업률',
    
    # 연령별 실업률
    'LNS14000012': '실업률 - 16-19세',
    'LNS14000025': '실업률 - 20세 이상 남성',
    'LNS14000026': '실업률 - 20세 이상 여성',
    
    # 인종별 실업률
    'LNS14000003': '실업률 - 백인',
    'LNS14000006': '실업률 - 흑인',
    'LNS14032183': '실업률 - 아시아계',
    'LNS14000009': '실업률 - 히스패닉',
    
    # 교육수준별 실업률
    'LNS14027659': '실업률 - 고졸 미만',
    'LNS14027660': '실업률 - 고졸',
    'LNS14027689': '실업률 - 대학 중퇴/전문대',
    'LNS14027662': '실업률 - 대졸 이상',
    
    # 실업 기간
    'LNS13008396': '실업자 - 5주 미만',
    'LNS13008756': '실업자 - 5-14주',
    'LNS13008516': '실업자 - 15주 이상',
    'LNS13008636': '실업자 - 27주 이상',
    'LNS13008275': '평균 실업 기간',
    'LNS13008276': '중간값 실업 기간',
    
    # 실업 유형
    'LNS13023621': '실직자',
    'LNS13023653': '일시해고자',
    'LNS13025699': '영구해고자',
    'LNS13023705': '자발적 이직자',
    'LNS13023557': '재진입자',
    'LNS13023569': '신규진입자',
    
    # 기타 노동시장 지표
    'LNS12032194': '경제적 이유 파트타임',
    'LNS15000000': '비경제활동인구',
    'LNS15026642': '한계노동력',
    'LNS15026645': '구직단념자',
    'LNS13327709': 'U-6 실업률 (광의)',
    'LNS12026619': '복수직업 보유자',
    'LNS12026620': '복수직업 보유율',
    
    # 날씨 영향
    'LNU02036012': '악천후 결근',
    'LNU02033224': '악천후 단축근무',
    
    # 20세 이상 주요 지표
    'LNS11000024': '경제활동인구 - 20세 이상',
    'LNS11300024': '경제활동참가율 - 20세 이상',
    'LNS12000024': '취업자수 - 20세 이상',
    'LNS13000024': '실업자수 - 20세 이상',
    'LNS14000024': '실업률 - 20세 이상',
    
    # 25세 이상 주요 지표
    'LNS11000048': '경제활동인구 - 25세 이상',
    'LNS11300048': '경제활동참가율 - 25세 이상',
    'LNS12000048': '취업자수 - 25세 이상',
    'LNS13000048': '실업자수 - 25세 이상',
    'LNS14000048': '실업률 - 25세 이상',
    
    # 55세 이상 주요 지표
    'LNS11024230': '경제활동인구 - 55세 이상',
    'LNS11324230': '경제활동참가율 - 55세 이상',
    'LNS12024230': '취업자수 - 55세 이상',
    'LNS13024230': '실업자수 - 55세 이상',
    'LNS14024230': '실업률 - 55세 이상',
    
    # 히스패닉/라티노 전체 노동시장 지표
    'LNS11000009': '경제활동인구 - 히스패닉/라티노',
    'LNS11300009': '경제활동참가율 - 히스패닉/라티노',
    'LNS12000009': '취업자수 - 히스패닉/라티노',
    'LNS13000009': '실업자수 - 히스패닉/라티노',
    'LNS15000009': '비경제활동인구 - 히스패닉/라티노',
    
    # === 추가 연령별 노동시장 지표 한국어 매핑 (Not Seasonally Adjusted) ===
    # 18세 이상
    'LNU01076975': '경제활동인구 - 18세 이상 (비계절조정)',
    'LNU01376975': '경제활동참가율 - 18세 이상 (비계절조정)',
    'LNU02076975': '취업자수 - 18세 이상 (비계절조정)',
    'LNU03076975': '실업자수 - 18세 이상 (비계절조정)',
    'LNU04076975': '실업률 - 18세 이상 (비계절조정)',
    'LNU05076975': '비경제활동인구 - 18세 이상 (비계절조정)',
    
    # 25세 이상 (비계절조정)
    'LNU01000048': '경제활동인구 - 25세 이상 (비계절조정)',
    'LNU01300048': '경제활동참가율 - 25세 이상 (비계절조정)',
    'LNU02000048': '취업자수 - 25세 이상 (비계절조정)',
    'LNU02500048': '풀타임 취업자 - 25세 이상 (비계절조정)',
    'LNU02600048': '파트타임 취업자 - 25세 이상 (비계절조정)',
    'LNU03000048': '실업자수 - 25세 이상 (비계절조정)',
    'LNU03100048': '풀타임 구직자 - 25세 이상 (비계절조정)',
    'LNU03200048': '파트타임 구직자 - 25세 이상 (비계절조정)',
    'LNU04000048': '실업률 - 25세 이상 (비계절조정)',
    
    # 45세 이상 (비계절조정)
    'LNU01000092': '경제활동인구 - 45세 이상 (비계절조정)',
    'LNU01300092': '경제활동참가율 - 45세 이상 (비계절조정)',
    'LNU02000092': '취업자수 - 45세 이상 (비계절조정)',
    'LNU03000092': '실업자수 - 45세 이상 (비계절조정)',
    'LNU04000092': '실업률 - 45세 이상 (비계절조정)',
    'LNU05000092': '비경제활동인구 - 45세 이상 (비계절조정)',
    
    # 55세 이상 (비계절조정)
    'LNU01024230': '경제활동인구 - 55세 이상 (비계절조정)',
    'LNU01324230': '경제활동참가율 - 55세 이상 (비계절조정)',
    'LNU02024230': '취업자수 - 55세 이상 (비계절조정)',
    'LNU02524230': '풀타임 취업자 - 55세 이상 (비계절조정)',
    'LNU02624230': '파트타임 취업자 - 55세 이상 (비계절조정)',
    'LNU03024230': '실업자수 - 55세 이상 (비계절조정)',
    'LNU03124230': '풀타임 구직자 - 55세 이상 (비계절조정)',
    'LNU03224230': '파트타임 구직자 - 55세 이상 (비계절조정)',
    'LNU04024230': '실업률 - 55세 이상 (비계절조정)',
    'LNU05024230': '비경제활동인구 - 55세 이상 (비계절조정)',
    
    # 65세 이상 (비계절조정)
    'LNU01000097': '경제활동인구 - 65세 이상 (비계절조정)',
    'LNU01300097': '경제활동참가율 - 65세 이상 (비계절조정)',
    'LNU02000097': '취업자수 - 65세 이상 (비계절조정)',
    'LNU03000097': '실업자수 - 65세 이상 (비계절조정)',
    'LNU04000097': '실업률 - 65세 이상 (비계절조정)',
    'LNU05000097': '비경제활동인구 - 65세 이상 (비계절조정)',
    
    # === 추가 인종별 노동시장 지표 한국어 매핑 (Not Seasonally Adjusted) ===
    # 백인 (비계절조정)
    'LNU01000003': '경제활동인구 - 백인 (비계절조정)',
    'LNU01300003': '경제활동참가율 - 백인 (비계절조정)',
    'LNU02000003': '취업자수 - 백인 (비계절조정)',
    'LNU02500003': '풀타임 취업자 - 백인 (비계절조정)',
    'LNU02600003': '파트타임 취업자 - 백인 (비계절조정)',
    'LNU03000003': '실업자수 - 백인 (비계절조정)',
    'LNU04000003': '실업률 - 백인 (비계절조정)',
    'LNU05000003': '비경제활동인구 - 백인 (비계절조정)',
    
    # 흑인 (비계절조정)
    'LNU01000006': '경제활동인구 - 흑인 (비계절조정)',
    'LNU01300006': '경제활동참가율 - 흑인 (비계절조정)',
    'LNU02000006': '취업자수 - 흑인 (비계절조정)',
    'LNU02500006': '풀타임 취업자 - 흑인 (비계절조정)',
    'LNU02600006': '파트타임 취업자 - 흑인 (비계절조정)',
    'LNU03000006': '실업자수 - 흑인 (비계절조정)',
    'LNU04000006': '실업률 - 흑인 (비계절조정)',
    'LNU05000006': '비경제활동인구 - 흑인 (비계절조정)',
    
    # 아시아계 (비계절조정)
    'LNU01032183': '경제활동인구 - 아시아계 (비계절조정)',
    'LNU01332183': '경제활동참가율 - 아시아계 (비계절조정)',
    'LNU02032183': '취업자수 - 아시아계 (비계절조정)',
    'LNU02532183': '풀타임 취업자 - 아시아계 (비계절조정)',
    'LNU02632183': '파트타임 취업자 - 아시아계 (비계절조정)',
    'LNU03032183': '실업자수 - 아시아계 (비계절조정)',
    'LNU04032183': '실업률 - 아시아계 (비계절조정)',
    'LNU05032183': '비경제활동인구 - 아시아계 (비계절조정)',
    
    # 아메리카 원주민 (비계절조정)
    'LNU01035243': '경제활동인구 - 아메리카 원주민 (비계절조정)',
    'LNU01335243': '경제활동참가율 - 아메리카 원주민 (비계절조정)',
    'LNU02035243': '취업자수 - 아메리카 원주민 (비계절조정)',
    'LNU03035243': '실업자수 - 아메리카 원주민 (비계절조정)',
    'LNU04035243': '실업률 - 아메리카 원주민 (비계절조정)',
    'LNU05035243': '비경제활동인구 - 아메리카 원주민 (비계절조정)',
    
    # 하와이 원주민 및 태평양 섬 주민 (비계절조정)
    'LNU01035553': '경제활동인구 - 하와이/태평양 원주민 (비계절조정)',
    'LNU01335553': '경제활동참가율 - 하와이/태평양 원주민 (비계절조정)',
    'LNU02035553': '취업자수 - 하와이/태평양 원주민 (비계절조정)',
    'LNU03035553': '실업자수 - 하와이/태평양 원주민 (비계절조정)',
    'LNU04035553': '실업률 - 하와이/태평양 원주민 (비계절조정)',
    'LNU05035553': '비경제활동인구 - 하와이/태평양 원주민 (비계절조정)',
    
    # === 기본 지표들의 비계절조정 버전 한국어 매핑 ===
    'LNU01000000': '경제활동인구 (비계절조정)',
    'LNU01300000': '경제활동참가율 (비계절조정)',
    'LNU02000000': '취업자수 (비계절조정)',
    'LNU02500000': '풀타임 취업자 (비계절조정)',
    'LNU02600000': '파트타임 취업자 (비계절조정)',
    'LNU03000000': '실업자수 (비계절조정)',
    'LNU03100000': '풀타임 구직자 (비계절조정)',
    'LNU03200000': '파트타임 구직자 (비계절조정)',
    'LNU04000000': '실업률 (비계절조정)',
    'LNU04100000': '풀타임 실업률 (비계절조정)',
    'LNU04200000': '파트타임 실업률 (비계절조정)',
    'LNU05000000': '비경제활동인구 (비계절조정)'
}

# CPS 카테고리 분류
CPS_CATEGORIES = {
    '핵심지표': {
        '주요 지표': ['LNS14000000', 'LNS11300000', 'LNS12300000'],
        '고용 현황': ['LNS12000000', 'LNS13000000', 'LNS11000000'],
        '광의 실업률': ['LNS14000000', 'LNS13327709']
    },
    '인구통계별': {
        '연령별': ['LNS14000012', 'LNS14000025', 'LNS14000026'],
        '인종별': ['LNS14000003', 'LNS14000006', 'LNS14032183', 'LNS14000009'],
        '교육수준별': ['LNS14027659', 'LNS14027660', 'LNS14027689', 'LNS14027662']
    },
    '연령대별분석': {
        '16세이상전체': ['LNS11000000', 'LNS11300000', 'LNS12000000', 'LNS13000000', 'LNS14000000', 'LNS15000000'],
        '18세이상': ['LNU01076975', 'LNU01376975', 'LNU02076975', 'LNU03076975', 'LNU04076975', 'LNU05076975'],
        '20세이상': ['LNS11000024', 'LNS11300024', 'LNS12000024', 'LNS13000024', 'LNS14000024'],
        '25세이상': ['LNS11000048', 'LNS11300048', 'LNS12000048', 'LNS13000048', 'LNS14000048'],
        '25세이상비계절조정': ['LNU01000048', 'LNU01300048', 'LNU02000048', 'LNU03000048', 'LNU04000048'],
        '45세이상': ['LNU01000092', 'LNU01300092', 'LNU02000092', 'LNU03000092', 'LNU04000092', 'LNU05000092'],
        '55세이상': ['LNS11024230', 'LNS11324230', 'LNS12024230', 'LNS13024230', 'LNS14024230'],
        '55세이상비계절조정': ['LNU01024230', 'LNU01324230', 'LNU02024230', 'LNU03024230', 'LNU04024230', 'LNU05024230'],
        '65세이상': ['LNU01000097', 'LNU01300097', 'LNU02000097', 'LNU03000097', 'LNU04000097', 'LNU05000097']
    },
    '인종별분석': {
        '백인': ['LNS14000003', 'LNU01000003', 'LNU01300003', 'LNU02000003', 'LNU03000003', 'LNU04000003', 'LNU05000003'],
        '흑인': ['LNS14000006', 'LNU01000006', 'LNU01300006', 'LNU02000006', 'LNU03000006', 'LNU04000006', 'LNU05000006'],
        '아시아계': ['LNS14032183', 'LNU01032183', 'LNU01332183', 'LNU02032183', 'LNU03032183', 'LNU04032183', 'LNU05032183'],
        '아메리카원주민': ['LNU01035243', 'LNU01335243', 'LNU02035243', 'LNU03035243', 'LNU04035243', 'LNU05035243'],
        '하와이태평양원주민': ['LNU01035553', 'LNU01335553', 'LNU02035553', 'LNU03035553', 'LNU04035553', 'LNU05035553'],
        '히스패닉라티노': ['LNS14000009', 'LNS11000009', 'LNS11300009', 'LNS12000009', 'LNS13000009', 'LNS15000009']
    },
    '계절조정비교': {
        '경제활동인구': ['LNS11000000', 'LNU01000000'],
        '경제활동참가율': ['LNS11300000', 'LNU01300000'],
        '취업자수': ['LNS12000000', 'LNU02000000'],
        '실업자수': ['LNS13000000', 'LNU03000000'],
        '실업률': ['LNS14000000', 'LNU04000000'],
        '비경제활동인구': ['LNS15000000', 'LNU05000000']
    },
    '실업분석': {
        '실업기간': ['LNS13008396', 'LNS13008756', 'LNS13008516', 'LNS13008636'],
        '실업기간통계': ['LNS13008275', 'LNS13008276'],
        '실업유형': ['LNS13023621', 'LNS13023653', 'LNS13025699', 'LNS13023705']
    },
    '고용형태': {
        '풀타임/파트타임': ['LNS12500000', 'LNS12600000', 'LNS12032194'],
        '복수직업': ['LNS12026619', 'LNS12026620']
    },
    '노동시장참여': {
        '비경제활동': ['LNS15000000', 'LNS15026642', 'LNS15026645'],
        '진입/이탈': ['LNS13023557', 'LNS13023569']
    }
}

# %%
# === 전역 변수 ===
CSV_FILE_PATH = '/home/jyp0615/us_eco/data/cps_data.csv'
CPS_DATA = {}

# %%
# === 데이터 로드 함수 ===
def load_cps_data(start_date='2020-01-01', smart_update=True, force_reload=False):
    """통합 함수 사용한 CPS 데이터 로드"""
    global CPS_DATA

    # CPS_SERIES를 {id: id} 형태로 변환 (load_economic_data가 예상하는 형태)
    series_dict = {series_id: series_id for series_id in CPS_SERIES.keys()}

    result = load_economic_data(
        series_dict=series_dict,
        data_source='BLS',
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
        tolerance=1000.0  # 고용 데이터 허용 오차
    )

    if result:
        CPS_DATA = result
        print_load_info()
        return True
    else:
        print("❌ CPS 데이터 로드 실패")
        return False

def print_load_info():
    """CPS 데이터 로드 정보 출력"""
    if not CPS_DATA or 'load_info' not in CPS_DATA:
        print("⚠️ 로드된 데이터가 없습니다.")
        return
    
    info = CPS_DATA['load_info']
    print(f"\n📊 CPS 데이터 로드 완료!")
    print(f"   📅 로드 시간: {info['load_time'].strftime('%Y-%m-%d %H:%M:%S') if info['load_time'] else 'N/A'}")
    print(f"   📈 시리즈 개수: {info['series_count']}개")
    print(f"   📊 데이터 포인트: {info['data_points']}개")
    print(f"   🎯 시작 날짜: {info['start_date']}")
    print(f"   🔗 데이터 소스: {info['source']}")
    
    if 'raw_data' in CPS_DATA and not CPS_DATA['raw_data'].empty:
        latest_date = CPS_DATA['raw_data'].index[-1].strftime('%Y-%m-%d')
        print(f"   📅 최신 데이터: {latest_date}")

# %%
# === 범용 시각화 함수 ===
def plot_cps_series_advanced(series_list, chart_type='multi_line', 
                            data_type='mom', periods=None, target_date=None):
    """범용 CPS 시각화 함수 - plot_economic_series 활용"""
    if not CPS_DATA:
        print("⚠️ 먼저 load_cps_data()를 실행하세요.")
        return None

    return plot_economic_series(
        data_dict=CPS_DATA,
        series_list=series_list,
        chart_type=chart_type,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=CPS_KOREAN_NAMES
    )

# %%
# === 데이터 Export 함수 ===
def export_cps_data(series_list, data_type='mom', periods=None, 
                   target_date=None, export_path=None, file_format='excel'):
    """CPS 데이터 export 함수 - export_economic_data 활용"""
    if not CPS_DATA:
        print("⚠️ 먼저 load_cps_data()를 실행하세요.")
        return None

    return export_economic_data(
        data_dict=CPS_DATA,
        series_list=series_list,
        data_type=data_type,
        periods=periods,
        target_date=target_date,
        korean_names=CPS_KOREAN_NAMES,
        export_path=export_path,
        file_format=file_format
    )

# %%
# === 데이터 접근 함수들 ===

def clear_cps_data():
    """CPS 데이터 초기화"""
    global CPS_DATA
    CPS_DATA = {}
    print("🗑️ CPS 데이터가 초기화되었습니다")

def get_raw_data(series_names=None):
    """원본 레벨 데이터 반환"""
    if not CPS_DATA or 'raw_data' not in CPS_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_cps_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return CPS_DATA['raw_data'].copy()
    
    available_series = [s for s in series_names if s in CPS_DATA['raw_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return CPS_DATA['raw_data'][available_series].copy()

def get_mom_data(series_names=None):
    """전월대비 변화 데이터 반환"""
    if not CPS_DATA or 'mom_data' not in CPS_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_cps_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return CPS_DATA['mom_data'].copy()
    
    available_series = [s for s in series_names if s in CPS_DATA['mom_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return CPS_DATA['mom_data'][available_series].copy()

def get_yoy_data(series_names=None):
    """전년동월대비 변화 데이터 반환"""
    if not CPS_DATA or 'yoy_data' not in CPS_DATA:
        print("⚠️ 데이터가 로드되지 않았습니다. load_cps_data()를 먼저 실행하세요.")
        return pd.DataFrame()
    
    if series_names is None:
        return CPS_DATA['yoy_data'].copy()
    
    available_series = [s for s in series_names if s in CPS_DATA['yoy_data'].columns]
    if not available_series:
        print(f"⚠️ 요청한 시리즈가 없습니다: {series_names}")
        return pd.DataFrame()
    
    return CPS_DATA['yoy_data'][available_series].copy()

def list_available_series():
    """사용 가능한 시리즈 목록 반환"""
    if not CPS_DATA or 'raw_data' not in CPS_DATA:
        return []
    return list(CPS_DATA['raw_data'].columns)

# %%
# === 유틸리티 함수들 ===

def show_available_series():
    """사용 가능한 CPS 시리즈 표시"""
    print("=== 사용 가능한 CPS 시리즈 ===")
    
    for series_id, description in CPS_SERIES.items():
        korean_name = CPS_KOREAN_NAMES.get(series_id, description)
        print(f"  '{series_id}': {korean_name} ({description})")

def show_category_options():
    """사용 가능한 카테고리 옵션 표시"""
    print("=== 사용 가능한 카테고리 ===")
    for category, groups in CPS_CATEGORIES.items():
        print(f"\n{category}:")
        for group_name, series_list in groups.items():
            print(f"  {group_name}: {len(series_list)}개 시리즈")
            for series_id in series_list:
                korean_name = CPS_KOREAN_NAMES.get(series_id, series_id)
                print(f"    - {series_id}: {korean_name}")

def get_data_status():
    """현재 데이터 상태 반환"""
    if not CPS_DATA or 'load_info' not in CPS_DATA:
        return {
            'loaded': False,
            'series_count': 0,
            'available_series': [],
            'load_info': {'loaded': False}
        }
    return {
        'loaded': CPS_DATA['load_info']['loaded'],
        'series_count': CPS_DATA['load_info']['series_count'],
        'available_series': list_available_series(),
        'load_info': CPS_DATA['load_info']
    }

# %%
# === 사용 예시 ===

print("=== 리팩토링된 CPS 분석 도구 사용법 ===")
print("1. 데이터 로드:")
print("   load_cps_data()  # 스마트 업데이트")
print("   load_cps_data(force_reload=True)  # 강제 재로드")
print()
print("2. 🔥 범용 시각화 (가장 강력!):")
print("   plot_cps_series_advanced(['LNS14000000', 'LNS14000003'], 'multi_line', 'mom')")
print("   plot_cps_series_advanced(['LNS14000000'], 'horizontal_bar', 'yoy')")
print("   plot_cps_series_advanced(['LNS11000000'], 'single_line', 'mom', periods=24)")
print()
print("3. 🔥 데이터 Export:")
print("   export_cps_data(['LNS14000000', 'LNS14000003'], 'mom')")
print("   export_cps_data(['LNS11000000'], 'raw', periods=24, file_format='csv')")
print("   export_cps_data(['LNS14000000'], 'yoy', target_date='2024-06-01')")
print()
print("✅ plot_cps_series_advanced()는 어떤 시리즈든 원하는 형태로 시각화!")
print("✅ export_cps_data()는 시각화와 동일한 데이터를 엑셀/CSV로 export!")
print("✅ 모든 함수가 us_eco_utils의 통합 함수 사용!")

# %%
load_cps_data()
plot_cps_series_advanced(['LNS14000000', 'LNS14000003'], 'multi_line', 'mom')
# %%
plot_cps_series_advanced(['LNU05024230', 'LNU03000000'], 'multi_line', 'yoy')

# %%
plot_cps_series_advanced(['LNU05024230', 'LNU03000000'], 'dual_axis', 'raw')

# %%
export_cps_data(['LNU05024230', 'LNU03000000'], 'raw')

# %%
