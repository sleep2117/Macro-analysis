"""
Global indices/sectors/factors universe with data validation
"""

# Note: file moved under global_universe/ for project organization

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Ticker validation functions
def validate_ticker(symbol, min_days=252):
    """
    Validate if ticker has sufficient data on Yahoo Finance
    Returns: dict with validation results
    """
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="2y")
        
        if hist.empty:
            return {
                'symbol': symbol,
                'valid': False,
                'days_available': 0,
                'last_date': None,
                'error': 'No data available'
            }
        
        days_available = len(hist)
        last_date = hist.index[-1].strftime('%Y-%m-%d') if not hist.empty else None
        
        return {
            'symbol': symbol,
            'valid': days_available >= min_days,
            'days_available': days_available,
            'last_date': last_date,
            'error': None if days_available >= min_days else f'Insufficient data: {days_available} days'
        }
    except Exception as e:
        return {
            'symbol': symbol,
            'valid': False,
            'days_available': 0,
            'last_date': None,
            'error': str(e)
        }

def validate_universe(universe_dict):
    """
    Validate all tickers in the investment universe
    Returns: dict with validation results
    """
    results = {}
    
    for country, country_data in universe_dict.items():
        results[country] = {'sectors': {}, 'factors': {}}
        
        # Validate sectors
        if 'sectors' in country_data:
            for sector, sector_data in country_data['sectors'].items():
                tickers_to_check = []
                if 'index' in sector_data and sector_data['index']:
                    tickers_to_check.append(('index', sector_data['index']))
                if 'etf' in sector_data and sector_data['etf']:
                    tickers_to_check.append(('etf', sector_data['etf']))
                
                results[country]['sectors'][sector] = {}
                for ticker_type, ticker in tickers_to_check:
                    results[country]['sectors'][sector][ticker_type] = validate_ticker(ticker)
        
        # Validate factors
        if 'factors' in country_data:
            for factor, factor_data in country_data['factors'].items():
                tickers_to_check = []
                if 'index' in factor_data and factor_data['index']:
                    tickers_to_check.append(('index', factor_data['index']))
                if 'etf' in factor_data and factor_data['etf']:
                    tickers_to_check.append(('etf', factor_data['etf']))
                
                results[country]['factors'][factor] = {}
                for ticker_type, ticker in tickers_to_check:
                    results[country]['factors'][factor][ticker_type] = validate_ticker(ticker)
    
    return results

investment_universe = {
    'US': {
        'currency': 'USD',
        'sectors': {
            'Technology':    {'index': None, 'etf': 'XLK',  'currency': 'USD', 'valuation_data': True,  'alternatives': ['VGT','FTEC']},
            'Healthcare':    {'index': None, 'etf': 'XLV',  'currency': 'USD', 'valuation_data': True,  'alternatives': ['VHT','FHLC']},
            'Financials':    {'index': None, 'etf': 'XLF',  'currency': 'USD', 'valuation_data': True,  'alternatives': ['VFH']},
            'Cons_Discr.':   {'index': None, 'etf': 'XLY',  'currency': 'USD', 'valuation_data': True,  'alternatives': ['VCR']},
            'Cons_Staples':  {'index': None, 'etf': 'XLP',  'currency': 'USD', 'valuation_data': True,  'alternatives': ['VDC']},
            'Industrials':   {'index': None, 'etf': 'XLI',  'currency': 'USD', 'valuation_data': True,  'alternatives': ['VIS']},
            'Energy':        {'index': None, 'etf': 'XLE',  'currency': 'USD', 'valuation_data': True,  'alternatives': ['VDE']},
            'Materials':     {'index': None, 'etf': 'XLB',  'currency': 'USD', 'valuation_data': True,  'alternatives': ['VAW']},
            'Utilities':     {'index': None, 'etf': 'XLU',  'currency': 'USD', 'valuation_data': True,  'alternatives': ['VPU']},
            'Comm_Services': {'index': None, 'etf': 'XLC',  'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Real_Estate':   {'index': None, 'etf': 'XLRE', 'currency': 'USD', 'valuation_data': True,  'alternatives': ['VNQ']}
        },
        'factors': {
            'Large_Cap':        {'index': '^GSPC',  'currency': 'USD', 'valuation_data': False, 'alternatives': ['SPY','VOO']},
            'Mid_Cap':          {'index': '^SP400', 'currency': 'USD', 'valuation_data': False, 'alternatives': ['IJH']},
            'Small_Cap':        {'index': '^RUT',   'currency': 'USD', 'valuation_data': False, 'alternatives': ['IWM','IJR']},
            'Small_Cap_600':    {'index': '^SP600', 'currency': 'USD', 'valuation_data': False, 'alternatives': ['IJR','SLY','VIOO']},
            'Nasdaq_100':       {'index': '^NDX',   'currency': 'USD', 'valuation_data': False, 'alternatives': ['QQQ']},
            'Micro_Cap':        {'index': None,     'etf': 'IWC',      'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Small_Value':      {'index': None,     'etf': 'IJS',      'currency': 'USD', 'valuation_data': True,  'alternatives': ['VBR','RZV']},
            'Small_Growth':     {'index': None,     'etf': 'IJT',      'currency': 'USD', 'valuation_data': True,  'alternatives': ['VBK','RZG']},
            'Mid_Value':        {'index': None,     'etf': 'IWS',      'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Mid_Growth':       {'index': None,     'etf': 'IWP',      'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Equal_Weight':     {'index': None,     'etf': 'RSP',      'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Low_Vol':          {'index': None,     'etf': 'USMV',     'currency': 'USD', 'valuation_data': True,  'alternatives': ['SPLV']},
            'High_Beta':        {'index': None,     'etf': 'SPHB',     'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Low_Beta':         {'index': None,     'etf': 'SPLV',     'currency': 'USD', 'valuation_data': True,  'alternatives': ['USMV']},
            'High_Dividend':    {'index': None,     'etf': 'VYM',      'currency': 'USD', 'valuation_data': True,  'alternatives': ['HDV','SCHD','DVY']},
            'Dividend_Growth':  {'index': None,     'etf': 'VIG',      'currency': 'USD', 'valuation_data': True,  'alternatives': ['DGRO','DGRW']},
            'Value':            {'index': None,     'etf': 'IUSV',     'currency': 'USD', 'valuation_data': True,  'alternatives': ['VLUE','VTV','IVE']},
            'Growth':           {'index': None,     'etf': 'IUSG',     'currency': 'USD', 'valuation_data': True,  'alternatives': ['IVW','VUG']},
            'Momentum':         {'index': None,     'etf': 'MTUM',     'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Quality':          {'index': None,     'etf': 'QUAL',     'currency': 'USD', 'valuation_data': True,  'alternatives': ['SPHQ']},
            'High_Quality':     {'index': None,     'etf': 'SPHQ',     'currency': 'USD', 'valuation_data': True,  'alternatives': ['QUAL','QLTY']},
            'Profitability':    {'index': None,     'etf': 'COWZ',     'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Multi_Factor':     {'index': None,     'etf': 'LRGF',     'currency': 'USD', 'valuation_data': True,  'alternatives': []}
        }
    },

    'Europe': {
        'currency': 'EUR',
        'sectors': {
            # Technology: use ETF as primary (index has short history on Yahoo)
            'Technology':    {'index': None,     'etf': 'EXV3.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': ['SX8P.Z']},
            'Healthcare':    {'index': 'SXDP.Z', 'etf': 'EXV4.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Utilities':     {'index': 'SX6P.Z', 'etf': 'EXH9.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Telecom':       {'index': 'SXKP.Z', 'etf': 'EXV2.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Automobiles':   {'index': 'SXAP.Z', 'etf': 'EXV5.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Basic_Resrcs':  {'index': 'SXPP.Z', 'etf': 'EXV6.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Chemicals':     {'index': 'SX4P.Z', 'etf': 'EXV7.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            # Banks: use ETF as primary (index has short history on Yahoo)
            'Banks':         {'index': None,     'etf': 'EXV1.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': ['SX7P.Z']},
            'Insurance':     {'index': 'SXIP.Z', 'etf': 'EXH5.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Industrials':   {'index': 'SXNP.Z', 'etf': 'EXH4.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Construction':  {'index': 'SXOP.Z', 'etf': 'EXV8.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {
            'Large_Cap':     {'index': '^STOXX', 'currency': 'EUR', 'valuation_data': False, 'alternatives': ['EXSA.DE','DX2X.DE']},
            'Small_Cap(EMU)':{'index': None, 'etf': 'SMEA.MI', 'currency':'EUR','valuation_data':True,'alternatives': ['IEUS']},
            'Value':         {'index': None, 'etf': 'IEVL.MI','currency':'EUR','valuation_data':True,'alternatives':['CEMS.DE']},
            'Momentum':      {'index': None, 'etf': 'CEMR.DE','currency':'EUR','valuation_data':True,'alternatives': []},
            'Quality':       {'index': None, 'etf': 'CEMQ.DE', 'currency':'EUR','valuation_data':True,'alternatives':['IEFQ.L']},
            'Min_Vol':       {'index': None, 'etf': 'EUMV',    'currency':'USD','valuation_data':True,'alternatives': []},
            'EuroStoxx50':   {'index': '^STOXX50E','currency': 'EUR','valuation_data': False,'alternatives': ['FEZ','EXW1.DE']},
        }
    },

    'Germany': {
        'currency': 'EUR',
        'sectors': {
            'Technology':   {'index': '^CXPHX','currency':'EUR','valuation_data':False, 'alternatives':['EXS2.DE']},
            'Healthcare':   {'index': '^CXPPX','currency':'EUR','valuation_data':False, 'alternatives': []},
            'Industrials':  {'index': '^CXPNX','currency':'EUR','valuation_data':False, 'alternatives': []},
            'Fin_Services': {'index': '^CXPVX','currency':'EUR','valuation_data':False, 'alternatives': []},
            'Insurance':    {'index': '^CXPIX','currency':'EUR','valuation_data':False, 'alternatives': []},
        },
        'factors': {
            'Large_Cap': {'index': '^GDAXI','currency':'EUR','valuation_data':False,'alternatives':['EXS1.DE']},
            'Mid_Cap':   {'index': '^MDAXI','currency':'EUR','valuation_data':False,'alternatives':['EXS3.DE']},
            'Small_Cap': {'index': '^SDAXI','currency':'EUR','valuation_data':False,'alternatives': []},
        }
    },

    'Japan': {
        'currency': 'JPY',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'EWJ',   'currency': 'USD', 'valuation_data': True, 'alternatives': ['DXJ','HEWJ','BBJP','JPXN']},
            'Nikkei_225':   {'index': '^N225','currency': 'JPY', 'valuation_data': False,'alternatives': ['EWJ']},
            # TOPIX-17 sector ETFs (JPY, Tokyo)
            'Foods':                     {'index': None, 'etf': '1617.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Energy_Resources':          {'index': None, 'etf': '1618.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Construction_Materials':    {'index': None, 'etf': '1619.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Raw_Materials_Chemicals':   {'index': None, 'etf': '1620.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Pharmaceuticals':           {'index': None, 'etf': '1621.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Autos_Transport_Equip':     {'index': None, 'etf': '1622.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Steel_Nonferrous':          {'index': None, 'etf': '1623.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Machinery':                 {'index': None, 'etf': '1624.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Electric_Precision':        {'index': None, 'etf': '1625.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'IT_Services_Others':        {'index': None, 'etf': '1626.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Electric_Power_Gas':        {'index': None, 'etf': '1627.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Transportation_Logistics':  {'index': None, 'etf': '1628.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Commercial_Wholesale':      {'index': None, 'etf': '1629.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Retail_Trade':              {'index': None, 'etf': '1630.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Banks':                     {'index': None, 'etf': '1631.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Financials_ex_Banks':       {'index': None, 'etf': '1632.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Real_Estate':               {'index': None, 'etf': '1633.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {
            'Large_Cap': {'index': None, 'etf': 'EWJ',  'currency': 'USD', 'valuation_data': True, 'alternatives': ['DXJ']},
            'Small_Cap': {'index': None, 'etf': 'SCJ',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Value':     {'index': None, 'etf': 'EWJV', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        }
    },

    'China': {
        'currency': 'CNY',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'FXI',  'currency': 'USD', 'valuation_data': True, 'alternatives': ['MCHI','ASHR','GXC']},
            'Technology':   {'index': None, 'etf': 'CQQQ', 'currency': 'USD', 'valuation_data': True, 'alternatives': ['KWEB']},
            'Consumer':     {'index': None, 'etf': 'CHIQ', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Cons_Staples': {'index': None, 'etf': 'CHIS', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Financials':   {'index': None, 'etf': 'CHIX', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Industrials':  {'index': None, 'etf': 'CHII', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Materials':    {'index': None, 'etf': 'CHIM', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Energy':       {'index': None, 'etf': 'CHIE', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Utilities':    {'index': None, 'etf': 'CHIU', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Comm_Services':{'index': None, 'etf': 'CHIC', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Real_Estate':  {'index': None, 'etf': 'CHIR', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Healthcare':   {'index': None, 'etf': 'CHIH', 'currency': 'USD', 'valuation_data': True, 'alternatives': ['KURE']},
        },
        'factors': {
            'Large_Cap': {'index': None, 'etf': 'FXI',  'currency': 'USD', 'valuation_data': True, 'alternatives': ['GXC']},
            'Small_Cap': {'index': None, 'etf': 'ECNS', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'A_Shares':  {'index': None, 'etf': 'ASHR', 'currency': 'USD', 'valuation_data': True, 'alternatives': ['CNYA','KBA']},
            'CSI_300':   {'index': '000300.SS','currency': 'CNY','valuation_data': False,'alternatives': ['ASHR']},
            'Growth':    {'index': None, 'etf': 'CNXT', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        }
    },

    'Canada': {
        'currency': 'CAD',
        'sectors': {
            'Broad_Market': {'index': '^GSPTSE', 'currency': 'CAD', 'valuation_data': False, 'alternatives': ['XIC.TO','EWC']},
        },
        'factors': {}
    },

    'Brazil': {
        'currency': 'BRL',
        'sectors': {
            'Broad_Market': {'index': '^BVSP', 'currency': 'BRL', 'valuation_data': False, 'alternatives': ['EWZ']},
        },
        'factors': {}
    },

    'Global_ExUS': {
        'currency': 'USD',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'ACWX', 'currency': 'USD', 'valuation_data': True, 'alternatives': ['VEU','VEA','IEFA']},
        },
        'factors': {}
    },

    'Singapore': {
        'currency': 'SGD',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'EWS', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'Indonesia': {
        'currency': 'IDR',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'EIDO', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'Thailand': {
        'currency': 'THB',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'THD', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'Malaysia': {
        'currency': 'MYR',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'EWM', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'India': {
        'currency': 'INR',
        'sectors': {
            'Broad_Market': {'index': '^NSEI', 'currency': 'INR', 'valuation_data': False, 'alternatives': ['INDA','EPI','INDY']},
            'Financials':   {'index': None,    'etf': 'INDF', 'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Consumer':     {'index': None,    'etf': 'INCO', 'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Technology':   {'index': None,    'etf': 'INQQ', 'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Small_Cap':    {'index': None,    'etf': 'SMIN', 'currency': 'USD', 'valuation_data': True,  'alternatives': ['SCIF']},
            'Value(Proxy)': {'index': None,    'etf': 'EPI',  'currency': 'USD', 'valuation_data': True,  'alternatives': []},
        },
        'factors': {}
    },

    'UK': {
        'currency': 'GBP',
        'sectors': {
            'Broad_Market': {'index': '^FTSE', 'currency': 'GBP', 'valuation_data': False, 'alternatives': ['ISF.L','VUKE.L']},
        },
        'factors': {}
    },

    'France': {
        'currency': 'EUR',
        'sectors': {
            'Broad_Market': {'index': '^FCHI', 'currency': 'EUR', 'valuation_data': False, 'alternatives': ['EWQ']},
        },
        'factors': {}
    },

    'Italy': {
        'currency': 'EUR',
        'sectors': {
            'Broad_Market': {'index': 'FTSEMIB.MI', 'currency': 'EUR', 'valuation_data': False, 'alternatives': ['EWI']},
        },
        'factors': {}
    },

    'Spain': {
        'currency': 'EUR',
        'sectors': {
            'Broad_Market': {'index': '^IBEX', 'currency': 'EUR', 'valuation_data': False, 'alternatives': ['EWP']},
        },
        'factors': {}
    },

    'Taiwan': {
        'currency': 'TWD',
        'sectors': {
            'Broad_Market':     {'index': '^TWII', 'currency': 'TWD', 'valuation_data': False, 'alternatives': ['EWT']},
        },
        'factors': {}
    },

    'Hong_Kong': {
        'currency': 'HKD',
        'sectors': {
            'Broad_Market': {'index': '^HSI', 'currency': 'HKD', 'valuation_data': False, 'alternatives': ['EWH']},
        },
        'factors': {}
    },
}

# 환율 페어 정의 (USD 기준 쿼트)
CURRENCY_PAIRS = {
    'USDEUR': 'EUR',
    'USDGBP': 'GBP',
    'USDJPY': 'JPY',
    'USDCNY': 'CNY',
    'USDSGD': 'SGD',
    'USDHKD': 'HKD',
    'USDTWD': 'TWD',
    'USDTHB': 'THB',
    'USDIDR': 'IDR',
    'USDMYR': 'MYR',
}

def get_best_ticker(asset_data):
    """
    Select the best available ticker from asset data
    Priority: Valid primary ticker > Valid alternatives > None
    """
    # Check primary index
    if 'index' in asset_data and asset_data['index']:
        validation = validate_ticker(asset_data['index'])
        if validation['valid']:
            return asset_data['index'], 'index'
    
    # Check primary ETF
    if 'etf' in asset_data and asset_data['etf']:
        validation = validate_ticker(asset_data['etf'])
        if validation['valid']:
            return asset_data['etf'], 'etf'
    
    # Check alternatives
    if 'alternatives' in asset_data and asset_data['alternatives']:
        for alt_ticker in asset_data['alternatives']:
            validation = validate_ticker(alt_ticker)
            if validation['valid']:
                return alt_ticker, 'alternative'
    
    # Return primary ticker even if invalid (for debugging)
    if 'index' in asset_data and asset_data['index']:
        return asset_data['index'], 'invalid'
    elif 'etf' in asset_data and asset_data['etf']:
        return asset_data['etf'], 'invalid'
    
    return None, 'none'

def create_validated_universe():
    """
    Create a new universe with only validated tickers
    """
    validated_universe = {}
    
    for country, country_data in investment_universe.items():
        validated_universe[country] = {
            'currency': country_data['currency'],
            'sectors': {},
            'factors': {}
        }
        
        # Process sectors
        if 'sectors' in country_data:
            for sector, asset_data in country_data['sectors'].items():
                best_ticker, ticker_type = get_best_ticker(asset_data)
                if best_ticker:
                    validated_universe[country]['sectors'][sector] = {
                        'ticker': best_ticker,
                        'type': ticker_type,
                        'currency': asset_data['currency'],
                        'original_data': asset_data
                    }
        
        # Process factors
        if 'factors' in country_data:
            for factor, asset_data in country_data['factors'].items():
                best_ticker, ticker_type = get_best_ticker(asset_data)
                if best_ticker:
                    validated_universe[country]['factors'][factor] = {
                        'ticker': best_ticker,
                        'type': ticker_type,
                        'currency': asset_data['currency'],
                        'original_data': asset_data
                    }
    
    return validated_universe

def validate_all_tickers():
    """
    Run comprehensive validation on all tickers in the universe
    """
    print("Starting ticker validation...")
    validation_results = validate_universe(investment_universe)
    
    # Summary report
    total_tickers = 0
    valid_tickers = 0
    invalid_tickers = []
    
    for country, country_results in validation_results.items():
        print(f"\n=== {country} ===")
        
        for category in ['sectors', 'factors']:
            if category in country_results:
                for name, ticker_results in country_results[category].items():
                    for ticker_type, result in ticker_results.items():
                        total_tickers += 1
                        if result['valid']:
                            valid_tickers += 1
                            print(f"✓ {name} ({ticker_type}): {result['symbol']} - {result['days_available']} days")
                        else:
                            invalid_tickers.append(result)
                            print(f"✗ {name} ({ticker_type}): {result['symbol']} - {result['error']}")
    
    print(f"\n=== SUMMARY ===")
    print(f"Total tickers: {total_tickers}")
    print(f"Valid tickers: {valid_tickers}")
    print(f"Invalid tickers: {len(invalid_tickers)}")
    print(f"Success rate: {valid_tickers/total_tickers*100:.1f}%")
    
    return validation_results

# Add data collection functions
def collect_data(ticker, period='2y'):
    """
    Collect historical data for a ticker
    """
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period=period)
        info = stock.info
        
        return {
            'ticker': ticker,
            'data': hist,
            'info': info,
            'success': True,
            'error': None
        }
    except Exception as e:
        return {
            'ticker': ticker,
            'data': None,
            'info': None,
            'success': False,
            'error': str(e)
        }

def collect_universe_data(validated_universe=None, period='2y'):
    """
    Collect data for all tickers in the validated universe
    """
    if validated_universe is None:
        validated_universe = create_validated_universe()
    
    data_collection = {}
    
    for country, country_data in validated_universe.items():
        data_collection[country] = {
            'currency': country_data['currency'],
            'sectors': {},
            'factors': {}
        }
        
        # Collect sector data
        for sector, asset_info in country_data.get('sectors', {}).items():
            ticker = asset_info['ticker']
            data_result = collect_data(ticker, period)
            data_collection[country]['sectors'][sector] = {
                **asset_info,
                **data_result
            }
        
        # Collect factor data
        for factor, asset_info in country_data.get('factors', {}).items():
            ticker = asset_info['ticker']
            data_result = collect_data(ticker, period)
            data_collection[country]['factors'][factor] = {
                **asset_info,
                **data_result
            }
    
    return data_collection

# ============================
# Daily CSV Cache (New)
# ============================
import os
from pathlib import Path as _Path
import time
import re
import json as _json
import subprocess as _sp

_BASE_DIR = _Path(__file__).resolve().parent
_DATA_DIR = _BASE_DIR / "data" / "daily"
_DATA_DIR.mkdir(parents=True, exist_ok=True)

def _sanitize_symbol(symbol: str) -> str:
    """Filesystem-safe filename for a Yahoo symbol."""
    if symbol.startswith('^'):
        symbol = 'IDX_' + symbol[1:]
    return re.sub(r"[^A-Za-z0-9._-]", "_", symbol)

def _csv_path_for(symbol: str) -> _Path:
    return _DATA_DIR / f"{_sanitize_symbol(symbol)}.csv"

def _load_existing_csv(path: _Path):
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, parse_dates=["Date"], index_col="Date")
        df = df[~df.index.duplicated(keep='last')].sort_index()
        return df
    except Exception:
        return None

def _fetch_history(symbol: str, start: datetime | None = None, end: datetime | None = None, max_retries: int = 3, pause: float = 0.5) -> pd.DataFrame:
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            t = yf.Ticker(symbol)
            if start is None and end is None:
                # Try a sequence of periods from longest to shortest; some symbols only allow 1d/5d
                periods = ["max", "10y", "5y", "2y", "1y", "6mo", "3mo", "1mo", "5d", "1d"]
                hist = pd.DataFrame()
                last_exc = None
                for p in periods:
                    try:
                        h = t.history(period=p, interval="1d")
                        if h is not None and not h.empty:
                            hist = h
                            break
                    except Exception as e:
                        last_exc = e
                        continue
                if hist is None or hist.empty:
                    # yfinance failed (common on CI). Fallback to Yahoo chart API via curl
                    for p in periods:
                        h = _fetch_history_via_chart(symbol, period=p)
                        if h is not None and not h.empty:
                            hist = h
                            break
                    if hist is None or hist.empty:
                        if last_exc:
                            raise last_exc
                        return pd.DataFrame()
            else:
                # Incremental fetch with explicit end to avoid start > end errors
                today = datetime.utcnow()
                if end is None:
                    end = today
                if start is not None and start.date() > end.date():
                    # Nothing to fetch yet
                    return pd.DataFrame()
                kwargs = {"interval": "1d", "start": start.strftime("%Y-%m-%d") if start else None, "end": end.strftime("%Y-%m-%d") if end else None}
                # Remove Nones
                kwargs = {k: v for k, v in kwargs.items() if v is not None}
                try:
                    hist = t.history(**kwargs)
                except Exception:
                    # As a fallback, try a short period if range query fails
                    hist = t.history(period="5d", interval="1d")
                    if hist is None or hist.empty:
                        # fallback to chart api for a short period
                        hist = _fetch_history_via_chart(symbol, period="5d")
            if not hist.empty:
                cols = [c.strip().title() for c in hist.columns]
                hist.columns = cols
                keep = [c for c in ["Open","High","Low","Close","Adj Close","Volume"] if c in hist.columns]
                hist = hist[keep]
            return hist
        except Exception as e:
            last_err = e
            time.sleep(min(10, pause * (2 ** (attempt - 1))))
    raise RuntimeError(f"Failed to fetch history for {symbol}: {last_err}")

def update_symbol_csv(symbol: str, pause: float = 0.6) -> tuple[_Path, int]:
    path = _csv_path_for(symbol)
    existing = _load_existing_csv(path)
    if existing is None or existing.empty:
        df = _fetch_history(symbol, pause=pause)
        if df is None or df.empty:
            # write empty placeholder
            pd.DataFrame(columns=["Open","High","Low","Close","Adj Close","Volume"]).to_csv(path)
            return path, 0
        tmp = path.with_suffix('.csv.tmp')
        df.to_csv(tmp, index_label="Date")
        os.replace(tmp, path)
        return path, len(df)
    last = existing.index.max()
    start = last + timedelta(days=1)
    new = _fetch_history(symbol, start=start, pause=pause)
    if new is None or new.empty:
        return path, 0
    combined = pd.concat([existing, new])
    combined = combined[~combined.index.duplicated(keep='last')].sort_index()
    tmp = path.with_suffix('.csv.tmp')
    combined.to_csv(tmp, index_label="Date")
    os.replace(tmp, path)
    return path, len(new)

def list_all_symbols(universe: dict, include_alternatives: bool = True) -> list[str]:
    symbols: list[str] = []
    for _, country_data in universe.items():
        for section in ("sectors", "factors"):
            for _, asset in country_data.get(section, {}).items():
                for key in ("index", "etf"):
                    v = asset.get(key)
                    if v:
                        symbols.append(str(v))
                if include_alternatives:
                    for alt in asset.get("alternatives", []) or []:
                        if alt:
                            symbols.append(str(alt))
    # dedupe keep order
    seen = set(); out = []
    for s in symbols:
        if s not in seen:
            seen.add(s); out.append(s)
    return out

def list_primary_symbols(universe: dict) -> list[str]:
    """
    Return one primary symbol per asset: prefer 'index' if present else 'etf'.
    Skips alternatives.
    """
    symbols: list[str] = []
    for _, country_data in universe.items():
        for section in ("sectors", "factors"):
            for _, asset in country_data.get(section, {}).items():
                sym = asset.get("index") or asset.get("etf")
                if sym:
                    symbols.append(str(sym))
    # Deduplicate preserving order
    seen = set(); out = []
    for s in symbols:
        if s not in seen:
            seen.add(s); out.append(s)
    return out

def build_symbols_catalog(universe: dict, primary_only: bool = True) -> pd.DataFrame:
    rows = []
    for country, cdata in universe.items():
        for section in ("sectors", "factors"):
            for name, asset in cdata.get(section, {}).items():
                if primary_only:
                    key = "index" if asset.get("index") else ("etf" if asset.get("etf") else None)
                    if key:
                        v = asset[key]
                        rows.append({
                            "country": country,
                            "category": section,
                            "name": name,
                            "field": key,
                            "symbol": v,
                            "currency": asset.get("currency"),
                            "file": str(_csv_path_for(v)),
                        })
                else:
                    for key in ("index", "etf"):
                        v = asset.get(key)
                        if v:
                            rows.append({
                                "country": country,
                                "category": section,
                                "name": name,
                                "field": key,
                                "symbol": v,
                                "currency": asset.get("currency"),
                                "file": str(_csv_path_for(v)),
                            })
                    for alt in asset.get("alternatives", []) or []:
                        rows.append({
                            "country": country,
                            "category": section,
                            "name": name,
                            "field": "alternative",
                            "symbol": alt,
                            "currency": asset.get("currency"),
                            "file": str(_csv_path_for(alt)),
                        })
    df = pd.DataFrame(rows).drop_duplicates(subset=["field","symbol"]).reset_index(drop=True)
    (_BASE_DIR / "data").mkdir(exist_ok=True)
    df.to_csv(_BASE_DIR / "data" / "symbols_catalog.csv", index=False)
    return df

def _fetch_history_via_chart(symbol: str, period: str = "max", interval: str = "1d") -> pd.DataFrame | None:
    """Fallback downloader using Yahoo chart API via curl with browser-like headers.
    Returns OHLCV (+ Adj Close if available) DataFrame or None.
    """
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={period}&interval={interval}"
        headers = [
            "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "-H", "Accept: application/json, text/plain, */*",
            "-H", f"Referer: https://finance.yahoo.com/quote/{symbol}",
            "-H", "Accept-Language: en-US,en;q=0.9",
        ]
        out = _sp.check_output(["curl", "-s", *headers, url], timeout=20)
        if not out:
            return None
        data = _json.loads(out.decode("utf-8", errors="ignore"))
        result = (data or {}).get("chart", {}).get("result")
        if not result:
            return None
        res = result[0]
        ts = res.get("timestamp") or []
        if not ts:
            return None
        import numpy as _np
        idx = pd.to_datetime(_np.array(ts, dtype="int64"), unit="s", utc=True).tz_convert(None)
        ind = res.get("indicators", {})
        quote = (ind.get("quote") or [{}])[0]
        open_ = quote.get("open")
        high_ = quote.get("high")
        low_ = quote.get("low")
        close_ = quote.get("close")
        vol_ = quote.get("volume")
        df = pd.DataFrame({
            "Open": open_,
            "High": high_,
            "Low": low_,
            "Close": close_,
            "Volume": vol_,
        }, index=idx)
        adj = (ind.get("adjclose") or [{}])[0].get("adjclose")
        if adj is not None:
            df["Adj Close"] = adj
        # Clean
        df = df.dropna(how="all")
        if "Adj Close" in df.columns:
            cols = ["Open","High","Low","Close","Adj Close","Volume"]
            df = df[[c for c in cols if c in df.columns]]
        return df
    except Exception:
        return None

def update_all_daily_data(universe: dict, pause: float = 0.6, symbols: list[str] | None = None) -> pd.DataFrame:
    if symbols is None:
        symbols = list_primary_symbols(universe)
    results = []
    for sym in symbols:
        try:
            path, added = update_symbol_csv(sym, pause=pause)
            results.append({"symbol": sym, "file": str(path), "added": added, "status": "ok"})
        except Exception as e:
            results.append({"symbol": sym, "file": str(_csv_path_for(sym)), "added": 0, "status": f"error: {e}"})
        time.sleep(pause)
    df = pd.DataFrame(results)
    df.to_csv(_BASE_DIR / "data" / "update_summary.csv", index=False)
    return df

# ============================
# Valuation Snapshot Cache (New)
# ============================

_VAL_DIR = _BASE_DIR / "data" / "valuations"
_VAL_DIR.mkdir(parents=True, exist_ok=True)

_VALUATION_FIELDS = [
    # Limit to the 3 consistently available fields
    "trailingPE",
    "priceToBook",
    "trailingAnnualDividendYield",
]

def _valuation_csv_path(symbol: str) -> _Path:
    return _VAL_DIR / f"{_sanitize_symbol(symbol)}.csv"

def fetch_valuation_snapshot(symbol: str) -> dict | None:
    """
    Fetch snapshot valuation metrics for a symbol using yfinance .info.
    Returns dict with available fields; None if unavailable.
    """
    try:
        t = yf.Ticker(symbol)
        info = t.info
        if not info or not isinstance(info, dict):
            return None
        row = {k: info.get(k) for k in _VALUATION_FIELDS}
        # If all values are None, treat as unavailable
        if all(v is None for v in row.values()):
            return None
        # Add metadata
        row.update({
            "symbol": symbol,
            "currency": info.get("currency"),
            "quoteType": info.get("quoteType"),
        })
        return row
    except Exception:
        return None

def list_primary_symbols_for_valuation(universe: dict) -> list[str]:
    """
    Prefer index; if the index lacks valuation metrics, fallback to ETF.
    Alternatives are not used here by design.
    """
    symbols: list[str] = []
    for _, country in universe.items():
        for section in ("sectors", "factors"):
            for _, asset in country.get(section, {}).items():
                primary = asset.get("index") or asset.get("etf")
                if primary:
                    symbols.append(str(primary))
    # Dedupe keep order
    seen = set(); out = []
    for s in symbols:
        if s not in seen:
            seen.add(s); out.append(s)
    return out

def update_valuation_csv(symbol: str) -> tuple[_Path, bool]:
    """
    Append today's valuation snapshot for symbol if not already present.
    Returns (path, appended?)
    """
    path = _valuation_csv_path(symbol)
    from datetime import datetime as _dt
    today = _dt.utcnow().date().isoformat()
    snap = fetch_valuation_snapshot(symbol)
    if snap is None:
        return path, False
    df_new = pd.DataFrame([{**{"Date": today}, **snap}])
    if path.exists():
        try:
            df = pd.read_csv(path)
            if (df["Date"] == today).any():
                return path, False
            df = pd.concat([df, df_new], ignore_index=True)
        except Exception:
            df = df_new
    else:
        df = df_new
    tmp = path.with_suffix(".csv.tmp")
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)
    return path, True

def update_all_valuations(universe: dict, pause: float = 0.2) -> pd.DataFrame:
    """
    Build/update daily valuation snapshots for each asset (one symbol per asset).
    Preference: index; if index has no valuation fields, fallback to ETF.
    No use of alternatives and no historical backfill (snapshot only).
    """
    rows = []
    for country, cdata in universe.items():
        for section in ("sectors", "factors"):
            for name, asset in cdata.get(section, {}).items():
                chosen = asset.get("index") or asset.get("etf")
                used_fallback = False
                path, updated = _valuation_csv_path(""), False  # init
                if chosen:
                    path, updated = update_valuation_csv(chosen)
                    if not updated and asset.get("index") and asset.get("etf") and asset.get("index") == chosen:
                        # fallback to ETF if index had no valuation data today
                        fb = asset.get("etf")
                        if fb:
                            path, updated = update_valuation_csv(fb)
                            used_fallback = True
                rows.append({
                    "country": country,
                    "category": section,
                    "name": name,
                    "primary": chosen,
                    "fallback_to_etf": used_fallback,
                    "file": str(path) if isinstance(path, _Path) else str(path),
                    "updated": updated,
                })
                time.sleep(pause)
    df = pd.DataFrame(rows)
    df.to_csv(_BASE_DIR / "data" / "valuations_update_summary.csv", index=False)
    return df

# Run data updater if script is executed
if __name__ == "__main__":
    print("Updating daily CSV cache for primary symbols only (index preferred)...")
    try:
        # Build catalog for primary symbols (index > ETF). No alternatives.
        build_symbols_catalog(investment_universe, primary_only=True)
        syms = list_primary_symbols(investment_universe)
        # Allow optional limit via env for testing speed
        import os as _os
        _max = _os.environ.get("MAX_SYMBOLS")
        if _max:
            syms = syms[:int(_max)]
        summary = update_all_daily_data(investment_universe, pause=0.3, symbols=syms)
        print("Price update complete. Summary saved to data/update_summary.csv")
        # Valuation snapshots (snapshot only, daily append)
        print("Updating valuation snapshots (primary symbols, with ETF fallback)...")
        vsummary = update_all_valuations(investment_universe, pause=0.05)
        print("Valuation update complete. Saved to data/valuations_update_summary.csv")
        print(vsummary.head())
    except Exception as e:
        print(f"Error during update: {e}")

# Import visualization functions
try:
    exec(open('/home/jyp0615/kpds_fig_format_enhanced.py').read())
    print("KPDS visualization library loaded successfully")
except Exception as e:
    print(f"Warning: Could not load KPDS visualization library: {e}")

# Analysis Functions
def calculate_returns(df_data, period_days=252):
    """
    Calculate various return metrics
    """
    returns = {}
    
    for period_name, days in [('1D', 1), ('1W', 7), ('1M', 30), ('3M', 90), ('6M', 180), ('1Y', 252), ('2Y', 504)]:
        if len(df_data) >= days:
            if days == 1:
                returns[period_name] = ((df_data.iloc[-1] / df_data.iloc[-2]) - 1) * 100
            else:
                returns[period_name] = ((df_data.iloc[-1] / df_data.iloc[-days]) - 1) * 100
        else:
            returns[period_name] = None
    
    return returns

def calculate_risk_metrics(df_data):
    """
    Calculate risk metrics (volatility, Sharpe ratio, MDD)
    """
    if len(df_data) < 30:
        return {'volatility': None, 'sharpe': None, 'max_drawdown': None}
    
    returns = df_data.pct_change().dropna()
    
    # Annualized volatility
    volatility = returns.std() * np.sqrt(252) * 100
    
    # Sharpe ratio (assuming 0% risk-free rate)
    sharpe = (returns.mean() * 252) / (returns.std() * np.sqrt(252))
    
    # Maximum drawdown
    cumulative = (1 + returns).cumprod()
    peak = cumulative.expanding().max()
    drawdown = (cumulative / peak) - 1
    max_drawdown = drawdown.min() * 100
    
    return {
        'volatility': volatility,
        'sharpe': sharpe,
        'max_drawdown': max_drawdown
    }

def perform_sector_analysis(data_collection, country='US'):
    """
    Perform comprehensive sector analysis for a specific country
    """
    print(f"Sector Analysis for {country}")
    
    if country not in data_collection:
        print(f"No data available for {country}")
        return None
    
    country_data = data_collection[country]
    sectors = country_data.get('sectors', {})
    
    if not sectors:
        print(f"No sector data available for {country}")
        return None
    
    analysis_results = {}
    
    # Collect all sector data into a DataFrame
    sector_prices = {}
    sector_returns = {}
    sector_risk_metrics = {}
    
    for sector_name, sector_info in sectors.items():
        if not sector_info['success'] or sector_info['data'] is None or sector_info['data'].empty:
            continue
            
        prices = sector_info['data']['Close']
        sector_prices[sector_name] = prices
        
        # Calculate returns and metrics
        returns_data = calculate_returns(prices)
        risk_data = calculate_risk_metrics(prices)
        
        sector_returns[sector_name] = returns_data
        sector_risk_metrics[sector_name] = risk_data
    
    if not sector_prices:
        print(f"No valid sector price data for {country}")
        return None
    
    # Create combined DataFrame
    price_df = pd.DataFrame(sector_prices)
    price_df = price_df.fillna(method='ffill').dropna()
    
    # Calculate normalized performance (rebased to 100)
    normalized_df = price_df / price_df.iloc[0] * 100
    
    analysis_results = {
        'price_data': price_df,
        'normalized_data': normalized_df,
        'returns': sector_returns,
        'risk_metrics': sector_risk_metrics,
        'currency': country_data['currency']
    }
    
    return analysis_results

def perform_factor_analysis(data_collection, country='US'):
    """
    Perform comprehensive factor analysis for a specific country
    """
    print(f"Factor Analysis for {country}")
    
    if country not in data_collection:
        print(f"No data available for {country}")
        return None
    
    country_data = data_collection[country]
    factors = country_data.get('factors', {})
    
    if not factors:
        print(f"No factor data available for {country}")
        return None
    
    analysis_results = {}
    
    # Collect all factor data
    factor_prices = {}
    factor_returns = {}
    factor_risk_metrics = {}
    
    for factor_name, factor_info in factors.items():
        if not factor_info['success'] or factor_info['data'] is None or factor_info['data'].empty:
            continue
            
        prices = factor_info['data']['Close']
        factor_prices[factor_name] = prices
        
        # Calculate returns and metrics
        returns_data = calculate_returns(prices)
        risk_data = calculate_risk_metrics(prices)
        
        factor_returns[factor_name] = returns_data
        factor_risk_metrics[factor_name] = risk_data
    
    if not factor_prices:
        print(f"No valid factor price data for {country}")
        return None
    
    # Create combined DataFrame
    price_df = pd.DataFrame(factor_prices)
    price_df = price_df.fillna(method='ffill').dropna()
    
    # Calculate normalized performance (rebased to 100)
    normalized_df = price_df / price_df.iloc[0] * 100
    
    analysis_results = {
        'price_data': price_df,
        'normalized_data': normalized_df,
        'returns': factor_returns,
        'risk_metrics': factor_risk_metrics,
        'currency': country_data['currency']
    }
    
    return analysis_results

def create_performance_summary(analysis_results):
    """
    Create performance summary table
    """
    returns_data = analysis_results['returns']
    risk_data = analysis_results['risk_metrics']
    
    summary_df = pd.DataFrame()
    
    for asset_name in returns_data.keys():
        asset_data = {
            '1D': returns_data[asset_name].get('1D'),
            '1W': returns_data[asset_name].get('1W'),
            '1M': returns_data[asset_name].get('1M'),
            '3M': returns_data[asset_name].get('3M'),
            '6M': returns_data[asset_name].get('6M'),
            '1Y': returns_data[asset_name].get('1Y'),
            '2Y': returns_data[asset_name].get('2Y'),
            'Volatility': risk_data[asset_name]['volatility'],
            'Sharpe': risk_data[asset_name]['sharpe'],
            'Max DD': risk_data[asset_name]['max_drawdown']
        }
        summary_df[asset_name] = asset_data
    
    return summary_df.T

def visualize_normalized_performance(analysis_results, title=None):
    """
    Visualize normalized performance using KPDS format
    """
    if 'df_multi_line_chart' not in globals():
        print("KPDS visualization library not available. Using basic plot.")
        analysis_results['normalized_data'].plot(figsize=(12, 8))
        return
    
    if title is None:
        title = "Normalized Performance Comparison"
    
    print(title)
    return df_multi_line_chart(
        analysis_results['normalized_data'],
        title=None,  # CLAUDE.md 규칙: 그래프 자체에는 메인 타이틀 X
        ytitle="Index (Base=100)"
    )

def generate_country_report(data_collection, country='US'):
    """
    Generate comprehensive country analysis report
    """
    print(f"\n{'='*60}")
    print(f"COMPREHENSIVE ANALYSIS REPORT - {country}")
    print(f"{'='*60}")
    
    # Sector Analysis
    print(f"\n>>> SECTOR ANALYSIS")
    sector_analysis = perform_sector_analysis(data_collection, country)
    
    if sector_analysis:
        print("\nSector Performance Summary:")
        sector_summary = create_performance_summary(sector_analysis)
        print(sector_summary.round(2))
        
        print("\nSector Performance Chart:")
        visualize_normalized_performance(sector_analysis, f"{country} Sector Performance")
    
    # Factor Analysis
    print(f"\n>>> FACTOR ANALYSIS")
    factor_analysis = perform_factor_analysis(data_collection, country)
    
    if factor_analysis:
        print("\nFactor Performance Summary:")
        factor_summary = create_performance_summary(factor_analysis)
        print(factor_summary.round(2))
        
        print("\nFactor Performance Chart:")
        visualize_normalized_performance(factor_analysis, f"{country} Factor Performance")
    
    return {
        'sectors': sector_analysis,
        'factors': factor_analysis
    }

def collect_currency_data():
    """
    Collect currency exchange rate data
    """
    currency_data = {}
    
    for pair_name, currency in CURRENCY_PAIRS.items():
        try:
            # Yahoo Finance currency format: EURUSD=X
            if pair_name == 'USDEUR':
                ticker = 'EURUSD=X'
                invert = True
            elif pair_name == 'USDGBP':
                ticker = 'GBPUSD=X'
                invert = True
            elif pair_name == 'USDJPY':
                ticker = 'USDJPY=X'
                invert = False
            elif pair_name == 'USDCNY':
                ticker = 'USDCNY=X'
                invert = False
            elif pair_name == 'USDSGD':
                ticker = 'USDSGD=X'
                invert = False
            elif pair_name == 'USDHKD':
                ticker = 'USDHKD=X'
                invert = False
            else:
                continue
                
            fx_data = yf.Ticker(ticker).history(period='2y')
            if not fx_data.empty:
                rates = fx_data['Close']
                if invert:
                    rates = 1 / rates
                currency_data[pair_name] = rates
                print(f"✓ Currency data collected: {pair_name}")
            else:
                print(f"✗ No data for: {pair_name}")
                
        except Exception as e:
            print(f"✗ Error collecting {pair_name}: {e}")
    
    return currency_data

def run_full_analysis():
    """
    Run comprehensive analysis workflow
    """
    print("Starting Full Global Indices Analysis")
    print("="*60)
    
    # Step 1: Validate tickers
    print("\n1. Validating tickers...")
    validation_results = validate_all_tickers()
    
    # Step 2: Create validated universe
    print("\n2. Creating validated universe...")
    validated_universe = create_validated_universe()
    
    # Step 3: Collect data
    print("\n3. Collecting market data...")
    data = collect_universe_data(validated_universe)
    
    # Step 4: Collect currency data
    print("\n4. Collecting currency data...")
    currency_data = collect_currency_data()
    
    # Step 5: Generate reports for key countries
    key_countries = ['US', 'Europe', 'Germany', 'Japan', 'China']
    country_reports = {}
    
    for country in key_countries:
        if country in data:
            country_reports[country] = generate_country_report(data, country)
    
    print(f"\n{'='*60}")
    print("ANALYSIS COMPLETE")
    print(f"{'='*60}")
    
    return {
        'validation_results': validation_results,
        'validated_universe': validated_universe,
        'market_data': data,
        'currency_data': currency_data,
        'country_reports': country_reports
    }
