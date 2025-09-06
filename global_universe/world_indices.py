"""
Global indices/sectors/factors universe - data collection utilities only
"""

# Note: file moved under global_universe/ for project organization

import yfinance as yf
import requests as _requests
from requests.adapters import HTTPAdapter as _HTTPAdapter
try:
    from urllib3.util.retry import Retry as _Retry
except Exception:
    _Retry = None
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Shared HTTP session for yfinance with retry/backoff and browser-like headers
def _build_http_session():
    s = _requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    })
    if _Retry is not None:
        retry = _Retry(
            total=5,
            connect=5,
            read=5,
            status=5,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["HEAD", "GET"]),
        )
        adapter = _HTTPAdapter(max_retries=retry)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
    return s

_YF_SESSION = _build_http_session()

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

# (Validation and analysis helpers removed: this module focuses on data collection only.)

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
    # Mark execution time for visibility in CI even when no rows are added
    try:
        ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    except Exception:
        ts = datetime.utcnow().isoformat() + "Z"
    df["run_at"] = ts
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

# (list_primary_symbols_for_valuation removed; use list_primary_symbols instead.)

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

def _batch_fetch_quote(symbols: list[str]) -> dict:
    """
    Fetch quote fields in a single call for multiple symbols using Yahoo v7 API via curl.
    Returns mapping: symbol -> dict(fields) for available results.
    Only a light set of headers to mimic a browser.
    """
    if not symbols:
        return {}
    import urllib.parse as _up
    # URL-encode symbols for query param (handles '^')
    joined = ",".join(symbols)
    # Request specific fields to ensure availability
    fields = [
        "trailingPE",
        "priceToBook",
        "trailingAnnualDividendYield",
        "dividendYield",
        "trailingAnnualDividendRate",
        "bookValue",
        "epsTrailingTwelveMonths",
        "regularMarketPrice",
        "currency",
        "quoteType",
    ]
    url = (
        f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={_up.quote(joined)}"
        f"&fields={_up.quote(','.join(fields))}"
    )
    headers = [
        "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "-H", "Accept: application/json, text/plain, */*",
        "-H", "Accept-Language: en-US,en;q=0.9",
        "-H", "Referer: https://finance.yahoo.com/",
    ]
    try:
        out = _sp.check_output(["curl", "-s", *headers, url], timeout=25)
        data = _json.loads(out.decode("utf-8", errors="ignore"))
        results = (((data or {}).get("quoteResponse") or {}).get("result")) or []
        out_map: dict[str, dict] = {}
        for item in results:
            sym = item.get("symbol")
            if not sym:
                continue
            # Base: direct fields
            row = {k: item.get(k) for k in _VALUATION_FIELDS}
            # Derive dividend yield if missing
            if row.get("trailingAnnualDividendYield") in (None, "None"):
                dy = item.get("dividendYield")
                if dy in (None, "None"):
                    rate = item.get("trailingAnnualDividendRate")
                    price = item.get("regularMarketPrice") or 0
                    if rate not in (None, "None") and price:
                        try:
                            dy = float(rate) / float(price)
                        except Exception:
                            dy = None
                row["trailingAnnualDividendYield"] = dy
            # Derive P/B if missing
            if row.get("priceToBook") in (None, "None"):
                bv = item.get("bookValue")
                price = item.get("regularMarketPrice")
                if bv not in (None, "None", 0) and price not in (None, "None"):
                    try:
                        row["priceToBook"] = float(price) / float(bv)
                    except Exception:
                        pass
            # Derive P/E if missing
            if row.get("trailingPE") in (None, "None"):
                eps = item.get("epsTrailingTwelveMonths")
                price = item.get("regularMarketPrice")
                if eps not in (None, "None", 0) and price not in (None, "None"):
                    try:
                        pe = float(price) / float(eps)
                        # Avoid absurd values due to tiny EPS
                        if np.isfinite(pe) and pe > 0:
                            row["trailingPE"] = pe
                    except Exception:
                        pass
            row.update({
                "symbol": sym,
                "currency": item.get("currency"),
                "quoteType": item.get("quoteType"),
            })
            out_map[sym] = row
        return out_map
    except Exception:
        return {}

def _append_valuation_row(symbol: str, row: dict) -> tuple[_Path, bool]:
    """Append a single day's row to the symbol's valuation CSV if not present."""
    path = _valuation_csv_path(symbol)
    from datetime import datetime as _dt
    today = _dt.utcnow().date().isoformat()
    df_new = pd.DataFrame([{**{"Date": today}, **row}])
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

def update_all_valuations(
    universe: dict,
    pause: float = 0.2,
    symbols: list[str] | None = None,
    max_symbols: int | None = None,
    mode: str = "batch_quote",
    chunk: int = 20,
    info_fallback: bool = True,
    max_info_calls: int | None = None,
) -> pd.DataFrame:
    """
    Build/update daily valuation snapshots for each asset (one symbol per asset).
    Preference: index; if index has no valuation fields, fallback to ETF.
    No use of alternatives and no historical backfill (snapshot only).
    """
    rows = []
    # Build the traversal list with optional filtering
    tasks = []
    for country, cdata in universe.items():
        for section in ("sectors", "factors"):
            for name, asset in cdata.get(section, {}).items():
                chosen = asset.get("index") or asset.get("etf")
                if symbols and chosen not in symbols:
                    continue
                tasks.append((country, section, name, asset, chosen))

    if max_symbols is not None:
        tasks = tasks[:max_symbols]

    if mode == "batch_quote":
        # 1) Primary fetch in chunks
        prim_syms = [t[4] for t in tasks if t[4]]
        prim_map: dict[str, dict] = {}
        for i in range(0, len(prim_syms), max(1, chunk)):
            batch = prim_syms[i:i+chunk]
            prim_map.update(_batch_fetch_quote(batch))
            time.sleep(pause)

        # 2) Build full fallback candidate lists per task (ETF + alternatives + index when applicable)
        fb_lists: list[list[str]] = []
        fb_set: list[str] = []
        for (_, _, _, asset, chosen) in tasks:
            cands: list[str] = []
            idx_sym = asset.get("index")
            etf_sym = asset.get("etf")
            alts = [a for a in (asset.get("alternatives") or []) if a]
            if chosen == idx_sym:
                if etf_sym:
                    cands.append(etf_sym)
                cands.extend(alts)
            else:
                # chosen is ETF or alternative; try other ETFs/alternatives first, then index
                # Keep order but skip the chosen symbol itself
                for a in ([etf_sym] if etf_sym else []) + alts:
                    if a and a != chosen and a not in cands:
                        cands.append(a)
                if idx_sym:
                    cands.append(idx_sym)
            fb_lists.append(cands)
            for s in cands:
                if s not in fb_set:
                    fb_set.append(s)

        fb_map: dict[str, dict] = {}
        if fb_set:
            for i in range(0, len(fb_set), max(1, chunk)):
                batch = fb_set[i:i+chunk]
                fb_map.update(_batch_fetch_quote(batch))
                time.sleep(pause)

        # 3) Write CSVs and build summary rows (with optional info fallback)
        info_calls = 0
        for ti, (country, section, name, asset, chosen) in enumerate(tasks):
            used_fallback = False
            path = _valuation_csv_path(chosen or "")
            updated = False
            used_symbol = chosen
            snap = prim_map.get(chosen)
            if snap is None or all(snap.get(k) in (None, "None") for k in _VALUATION_FIELDS):
                # try broader fallback list (ETF/alternatives/index depending on chosen)
                for sym in fb_lists[ti]:
                    alt = fb_map.get(sym)
                    if alt and not all(alt.get(k) in (None, "None") for k in _VALUATION_FIELDS):
                        path, updated = _append_valuation_row(sym, alt)
                        used_fallback = True
                        used_symbol = sym
                        break
            if not used_fallback and snap and not all(snap.get(k) in (None, "None") for k in _VALUATION_FIELDS):
                path, updated = _append_valuation_row(chosen, snap)
                used_symbol = chosen

            # Per-symbol info fallback (quoteSummary) if still not updated
            if not updated and info_fallback and (max_info_calls is None or info_calls < max_info_calls):
                # Try chosen, then ETF, then alternatives in order
                cand_syms = [s for s in [chosen, asset.get("etf"), *(asset.get("alternatives") or []), asset.get("index")] if s]
                for sym in cand_syms:
                    row = fetch_valuation_snapshot(sym)
                    info_calls += 1
                    if row and not all(row.get(k) in (None, "None") for k in _VALUATION_FIELDS):
                        path, updated = _append_valuation_row(sym, row)
                        used_fallback = used_fallback or (sym != chosen)
                        used_symbol = sym
                        time.sleep(pause)
                        break
            rows.append({
                "country": country,
                "category": section,
                "name": name,
                "primary": chosen,
                "fallback_to_etf": used_fallback,
                "used_symbol": used_symbol,
                "file": str(path) if isinstance(path, _Path) else str(path),
                "updated": updated,
            })
    else:
        # Legacy per-symbol yfinance .info mode
        for (country, section, name, asset, chosen) in tasks:
            used_fallback = False
            path, updated = _valuation_csv_path(""), False  # init
            if chosen:
                path, updated = update_valuation_csv(chosen)
                if not updated and asset.get("index") and asset.get("index") == chosen:
                    # fallback to ETF/alternative if index had no valuation data today
                    fb = asset.get("etf")
                    if not fb:
                        alts = asset.get("alternatives") or []
                        fb = alts[0] if alts else None
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
    # Mark execution time for visibility in CI
    try:
        ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    except Exception:
        ts = datetime.utcnow().isoformat() + "Z"
    df["run_at"] = ts
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
        # In CI we slow down more to mitigate Yahoo 429
        _is_ci = _os.environ.get("GITHUB_ACTIONS") == "true"
        price_pause = float(_os.environ.get("PRICE_PAUSE", "0.6" if _is_ci else "0.3"))
        summary = update_all_daily_data(investment_universe, pause=price_pause, symbols=syms)
        print("Price update complete. Summary saved to data/update_summary.csv")

        # Valuation snapshots (snapshot only, daily append)
        skip_vals = _os.environ.get("SKIP_VALUATIONS", "false").lower() in {"1","true","yes","on"}
        if not skip_vals:
            print("Updating valuation snapshots (primary symbols, with ETF fallback)...")
            val_pause = float(_os.environ.get("VALUATION_PAUSE", "1.0" if _is_ci else "0.2"))
            # Optional limiting in CI to avoid 429
            _max_val = _os.environ.get("MAX_VAL_SYMBOLS")
            _val_syms_env = _os.environ.get("VAL_SYMBOLS")  # comma-separated list, optional
            _val_symbols = None
            if _val_syms_env:
                _val_symbols = [s.strip() for s in _val_syms_env.split(",") if s.strip()]
            _mode = _os.environ.get("VALUATION_FETCH_MODE", "batch_quote")
            _chunk = int(_os.environ.get("VALUATION_CHUNK", "20"))
            _info_fallback = (_os.environ.get("VALUATION_INFO_FALLBACK", "1").lower() in {"1","true","yes","on"})
            _max_info_calls = _os.environ.get("MAX_INFO_CALLS")
            vsummary = update_all_valuations(
                investment_universe,
                pause=val_pause,
                symbols=_val_symbols,
                max_symbols=int(_max_val) if _max_val else None,
                mode=_mode,
                chunk=_chunk,
                info_fallback=_info_fallback,
                max_info_calls=int(_max_info_calls) if _max_info_calls else None,
            )
            print("Valuation update complete. Saved to data/valuations_update_summary.csv")
            print(vsummary.head())
        else:
            print("Skipping valuation snapshots (set FORCE_VALUATIONS=1 to override).")
    except Exception as e:
        print(f"Error during update: {e}")
