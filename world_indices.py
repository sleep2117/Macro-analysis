"""Global indices/sectors/factors universe"""

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
            'Technology':    {'index': 'SX8P', 'etf': 'EXV3.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Healthcare':    {'index': 'SXDP', 'etf': 'EXV4.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Utilities':     {'index': 'SX6P', 'etf': 'EXH9.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Telecom':       {'index': 'SXKP', 'etf': 'EXV2.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Automobiles':   {'index': 'SXAP', 'etf': 'EXV5.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Basic_Resrcs':  {'index': 'SXPP', 'etf': 'EXV6.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Chemicals':     {'index': 'SX4P', 'etf': 'EXV8.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Banks':         {'index': 'SX7P', 'etf': 'EXV1.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Insurance':     {'index': 'SXIP', 'etf': 'EXH5.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Industrials':   {'index': 'SXNP', 'etf': 'EXV7.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Construction':  {'index': 'SXOP', 'etf': 'EXOD.PA', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {
            'Large_Cap':     {'index': '^STOXX', 'currency': 'EUR', 'valuation_data': False, 'alternatives': ['EXSA.DE','DX2X.DE']},
            'Small_Cap(EMU)':{'index': None, 'etf': 'SMEA.MI', 'currency':'EUR','valuation_data':True,'alternatives': []},
            'Value':         {'index': None, 'etf': 'IEVL.MI','currency':'EUR','valuation_data':True,'alternatives':['CEMS.DE']},
            'Momentum':      {'index': None, 'etf': 'CEMR.DE','currency':'EUR','valuation_data':True,'alternatives': []},
            'Quality':       {'index': None, 'etf': 'IEFQ.L', 'currency':'EUR','valuation_data':True,'alternatives':['CEMQ.DE']},
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
            'Small_Cap': {'index': '^SDAXI','currency':'EUR','valuation_data':False,'alternatives':['SDXP.DE']},
        }
    },

    'Japan': {
        'currency': 'JPY',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'EWJ',  'currency': 'USD', 'valuation_data': True, 'alternatives': ['DXJ','HEWJ','BBJP']},
            'Technology':   {'index': None, 'etf': 'FLJP', 'currency': 'USD', 'valuation_data': True, 'alternatives': ['EWJ']},
        },
        'factors': {
            'Large_Cap': {'index': None, 'etf': 'EWJ',  'currency': 'USD', 'valuation_data': True, 'alternatives': ['DXJ']},
            'Small_Cap': {'index': None, 'etf': 'SCJ',  'currency': 'USD', 'valuation_data': True, 'alternatives': ['DXJS']},
            'Value':     {'index': None, 'etf': 'EWJV', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        }
    },

    'China': {
        'currency': 'CNY',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'FXI',  'currency': 'USD', 'valuation_data': True, 'alternatives': ['MCHI','ASHR','GXC']},
            'Technology':   {'index': None, 'etf': 'CQQQ', 'currency': 'USD', 'valuation_data': True, 'alternatives': ['KWEB']},
            'Consumer':     {'index': None, 'etf': 'CHIQ', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {
            'Large_Cap': {'index': None, 'etf': 'FXI',  'currency': 'USD', 'valuation_data': True, 'alternatives': ['GXC']},
            'Small_Cap': {'index': None, 'etf': 'ECNS', 'currency': 'USD', 'valuation_data': True, 'alternatives': ['HAO']},
            'A_Shares':  {'index': None, 'etf': 'ASHR', 'currency': 'USD', 'valuation_data': True, 'alternatives': ['CNYA','KBA']},
        }
    },

    'Southeast_Asia': {
        'currency': 'SGD',
        'sectors': {
            'Real_Estate_AXJ': {'index': None, 'etf':'CFA.SI', 'currency':'SGD', 'valuation_data': True, 'alternatives':['COI.SI']},
            'Broad_Market':    {'index': None, 'etf':'EWT',    'currency':'USD', 'valuation_data': True, 'alternatives':['THD','EWS','EWM']},
            'ASEAN':           {'index': None, 'etf':'ASEA',   'currency':'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {
            'ASEAN_LargeMid': {'index': 'FTSE/ASEAN 40', 'etf':'M62.SI', 'currency':'USD', 'valuation_data':True, 'alternatives':['QS0.SI']},
            'Singapore':      {'index': None, 'etf':'EWS',    'currency':'USD', 'valuation_data': True, 'alternatives': []},
            'Indonesia':      {'index': None, 'etf':'EIDO',   'currency':'USD', 'valuation_data': True, 'alternatives': []},
            'Thailand':       {'index': None, 'etf':'THD',    'currency':'USD', 'valuation_data': True, 'alternatives': []},
        }
    },

    'India': {
        'currency': 'INR',
        'sectors': {
            'Broad_Market': {'index': '^NSEI', 'currency': 'INR', 'valuation_data': False, 'alternatives': ['INDA','EPI','INDY']},
            'Financials':   {'index': None,    'etf': 'INDF', 'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Consumer':     {'index': None,    'etf': 'INCO', 'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Technology':   {'index': None,    'etf': 'INFY', 'currency': 'USD', 'valuation_data': False, 'alternatives': []},
        },
        'factors': {
            'Large_Cap':    {'index': '^NSEI', 'currency': 'INR', 'valuation_data': False, 'alternatives': ['INDA','INDY']},
            'Small_Cap':    {'index': None,    'etf': 'SMIN', 'currency': 'USD', 'valuation_data': True,  'alternatives': ['SCIF']},
            'Value(Proxy)': {'index': None,    'etf': 'EPI',  'currency': 'USD', 'valuation_data': True,  'alternatives': []},
        }
    },

    'UK': {
        'currency': 'GBP',
        'sectors': {
            'Broad_Market': {'index': '^FTSE', 'currency': 'GBP', 'valuation_data': False, 'alternatives': ['ISF.L','VUKE.L']},
        },
        'factors': {
            'Large_Cap': {'index': '^FTSE', 'currency': 'GBP', 'valuation_data': False, 'alternatives': ['EWU']},
        }
    },

    'France': {
        'currency': 'EUR',
        'sectors': {
            'Broad_Market': {'index': '^FCHI', 'currency': 'EUR', 'valuation_data': False, 'alternatives': ['EWQ']},
        },
        'factors': {
            'Large_Cap': {'index': '^FCHI', 'currency': 'EUR', 'valuation_data': False, 'alternatives': ['EWQ']},
        }
    },

    'Italy': {
        'currency': 'EUR',
        'sectors': {
            'Broad_Market': {'index': '^FTSEMIB', 'currency': 'EUR', 'valuation_data': False, 'alternatives': ['EWI']},
        },
        'factors': {
            'Large_Cap': {'index': '^FTSEMIB', 'currency': 'EUR', 'valuation_data': False, 'alternatives': ['EWI']},
        }
    },

    'Spain': {
        'currency': 'EUR',
        'sectors': {
            'Broad_Market': {'index': '^IBEX', 'currency': 'EUR', 'valuation_data': False, 'alternatives': ['EWP']},
        },
        'factors': {
            'Large_Cap': {'index': '^IBEX', 'currency': 'EUR', 'valuation_data': False, 'alternatives': ['EWP']},
        }
    },

    'Taiwan': {
        'currency': 'TWD',
        'sectors': {
            'Broad_Market':     {'index': '^TWII', 'currency': 'TWD', 'valuation_data': False, 'alternatives': ['EWT']},
            'Technology(Proxy)':{'index': None,   'etf': 'SMH',  'currency': 'USD', 'valuation_data': True,  'alternatives': []},
        },
        'factors': {
            'Large_Cap': {'index': '^TWII', 'currency': 'TWD', 'valuation_data': False, 'alternatives': ['EWT']},
        }
    },

    'Hong_Kong': {
        'currency': 'HKD',
        'sectors': {
            'Broad_Market': {'index': '^HSI', 'currency': 'HKD', 'valuation_data': False, 'alternatives': ['EWH']},
        },
        'factors': {
            'Large_Cap': {'index': '^HSI', 'currency': 'HKD', 'valuation_data': False, 'alternatives': ['EWH']},
        }
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

