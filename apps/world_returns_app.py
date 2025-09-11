import streamlit as st
import pandas as pd
import numpy as np
from datetime import date
import sys
from pathlib import Path

# Ensure repo root is on PYTHONPATH when running with `streamlit run`
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from global_universe import world_returns as wr
from global_universe import world_indices as wi


st.set_page_config(page_title="세계 시장 수익률", layout="wide")

st.title("세계 주식시장 수익률 및 위험지표")
st.caption("야후 파이낸스 데이터 · 현지/달러 기준 수익률, 샤프/소르티노 지수")

with st.sidebar:
    st.header("전역 옵션")
    period = st.selectbox("기간", ["1M", "YTD", "1Y", "3Y", "5Y", "10Y", "Custom"], index=1)
    start_d = None
    end_d = None
    if period == "Custom":
        col1, col2 = st.columns(2)
        with col1:
            start_d = st.date_input("시작일", value=date(date.today().year, 1, 1))
        with col2:
            end_d = st.date_input("종료일", value=date.today())
    use_usd_proxy = st.checkbox("USD 수익률에 USD ETF 우선 사용", value=False)
    calc_sharpe = st.checkbox("샤프 지수 계산", value=True)
    calc_sortino = st.checkbox("소르티노 지수 계산", value=True)
    do_update = st.button("가격 최신화(빠른)")

if do_update:
    # Update a safe, curated list (major benchmarks) to avoid heavy global runs
    syms = sorted({sym for (_, sym, _) in _major_benchmark_list()})
    wi.update_all_daily_data(wi.investment_universe, symbols=syms, lookback_days=7)
    # Clear caches so the next compute reads fresh data
    wr.clear_caches()
    st.cache_data.clear()
    st.success("주요 벤치마크 가격을 최신화했습니다(최근 구간).")

period_arg = None if period.lower() == "custom" else period.lower()

# ---------------- Common helpers ----------------

def style_table(sub: pd.DataFrame, value_cols: list[str], top_n: int = 3):
    # Highlight top n in green
    def highlight_top(s: pd.Series):
        vals = s.astype(float)
        order = vals.rank(ascending=False, method="min")
        return ["background-color: #a6e3a1" if (np.isfinite(v) and r <= top_n) else "" for v, r in zip(vals, order)]

    fmt = {c: "{:+.2f}" for c in value_cols}
    sty = (sub.style
           .format(fmt)
           .apply(highlight_top, subset=value_cols))
    return sty

def _auto_height(df: pd.DataFrame, row_px: int = 36, header_px: int = 38, max_px: int = 2000) -> int:
    try:
        rows = int(len(df))
    except Exception:
        rows = 12
    return min(max_px, header_px + row_px * max(1, rows))

def _format_df_for_display(df: pd.DataFrame, calc_sharpe: bool, calc_sortino: bool):
    cols = [
        "name","local_return","diff_vs_sp500_local","usd_return","diff_vs_sp500_usd",
    ]
    if calc_sharpe:
        cols += ["sharpe_local","sharpe_usd"]
    if calc_sortino:
        cols += ["sortino_local","sortino_usd"]
    sub = df[cols].copy()
    sub.columns = [
        "자산",
        "현지 수익률 %",
        "S&P 500 대비(현지) %",
        "USD 수익률 %",
        "S&P 500 대비(USD) %",
        *( ["샤프(현지)", "샤프(USD)"] if calc_sharpe else [] ),
        *( ["소르티노(현지)", "소르티노(USD)"] if calc_sortino else [] ),
    ]
    for c in ["현지 수익률 %","S&P 500 대비(현지) %","USD 수익률 %","S&P 500 대비(USD) %"]:
        sub[c] = sub[c] * 100
    return sub

@st.cache_data(show_spinner=True, ttl=900)
def _compute_from_specs(rows: tuple[tuple[str,str,str|None], ...], period_arg, start_d, end_d, calc_sharpe, calc_sortino, use_usd_proxy):
    specs = [wr.IndexSpec(name=n, local_symbol=sym, local_currency=cur) for (n,sym,cur) in rows]
    return wr.compute_returns_table(
        specs=specs,
        period=period_arg,
        start=start_d,
        end=end_d,
        calc_sharpe=calc_sharpe,
        calc_sortino=calc_sortino,
        use_usd_proxy_if_available=use_usd_proxy,
        ensure_data=False,
    )

def _build_specs_for(country: str, section: str):
    data = wi.investment_universe.get(country, {})
    sec = data.get(section, {})
    out = []
    for name, asset in sec.items():
        # Prefer index, but if index is stale or sparse, fallback to ETF
        idx_sym = asset.get("index")
        etf_sym = asset.get("etf")
        sym = idx_sym or etf_sym
        try:
            if idx_sym:
                s = wr._read_price_series(idx_sym)
                # Fallback if fewer than 60 points or last point older than ~90 days
                import pandas as _pd
                if len(s) < 60 or (s.index.max() < (_pd.Timestamp.today().normalize() - _pd.Timedelta(days=90))):
                    if etf_sym:
                        s2 = wr._read_price_series(etf_sym)
                        if not s2.empty:
                            sym = etf_sym
        except Exception:
            pass
        if not sym:
            continue
        cur = asset.get("currency") or data.get("currency") or "USD"
        kind = 'sectors' if section == 'sectors' else ('factors' if section == 'factors' else 'themes')
        label_country = wi.get_korean_label('countries', country)
        label_name = wi.get_korean_label(kind, name)
        out.append((f"{label_country} — {label_name}", sym, cur))
    return out

def _major_benchmark_list():
    ko = wi.get_korean_label
    def clab(c):
        return ko('countries', c)
    return [
        (f"{clab('US')} — S&P 500", "^GSPC", "USD"),
        (f"{clab('Canada')} — S&P/TSX", "^GSPTSE", "CAD"),
        (f"{clab('UK')} — FTSE 100", "^FTSE", "GBP"),
        (f"{clab('Germany')} — DAX", "^GDAXI", "EUR"),
        (f"{clab('France')} — CAC 40", "^FCHI", "EUR"),
        (f"{clab('Europe')} — EuroStoxx 50", "^STOXX50E", "EUR"),
        (f"{clab('Japan')} — {ko('sectors','Nikkei_225')}", "^N225", "JPY"),
        (f"{clab('China')} — CSI 300", "000300.SS", "CNY"),
        (f"{clab('Hong_Kong')} — Hang Seng", "^HSI", "HKD"),
        (f"{clab('Taiwan')} — TAIEX", "^TWII", "TWD"),
        (f"{clab('Korea')} — KOSPI", "^KS11", "KRW"),
        (f"{clab('India')} — NIFTY 50", "^NSEI", "INR"),
        (f"{clab('Australia')} — ASX 200", "^AXJO", "AUD"),
        (f"{clab('Singapore')} — STI", "^STI", "SGD"),
        (f"{clab('Switzerland')} — SMI", "^SSMI", "CHF"),
        (f"{clab('Spain')} — IBEX 35", "^IBEX", "EUR"),
        (f"{clab('Italy')} — FTSE MIB", "FTSEMIB.MI", "EUR"),
        (f"{clab('Netherlands')} — AEX", "^AEX", "EUR"),
        (f"{clab('Sweden')} — 스웨덴(EWD)", "EWD", "USD"),
        (f"{clab('Brazil')} — Ibovespa", "^BVSP", "BRL"),
        (f"{clab('Mexico')} — IPC", "^MXX", "MXN"),
    ]

# ---------------- Preset tabs ----------------
tab_bm, tab_sector, tab_factor, tab_theme, tab_custom = st.tabs([
    "Benchmarks", "Sectors", "Factors", "Themes", "Custom"
])

# Benchmarks (major countries)
with tab_bm:
    st.subheader("주요국 벤치마크")
    df_bm = _compute_from_specs(tuple(_major_benchmark_list()), period_arg, start_d, end_d, calc_sharpe, calc_sortino, use_usd_proxy)
    disp = _format_df_for_display(df_bm, calc_sharpe, calc_sortino)
    st.dataframe(style_table(disp,["현지 수익률 %","S&P 500 대비(현지) %","USD 수익률 %","S&P 500 대비(USD) %"]),
                 use_container_width=True, height=_auto_height(disp))

# Sectors
with tab_sector:
    st.subheader("국가별 섹터")
    countries = list(wi.investment_universe.keys())
    country = st.selectbox("국가", countries, index=countries.index("US") if "US" in countries else 0)
    rows = tuple(_build_specs_for(country, "sectors"))
    if not rows:
        st.info("No sector set defined for this country.")
    else:
        df_sec = _compute_from_specs(rows, period_arg, start_d, end_d, calc_sharpe, calc_sortino, use_usd_proxy)
        disp = _format_df_for_display(df_sec, calc_sharpe, calc_sortino)
        st.dataframe(style_table(disp,["현지 수익률 %","S&P 500 대비(현지) %","USD 수익률 %","S&P 500 대비(USD) %"]),
                     use_container_width=True, height=_auto_height(disp))

# Factors
with tab_factor:
    st.subheader("국가별 팩터")
    countries = list(wi.investment_universe.keys())
    country_f = st.selectbox("국가", countries, index=countries.index("US") if "US" in countries else 0, key="factor_country")
    rows = tuple(_build_specs_for(country_f, "factors"))
    if not rows:
        st.info("No factor set defined for this country.")
    else:
        df_fac = _compute_from_specs(rows, period_arg, start_d, end_d, calc_sharpe, calc_sortino, use_usd_proxy)
        disp = _format_df_for_display(df_fac, calc_sharpe, calc_sortino)
        st.dataframe(style_table(disp,["현지 수익률 %","S&P 500 대비(현지) %","USD 수익률 %","S&P 500 대비(USD) %"]),
                     use_container_width=True, height=_auto_height(disp))

# Themes
with tab_theme:
    st.subheader("국가/지역별 테마")
    countries = list(wi.investment_universe.keys())
    country_t = st.selectbox("국가/지역", countries, index=countries.index("US") if "US" in countries else 0, key="theme_country")
    rows = tuple(_build_specs_for(country_t, "themes"))
    if not rows:
        st.info("No themes set defined for this country.")
    else:
        df_th = _compute_from_specs(rows, period_arg, start_d, end_d, calc_sharpe, calc_sortino, use_usd_proxy)
        disp = _format_df_for_display(df_th, calc_sharpe, calc_sortino)
        st.dataframe(style_table(disp,["현지 수익률 %","S&P 500 대비(현지) %","USD 수익률 %","S&P 500 대비(USD) %"]),
                     use_container_width=True, height=_auto_height(disp))

# Custom: Universe picker + free symbols
with tab_custom:
    st.subheader("사용자 정의")
    tab1, tab2 = st.tabs(["카탈로그에서 선택", "티커 직접 입력"])

with tab1:
    st.caption("카탈로그에서 자산을 선택해 비교합니다(지수 우선·부족 시 ETF 대체).")
    @st.cache_data(ttl=3600)
    def _load_catalog():
        df = wi.build_symbols_catalog(wi.investment_universe, primary_only=True)
        # Korean labels
        def k_country(x):
            return wi.get_korean_label('countries', x)
        def k_name(cat, name):
            kind = 'sectors' if cat == 'sectors' else ('factors' if cat == 'factors' else 'themes')
            return wi.get_korean_label(kind, name)
        df["label"] = df.apply(lambda r: f"{k_country(r['country'])} | {k_name(r['category'], r['name'])} ({r['symbol']})", axis=1)
        return df
    cat = _load_catalog()
    sel_labels = st.multiselect("자산 선택", cat["label"].tolist(), default=[])
    if sel_labels:
        sub = cat[cat["label"].isin(sel_labels)]
        specs_custom = [
            wr.IndexSpec(
                name=row["label"],
                local_symbol=row["symbol"],
                local_currency=row.get("resolved_currency") or row.get("currency") or "USD",
            )
            for _, row in sub.iterrows()
        ]
        df2 = wr.compute_returns_table(
            specs=specs_custom,
            period=period_arg,
            start=start_d,
            end=end_d,
            calc_sharpe=calc_sharpe,
            calc_sortino=calc_sortino,
            use_usd_proxy_if_available=False,
            ensure_data=False,
        )
        show2 = df2[[
            "name","local_return","diff_vs_sp500_local","usd_return","diff_vs_sp500_usd",
            *( ["sharpe_local","sharpe_usd"] if calc_sharpe else []),
            *( ["sortino_local","sortino_usd"] if calc_sortino else []),
        ]].copy()
        show2.columns = [
            "자산",
            "현지 수익률 %",
            "S&P 500 대비(현지) %",
            "USD 수익률 %",
            "S&P 500 대비(USD) %",
            *( ["샤프(현지)", "샤프(USD)"] if calc_sharpe else [] ),
            *( ["소르티노(현지)", "소르티노(USD)"] if calc_sortino else [] ),
        ]
        for c in ["현지 수익률 %","S&P 500 대비(현지) %","USD 수익률 %","S&P 500 대비(USD) %"]:
            show2[c] = show2[c] * 100
        st.dataframe(style_table(show2, ["현지 수익률 %","S&P 500 대비(현지) %","USD 수익률 %","S&P 500 대비(USD) %"]), use_container_width=True, height=_auto_height(show2))

with tab2:
    st.caption("Type tickers separated by commas or newlines. Optionally add currency, e.g., ^N225 JPY or ^STOXX50E EUR.")
    user_text = st.text_area("Symbols", value="SPY, QQQ, EFA, EEM", height=80)
    # Parse user input
    tokens = [t.strip() for t in user_text.replace("\n", ",").split(",") if t.strip()]
    symbols: list[tuple[str,str|None]] = []
    for tok in tokens:
        parts = tok.split()
        if len(parts) == 2:
            symbols.append((parts[0], parts[1].upper()))
        else:
            symbols.append((parts[0], None))
    if symbols:
        specs_user = []
        for sym, cur in symbols:
            cur2 = cur or wi.resolve_currency(sym, None) or "USD"
            specs_user.append(wr.IndexSpec(name=sym, local_symbol=sym, local_currency=cur2))
        df3 = wr.compute_returns_table(
            specs=specs_user,
            period=period_arg,
            start=start_d,
            end=end_d,
            calc_sharpe=calc_sharpe,
            calc_sortino=calc_sortino,
            use_usd_proxy_if_available=False,
            ensure_data=False,
        )
        show3 = df3[[
            "name","local_return","diff_vs_sp500_local","usd_return","diff_vs_sp500_usd",
            *( ["sharpe_local","sharpe_usd"] if calc_sharpe else []),
            *( ["sortino_local","sortino_usd"] if calc_sortino else []),
        ]].copy()
        show3.columns = [
            "티커",
            "현지 수익률 %",
            "S&P 500 대비(현지) %",
            "USD 수익률 %",
            "S&P 500 대비(USD) %",
            *( ["샤프(현지)", "샤프(USD)"] if calc_sharpe else [] ),
            *( ["소르티노(현지)", "소르티노(USD)"] if calc_sortino else [] ),
        ]
        for c in ["현지 수익률 %","S&P 500 대비(현지) %","USD 수익률 %","S&P 500 대비(USD) %"]:
            show3[c] = show3[c] * 100
        st.dataframe(style_table(show3, ["현지 수익률 %","S&P 500 대비(현지) %","USD 수익률 %","S&P 500 대비(USD) %"]), use_container_width=True, height=_auto_height(show3))
