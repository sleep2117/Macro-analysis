import streamlit as st
import pandas as pd
import numpy as np
from datetime import date

from global_universe import world_returns as wr
from global_universe import world_indices as wi


st.set_page_config(page_title="World Indices Returns", layout="wide")

st.title("World Stock Market Returns and Sharpe Ratios")
st.caption("Data via Yahoo Finance; returns use local index where possible and USD-converted series via FX.\n")

with st.sidebar:
    st.header("Options")
    specs = wr.build_default_index_specs()
    names = [s.name for s in specs]
    chosen = st.multiselect("Benchmarks", names, default=names)

    period = st.selectbox("Period", ["YTD", "1Y", "3Y", "5Y", "10Y", "Custom"], index=0)
    start_d = None
    end_d = None
    if period == "Custom":
        col1, col2 = st.columns(2)
        with col1:
            start_d = st.date_input("Start", value=date(date.today().year, 1, 1))
        with col2:
            end_d = st.date_input("End", value=date.today())
    use_usd_proxy = st.checkbox("Prefer USD ETF when available", value=False)
    calc_sharpe = st.checkbox("Compute Sharpe", value=True)
    do_update = st.button("Refresh latest prices (quick)")

if do_update:
    # Update only for selected symbols
    sel_specs = [s for s in specs if s.name in chosen]
    syms = {s.local_symbol for s in sel_specs}
    for s in sel_specs:
        if s.usd_proxy:
            syms.add(s.usd_proxy)
    wi.update_all_daily_data(wi.investment_universe, symbols=sorted(syms), lookback_days=7)
    st.success("Price files refreshed (recent window).")

sel_specs = [s for s in wr.build_default_index_specs() if s.name in chosen]
period_arg = None if period.lower() == "custom" else period.lower()

df = wr.compute_returns_table(
    specs=sel_specs,
    period=period_arg,
    start=start_d,
    end=end_d,
    calc_sharpe=calc_sharpe,
    use_usd_proxy_if_available=use_usd_proxy,
)

# Top section: local currency and USD terms
meta = f"Period: {df.attrs.get('start')} to {df.attrs.get('end')}"
st.subheader(meta)

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

show_cols = [
    "name",
    "local_return",
    "diff_vs_sp500_local",
    "usd_return",
    "diff_vs_sp500_usd",
]
if calc_sharpe:
    show_cols += ["sharpe_local", "sharpe_usd"]

show = df[show_cols].copy()
show.columns = [
    "Index",
    "Local Return %",
    "Diff vs S&P 500 (Local) %",
    "USD Return %",
    "Diff vs S&P 500 (USD) %",
    *( ["Sharpe (Local)", "Sharpe (USD)"] if calc_sharpe else [] ),
]

# Convert decimals to percent
percent_cols = ["Local Return %", "Diff vs S&P 500 (Local) %", "USD Return %", "Diff vs S&P 500 (USD) %"]
for c in percent_cols:
    show[c] = show[c] * 100.0

st.markdown("### Returns")
st.dataframe(style_table(show, percent_cols), use_container_width=True)

csv = df.to_csv(index=False).encode("utf-8")
st.download_button("Download CSV", data=csv, file_name="world_returns.csv", mime="text/csv")

