# World Indices Returns (analysis + app)

This repo contains utilities to fetch global market data and compute flexible-period returns across major country indices, plus a small Streamlit app for quick viewing.

## Quickstart

- Create a venv and install deps:
  - `pip install -r requirements.txt`
- Update prices for the benchmarks (optional, the app will read/fetch on demand):
  - `python -m global_universe.world_indices` (or run the app and press "Refresh latest prices")
- Run the Streamlit app:
  - `streamlit run apps/world_returns_app.py`

## CLI usage

You can also run the analysis from the command line:

```
python -m global_universe.world_returns --period ytd
python -m global_universe.world_returns --period 1y
python -m global_universe.world_returns --period custom --start 2020-01-01 --end 2025-09-01
```

The CSV report is saved to `global_universe/data/reports/`.

## How returns are computed

- Local series: preference for native country index in local currency (e.g., `^N225` for Japan). If only a USD ETF exists, that series is used as-is.
- USD series: FX converts the local series to USD using Yahoo FX pairs (e.g., `USDJPY=X`, `EURUSD=X`). When the option is enabled in the app, a USD ETF proxy (e.g., `EWJ`) is preferred if it provides a longer series.
- Sharpe Ratio: based on daily returns, annualized with 252 trading days, risk-free set to 0 by default.

## Deploying privately

GitHub Pages itself is public for personal accounts. If you need private access:

- Streamlit Cloud (Team/Enterprise): connect this repo and keep the app private to your account/team.
- Or host behind access control (e.g., Cloudflare Zero Trust or Netlify password) by building a static report and serving it from a private site.
- For organizations with GitHub Enterprise Cloud, you can enable Pages access control and restrict visibility to org members.

This repo already includes the Streamlit app (`apps/world_returns_app.py`) which is the simplest way to run privately.

