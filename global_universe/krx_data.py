"""
KRX index data utilities (prices + fundamentals) via pykrx

This module mirrors the storage format used in world_indices.py:
- Daily OHLCV CSVs under data/daily/<symbol>.csv with columns
  [Open, High, Low, Close, Adj Close?, Volume] (Adj Close omitted if N/A)
- Daily valuation snapshots (PE, PB, DividendYield) under data/valuations/<symbol>.csv

Symbols here use a filesystem-safe prefix: KRX_IDX_<ticker>
e.g., KOSPI (1001) -> data/daily/KRX_IDX_1001.csv
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

from pykrx import stock

# --------------------
# Paths and helpers
# --------------------

BASE_DIR = Path(__file__).resolve().parent
DAILY_DIR = BASE_DIR / "data" / "daily"
VAL_DIR = BASE_DIR / "data" / "valuations"
DAILY_DIR.mkdir(parents=True, exist_ok=True)
VAL_DIR.mkdir(parents=True, exist_ok=True)


def _symbol_for_krx_index(ticker: str) -> str:
    """Create a filesystem-safe symbol for a KRX index code."""
    return f"KRX_IDX_{str(ticker).strip()}"


def _daily_csv_path(ticker: str) -> Path:
    return DAILY_DIR / f"{_symbol_for_krx_index(ticker)}.csv"


def _valuation_csv_path(ticker: str) -> Path:
    return VAL_DIR / f"{_symbol_for_krx_index(ticker)}.csv"


def _to_yyyymmdd(dt: datetime | str | None) -> str | None:
    if dt is None:
        return None
    if isinstance(dt, str):
        # Accept 'YYYY-MM-DD' or 'YYYYMMDD'
        s = dt.replace("-", "")
        if len(s) == 8:
            return s
        raise ValueError(f"Invalid date string: {dt}")
    return dt.strftime("%Y%m%d")


def list_krx_index_tickers(market: str = "KOSPI", date: str | None = None) -> pd.DataFrame:
    """Return DataFrame of available index tickers and names for a market.

    - market: 'KOSPI' | 'KOSDAQ' | 'KRX'
    - date: optional 'YYYYMMDD' or 'YYYY-MM-DD' (defaults to today)
    """
    if date is None:
        date = datetime.today().strftime("%Y%m%d")
    else:
        date = _to_yyyymmdd(date)
    codes = stock.get_index_ticker_list(date=date, market=market)
    rows = [{"ticker": c, "name": stock.get_index_ticker_name(c)} for c in codes]
    return pd.DataFrame(rows)


# --------------------
# Price (OHLCV)
# --------------------

def fetch_index_ohlcv(ticker: str, start: str | datetime | None = None, end: str | datetime | None = None, freq: str = "d") -> pd.DataFrame:
    """Fetch KRX index OHLCV from pykrx and normalize columns to English.

    Returns a DataFrame indexed by Timestamp with columns [Open, High, Low, Close, Volume].
    """
    if end is None:
        end = datetime.today()
    if start is None:
        # pykrx supports long history; pick early default
        start = datetime(1990, 1, 1)
    s = _to_yyyymmdd(start)
    e = _to_yyyymmdd(end)
    df = stock.get_index_ohlcv_by_date(s, e, str(ticker), freq=freq)
    if df is None or df.empty:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"]).astype({})
    # Normalize headers
    rename_map = {
        "시가": "Open",
        "고가": "High",
        "저가": "Low",
        "종가": "Close",
        "거래량": "Volume",
    }
    out = df.rename(columns=rename_map).copy()
    # Ensure we keep only expected columns
    keep = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in out.columns]
    out = out[keep]
    # Index normalization
    out.index.name = "Date"
    out = out.sort_index()
    return out


def _load_existing_daily(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, parse_dates=["Date"], index_col="Date")
        return df.sort_index()
    except Exception:
        return None


def update_index_daily_csv(ticker: str) -> tuple[Path, int]:
    """Create or incrementally update daily OHLCV CSV for a KRX index.

    Returns (path, rows_added)
    """
    path = _daily_csv_path(ticker)
    existing = _load_existing_daily(path)
    if existing is None or existing.empty:
        df = fetch_index_ohlcv(ticker)
        if df.empty:
            # write placeholder with schema
            pd.DataFrame(columns=["Open","High","Low","Close","Volume"]).to_csv(path, index_label="Date")
            return path, 0
        tmp = path.with_suffix(".csv.tmp")
        df.to_csv(tmp, index_label="Date")
        os.replace(tmp, path)
        return path, len(df)
    # Incremental
    last = existing.index.max()
    start = last + timedelta(days=1)
    new = fetch_index_ohlcv(ticker, start=start)
    if new is None or new.empty:
        return path, 0
    combined = pd.concat([existing, new])
    combined = combined[~combined.index.duplicated(keep="last")].sort_index()
    tmp = path.with_suffix(".csv.tmp")
    combined.to_csv(tmp, index_label="Date")
    os.replace(tmp, path)
    return path, len(new)


# --------------------
# Fundamentals (PE/PB/Dividend Yield)
# --------------------

VAL_FIELDS = [
    "trailingPE",                # from PER
    "priceToBook",               # from PBR
    "trailingAnnualDividendYield"  # from 배당수익률 (as decimal)
]


def fetch_index_fundamentals(ticker: str, start: str | datetime, end: str | datetime) -> pd.DataFrame:
    """Fetch KRX index fundamentals between dates; map to world_indices valuation schema.

    Returns DataFrame with columns [Date, symbol, currency, quoteType, trailingPE, priceToBook, trailingAnnualDividendYield]
    """
    s = _to_yyyymmdd(start)
    e = _to_yyyymmdd(end)
    df = stock.get_index_fundamental_by_date(s, e, str(ticker))
    if df is None or df.empty:
        return pd.DataFrame(columns=["Date","symbol","currency","quoteType",*VAL_FIELDS])

    # Rename and convert
    rename_map = {
        "PER": "trailingPE",
        "PBR": "priceToBook",
        "배당수익률": "trailingAnnualDividendYield",
    }
    out = df.rename(columns=rename_map).copy()
    # Dividend yield from percentage to decimal if looks like percentage
    if "trailingAnnualDividendYield" in out.columns:
        out["trailingAnnualDividendYield"] = pd.to_numeric(out["trailingAnnualDividendYield"], errors="coerce")
        # If values mostly > 1.0, assume percent and convert
        ser = out["trailingAnnualDividendYield"]
        if ser.dropna().gt(1.0).mean() > 0.5:
            out["trailingAnnualDividendYield"] = ser / 100.0
    # Keep desired cols and order to match world_indices valuation schema
    keep_metrics = [c for c in ["trailingPE","priceToBook","trailingAnnualDividendYield"] if c in out.columns]
    out = out[keep_metrics]
    out.index.name = "Date"
    out = out.sort_index().reset_index()
    # Append metadata at end: symbol, currency, quoteType
    out["symbol"] = _symbol_for_krx_index(ticker)
    out["currency"] = "KRW"
    out["quoteType"] = "INDEX"
    # Final column order: Date, metrics..., symbol, currency, quoteType
    columns = ["Date", *keep_metrics, "symbol", "currency", "quoteType"]
    out = out[columns]
    return out


def update_index_valuation_csv(ticker: str, mode: str = "append_today") -> tuple[Path, int]:
    """Update valuation CSV for a KRX index.

    - mode='append_today': append only today's row if missing
    - mode='backfill': if file missing/empty, write full history; else append from last+1 day
    Returns (path, rows_added)
    """
    path = _valuation_csv_path(ticker)
    today_str = datetime.utcnow().date().isoformat()

    canonical_cols = ["Date", "trailingPE", "priceToBook", "trailingAnnualDividendYield", "symbol", "currency", "quoteType"]

    def _ensure_canonical(path: Path):
        try:
            df0 = pd.read_csv(path)
            # Add missing cols
            for col in canonical_cols:
                if col not in df0.columns:
                    df0[col] = pd.NA
            df0 = df0[canonical_cols]
            df0 = df0.sort_values("Date").drop_duplicates(subset=["Date"], keep="last")
            tmp2 = path.with_suffix(".csv.tmp")
            df0.to_csv(tmp2, index=False)
            os.replace(tmp2, path)
        except Exception:
            pass

    if mode == "append_today":
        df = fetch_index_fundamentals(ticker, today_str, today_str)
        if df is None or df.empty:
            # Ensure file exists
            if not path.exists():
                pd.DataFrame(columns=["Date","symbol","currency","quoteType",*VAL_FIELDS]).to_csv(path, index=False)
            _ensure_canonical(path)
            return path, 0
        if path.exists():
            try:
                cur = pd.read_csv(path)
                if (cur["Date"] == today_str).any():
                    _ensure_canonical(path)
                    return path, 0
                new_df = pd.concat([cur, df], ignore_index=True)
            except Exception:
                new_df = df
        else:
            new_df = df
        # Reorder to canonical schema and sort
        for col in canonical_cols:
            if col not in new_df.columns:
                new_df[col] = pd.NA
        new_df = new_df[canonical_cols]
        new_df = new_df.sort_values("Date").drop_duplicates(subset=["Date"], keep="last")
        tmp = path.with_suffix(".csv.tmp")
        new_df.to_csv(tmp, index=False)
        os.replace(tmp, path)
        return path, len(df)

    # backfill mode
    if not path.exists() or os.path.getsize(path) == 0:
        # Full history from 1990
        df = fetch_index_fundamentals(ticker, "19900101", datetime.today())
        if df is None or df.empty:
            pd.DataFrame(columns=["Date","symbol","currency","quoteType",*VAL_FIELDS]).to_csv(path, index=False)
            _ensure_canonical(path)
            return path, 0
        # Ensure canonical order
        for col in canonical_cols:
            if col not in df.columns:
                df[col] = pd.NA
        df = df[canonical_cols]
        tmp = path.with_suffix(".csv.tmp")
        df.to_csv(tmp, index=False)
        os.replace(tmp, path)
        return path, len(df)
    # Incremental backfill
    try:
        cur = pd.read_csv(path)
        cur["Date"] = pd.to_datetime(cur["Date"]).dt.date.astype(str)
        last = max(cur["Date"]) if not cur.empty else "1990-01-01"
    except Exception:
        cur = None
        last = "1990-01-01"
    start_dt = datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)
    df = fetch_index_fundamentals(ticker, start_dt, datetime.today())
    if df is None or df.empty:
        # Even if no new data, ensure header/order canonical
        _ensure_canonical(path)
        return path, 0
    new_df = pd.concat([cur, df], ignore_index=True) if cur is not None else df
    # Canonical order and clean
    for col in canonical_cols:
        if col not in new_df.columns:
            new_df[col] = pd.NA
    new_df = new_df[canonical_cols]
    new_df = new_df.sort_values("Date").drop_duplicates(subset=["Date"], keep="last")
    tmp = path.with_suffix(".csv.tmp")
    new_df.to_csv(tmp, index=False)
    os.replace(tmp, path)
    return path, len(df)


# --------------------
# Quick manual test
# --------------------

if __name__ == "__main__":
    # Example: KOSPI index (1001)
    code = os.environ.get("KRX_INDEX", "1001")
    print(f"Updating daily OHLCV for KRX index {code}...")
    p, added = update_index_daily_csv(code)
    print(f"Saved: {p} (+{added} rows)")

    mode = os.environ.get("KRX_VAL_MODE", "append_today")  # or 'backfill'
    print(f"Updating valuation snapshots for {code} (mode={mode})...")
    vp, vadded = update_index_valuation_csv(code, mode=mode)
    print(f"Saved: {vp} (+{vadded} rows)")
