# Note: file moved under global_universe/
# %%
!pip install pykrx
# %%
from pykrx import stock
from pykrx import bond

tickers = stock.get_market_ticker_list("20190225")
print(tickers)
# %%
tickers = stock.get_index_ticker_list(market='KOSDAQ')
tickers
# %%
for ticker in stock.get_index_ticker_list():
    print(ticker, stock.get_index_ticker_name(ticker))
# %%
