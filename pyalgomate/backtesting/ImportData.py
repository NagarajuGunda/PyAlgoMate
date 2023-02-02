import yfinance

data = yfinance.download("SPY", start="1999-05-01", end="2021-06-16")
data.to_csv("spy.csv")