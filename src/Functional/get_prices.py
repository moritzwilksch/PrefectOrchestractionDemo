import yfinance as yf
from prefect import task, Flow


@task
def get_prices(ticker: str):
    yfticker = yf.Ticker(ticker.upper())
    history = yfticker.history(period="1y")
    print("Returning prices.")
    return history.to_records()


if __name__ == "__main__":
    print(get_prices("AAPL"))