from prefect.core.task import Task
import yfinance as yf
import time

# -----------------------------------------------------------------------------
class FetchYFTask(Task):
    def run(self, ticker: str):
        yfticker = yf.Ticker(ticker.upper())
        history = yfticker.history(period="1y")
        return history.to_records()

# -----------------------------------------------------------------------------
class WriteToDatabaseTask(Task):
    def run(self, data: list[tuple]):
        time.sleep(0.5)
        print("Written to DB")

