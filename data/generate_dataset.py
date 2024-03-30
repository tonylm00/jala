import yfinance as yf
import pandas as pd
import os

tickers = pd.read_csv("../data/tickers_sp500.csv")['Symbol']


def get_historical_data():
    print(tickers)
    data_list = []
    for ticker in tickers:
        print(ticker)
        stock = yf.Ticker(ticker)
        historical = stock.history(period="max")
        if not historical.empty:
            historical['Ticker'] = ticker
            data_list.append(historical)

    data = pd.concat(data_list, axis=0, ignore_index=True)
    return data


def save_to_csv(data, filename):
    if not os.path.exists(filename):
        data[:0].to_csv(filename, mode='w', header=True, index=False)
    data.to_csv(filename, mode='a', header=False, index=False)


if __name__ == "__main__":
    historical_data = get_historical_data()
    save_to_csv(historical_data, "historical_data.csv")
