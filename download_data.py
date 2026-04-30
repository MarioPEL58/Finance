import yfinance as yf
import pandas as pd
import os

# Lista dei ticker che vuoi monitorare
tickers = ["AAPL", "TSLA", "BTC-USD"]

# Crea la cartella 'data' se non esiste
if not os.path.exists('data'):
    os.makedirs('data')

for ticker in tickers:
    print(f"Scaricamento dati per {ticker}...")
    # Scarica gli ultimi 5 giorni di dati (ideale per aggiornamenti quotidiani)
    data = yf.download(ticker, period="5d", interval="1d")
    
    if not data.empty:
        filename = f"data/{ticker}_history.csv"
        # Se il file esiste già, lo aggiorna, altrimenti lo crea
        data.to_csv(filename)
        print(f"Salvato: {filename}")
