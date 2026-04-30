import yfinance as yf
import os

# Lista dei ticker (usiamo solo i simboli di Yahoo Finance)
tickers = [
    "CSSPX.MI",
    "XDAX.MI",
    "CSMIB.MI",
    "XESP.DE",
    "SXRW.DE",
    "XMEU.MI",
    "SGAJ.DE",
    "SW2CHB.MI"
]

if not os.path.exists('data'):
    os.makedirs('data')

for ticker in tickers:
    # Prende la parte prima del punto (es. CSSPX.MI -> CSSPX)
    clean_name = ticker.split('.')[0]
    print(f"Scaricamento Close per {clean_name}...")
    
    # auto_adjust=True per il prezzo rettificato
    data = yf.download(ticker, period="60d", interval="1d", auto_adjust=True)
    
    if not data.empty:
        # Selezioniamo solo la colonna 'Close'
        close_data = data[['Close']]
        
        # Salviamo come CSSPX.csv
        filename = f"data/{clean_name}.csv"
        close_data.to_csv(filename)
        print(f"Salvato: {filename}")
