import yfinance as yf
import pandas as pd
import os

# Lista dei ticker (usiamo solo i simboli di Yahoo Finance)
tickers = [
    "CSSPX.MI", "XDAX.MI", "CSMIB.MI", "XESP.DE", 
    "SXRW.DE", "XMEU.MI", "SGAJ.DE", "SW2CHB.MI"
]

# Crea la struttura data/ETF se non esiste
output_dir = 'data/ETF'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

for ticker in tickers:
    # Prende la parte prima del punto (es. CSSPX.MI -> CSSPX)
    clean_name = ticker.split('.')[0]
    print(f"Scaricamento dati storici (10 anni) per {clean_name}...")
    
    # period="10y" scarica i dati degli ultimi 10 anni
    # auto_adjust=True sposta il valore rettificato nella colonna 'Close'
    data = yf.download(ticker, period="10y", interval="1d", auto_adjust=True)
    
    if not data.empty:
        # Gestione del formato MultiIndex di yfinance per avere solo la colonna Close
        if isinstance(data.columns, pd.MultiIndex):
            # Se yfinance restituisce più livelli, prendiamo Close e il ticker specifico
            close_data = data['Close'][ticker].to_frame()
        else:
            close_data = data[['Close']]
        
        # Rinominiamo la colonna in 'Price' e arrotondiamo a 5 decimali
        close_data.columns = ['Price']
        close_data['Price'] = close_data['Price'].round(5)
        
        # Salvataggio nel percorso specifico data/ETF/nome.csv
        filename = f"{output_dir}/{clean_name}.csv"
        close_data.to_csv(filename)
        print(f"Salvato con successo: {filename}")
    else:
        print(f"Errore: Nessun dato trovato per {ticker}")
