import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta

tickers = ["CSSPX.MI", "XDAX.MI", "CSMIB.MI", "XESP.DE", "SXRW.DE", "XMEU.MI", "SGAJ.DE", "SW2CHB.MI"]
output_dir = 'data/ETF'

for ticker in tickers:
    clean_name = ticker.split('.')[0]
    filename = f"{output_dir}/{clean_name}.csv"
    
    # 1. Check if file exists to determine start date
    if os.path.exists(filename):
        existing_df = pd.read_csv(filename, index_col=0, parse_dates=True)
        # Start from the day after the last date in the file
        last_date = existing_df.index[-1]
        start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"Updating {clean_name} starting from {start_date}...")
    else:
        existing_df = pd.DataFrame()
        start_date = (datetime.now() - timedelta(days=365*10)).strftime('%Y-%m-%d')
        print(f"File not found. Downloading full 10y history for {clean_name}...")

    # 2. Download only the missing data
    # We use end=today to ensure we get everything up to the latest close
    new_data = yf.download(ticker, start=start_date, auto_adjust=True)

    if not new_data.empty:
        # Clean the new data (handle MultiIndex if necessary)
        if isinstance(new_data.columns, pd.MultiIndex):
            new_rows = new_data['Close'][ticker].to_frame()
        else:
            new_rows = new_data[['Close']]
            
        new_rows.columns = ['Price']
        new_rows['Price'] = new_rows['Price'].round(5)

        # 3. Combine and save
        updated_df = pd.concat([existing_df, new_rows])
        
        # Drop duplicates just in case of overlapping dates
        updated_df = updated_df[~updated_df.index.duplicated(keep='last')]
        
        updated_df.to_csv(filename)
        print(f"✅ {clean_name} updated.")
    else:
        print(f"ℹ️ No new data found for {clean_name} (Market might be closed).")
