import pandas as pd
import requests
import io
import os

# 1. Setup Folders
output_dir = 'data/Tassi'
os.makedirs(output_dir, exist_ok=True)
euribor_filename = f"{output_dir}/Euribor.csv"

# 2. ECB Keys (CRITICAL: REMOVED "FM." PREFIX)
series = {
    "1M": "D.U2.EUR.RT.MM.EURIBOR1MD_.HSTA",
    "3M": "D.U2.EUR.RT.MM.EURIBOR3MD_.HSTA",
    "6M": "D.U2.EUR.RT.MM.EURIBOR6MD_.HSTA",
    "12M": "D.U2.EUR.RT.MM.EURIBOR1YD_.HSTA",
}

all_series = []

print("Starting Euribor Update...")

for tenor, key in series.items():
    # The URL pattern for the new API
    url = f"https://data-api.ecb.europa.eu/service/data/ECB,FM,1.0/{key}?format=csvdata&startPeriod=2010-01-01"
    
    try:
        print(f"Fetching {tenor}...")
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            df = pd.read_csv(io.StringIO(response.text))
            
            # Use lowercase or check columns safely
            df.columns = [c.upper() for c in df.columns]
            
            if 'TIME_PERIOD' in df.columns and 'OBS_VALUE' in df.columns:
                df = df[['TIME_PERIOD', 'OBS_VALUE']].copy()
                df.columns = ['Date', 'Rate']
                df['Date'] = pd.to_datetime(df['Date'])
                df['Rate'] = pd.to_numeric(df['Rate'], errors='coerce')
                
                # Math: Daily growth factor
                df[tenor] = 1 + (df['Rate'] / 100 / 360)
                
                all_series.append(df.set_index('Date')[[tenor]])
                print(f"✓ {tenor} data successfully processed.")
            else:
                print(f"⚠ {tenor} format error: {df.columns}")
        else:
            print(f"⚠ {tenor} failed with status {response.status_code}")

    except Exception as e:
        print(f"⚠ Exception for {tenor}: {e}")

# 3. Save Results
if all_series:
    df_combined = pd.concat(all_series, axis=1).sort_index().ffill()
    # Calculate cumulative product (Index starts at 100)
    df_prices = (df_combined.cumprod() * 100).round(5)
    
    df_prices.to_csv(euribor_filename)
    print(f"✅ Success! Data saved to {euribor_filename}")
else:
    print("❌ No data was collected.")
    exit(1)
