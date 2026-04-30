import pandas as pd
import requests
import io
import os

# 1. Setup Folders
output_dir = 'data/Tassi'
os.makedirs(output_dir, exist_ok=True)
euribor_filename = f"{output_dir}/Euribor.csv"

# 2. Series Keys (Clean 2026 format)
series = {
    "1M": "M.U2.EUR.RT.MM.EURIBOR1MD_.HSTA",
    "3M": "M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA",
    "6M": "M.U2.EUR.RT.MM.EURIBOR6MD_.HSTA",
    "12M": "M.U2.EUR.RT.MM.EURIBOR1YD_.HSTA"
}

all_series = []

# Use a Session to handle cookies/headers more like a real browser
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Accept': 'text/csv'
})

print("Starting Euribor Update (Manual-Mirror Mode)...")

for tenor, key in series.items():
    # Exactly the string that worked for you
    url = f"https://data-api.ecb.europa.eu/service/data/FM/{key}?format=csvdata&startPeriod=2010-01-01"
    
    try:
        print(f"Fetching {tenor}...")
        response = session.get(url, timeout=30)
        
        if response.status_code == 200:
            # We use io.BytesIO(response.content) to handle the file stream safely
            df = pd.read_csv(io.BytesIO(response.content))
            
            # Clean up column names
            df.columns = [c.strip().upper() for c in df.columns]
            
            if 'TIME_PERIOD' in df.columns and 'OBS_VALUE' in df.columns:
                df = df[['TIME_PERIOD', 'OBS_VALUE']].copy()
                df.columns = ['Date', 'Rate']
                df['Date'] = pd.to_datetime(df['Date'])
                df['Rate'] = pd.to_numeric(df['Rate'], errors='coerce')
                
                # Math: Daily growth factor (1 + Rate/100/360)
                df[tenor] = 1 + (df['Rate'] / 100 / 360)
                
                all_series.append(df.set_index('Date')[[tenor]])
                print(f"✓ {tenor} Success")
            else:
                print(f"⚠ Format Error for {tenor}. Found: {df.columns.tolist()}")
        else:
            print(f"⚠ {tenor} failed with status {response.status_code}")

    except Exception as e:
        print(f"⚠ Request error for {tenor}: {e}")

# 3. Save Results
if all_series:
    df_combined = pd.concat(all_series, axis=1).sort_index()
    
    # Fill daily gaps for the price index
    all_days = pd.date_range(start=df_combined.index.min(), end=pd.Timestamp.today(), freq='D')
    df_combined = df_combined.reindex(all_days).ffill()
    
    # Cumulative Product = Price Index (Starting at 100)
    df_prices = (df_combined.cumprod() * 100).round(5)
    
    df_prices.to_csv(euribor_filename)
    print(f"✅ Success! Data saved to {euribor_filename}")
else:
    print("❌ No data collected.")
    exit(1)
