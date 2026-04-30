import pandas as pd
import requests
import io
import os

# 1. Setup Folders
output_dir = 'data/Tassi'
os.makedirs(output_dir, exist_ok=True)
euribor_filename = f"{output_dir}/Euribor.csv"

# 2. Updated 2026 ECB Series Keys
# Note: The 'M' after 'FM' stands for Monthly frequency. 
# The ECB now prioritizes monthly validated data for these benchmarks.
series = {
    "1M": "FM.M.U2.EUR.RT.MM.EURIBOR1MD_.HSTA",
    "3M": "FM.M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA",
    "6M": "FM.M.U2.EUR.RT.MM.EURIBOR6MD_.HSTA",
    "12M": "FM.M.U2.EUR.RT.MM.EURIBOR1YD_.HSTA"
}

all_series = []

print("Starting Euribor Update (SDMX 3.0)...")

for tenor, full_key in series.items():
    # The new 2026 URL structure: Agency (ECB), Dataflow (FM), Version (1.0)
    url = f"https://data-api.ecb.europa.eu/service/data/ECB,FM,1.0/{full_key}"
    
    params = {
        'format': 'csvdata',
        'startPeriod': '2010-01-01'
    }
    
    try:
        print(f"Fetching {tenor}...")
        # Essential headers for the new ECB Gateway
        headers = {
            'Accept': 'text/csv',
            'User-Agent': 'GitHub-Action-Ticker-Updater'
        }
        
        response = requests.get(url, params=params, headers=headers, timeout=30)
        
        if response.status_code == 200:
            df = pd.read_csv(io.StringIO(response.text))
            
            # Normalize column names (new API returns them in uppercase)
            df.columns = [c.upper() for c in df.columns]
            
            if 'TIME_PERIOD' in df.columns and 'OBS_VALUE' in df.columns:
                df = df[['TIME_PERIOD', 'OBS_VALUE']].copy()
                df.columns = ['Date', 'Rate']
                # Convert to monthly start dates
                df['Date'] = pd.to_datetime(df['Date'])
                df['Rate'] = pd.to_numeric(df['Rate'], errors='coerce')
                
                # Math: Daily factor for monthly-reported rates
                # Logic: (1 + (Rate / 100 / 360))
                df[tenor] = 1 + (df['Rate'] / 100 / 360)
                
                all_series.append(df.set_index('Date')[[tenor]])
                print(f"✓ {tenor} processed.")
            else:
                print(f"⚠ Column mismatch for {tenor}: {df.columns.tolist()}")
        else:
            print(f"⚠ {tenor} failed with status {response.status_code}")

    except Exception as e:
        print(f"⚠ Request error for {tenor}: {e}")

# 3. Save Results
if all_series:
    # Merge and forward-fill to daily dates if needed
    df_combined = pd.concat(all_series, axis=1).sort_index()
    
    # Reindex to daily if you want a daily price index from monthly data
    all_days = pd.date_range(start=df_combined.index.min(), end=df_combined.index.max(), freq='D')
    df_combined = df_combined.reindex(all_days).ffill()
    
    # Price Index starting at 100
    df_prices = (df_combined.cumprod() * 100).round(5)
    
    df_prices.to_csv(euribor_filename)
    print(f"✅ Success! Saved to {euribor_filename}")
else:
    print("❌ No data collected.")
    exit(1)
