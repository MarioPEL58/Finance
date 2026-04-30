import pandas as pd
import requests
import io
import os

# 1. Setup Folders
output_dir = 'data/Tassi'
os.makedirs(output_dir, exist_ok=True)
euribor_filename = f"{output_dir}/Euribor.csv"

# 2. ECB Keys (Updated to be more flexible)
# We use the specific codes but handle them carefully in the URL
maturities = {
    "1M": "D.U2.EUR.RT.MM.EURIBOR1MD_.HSTA",
    "3M": "D.U2.EUR.RT.MM.EURIBOR3MD_.HSTA",
    "6M": "D.U2.EUR.RT.MM.EURIBOR6MD_.HSTA",
    "12M": "D.U2.EUR.RT.MM.EURIBOR1YD_.HSTA",
}

all_series = []

print("Starting Euribor Update...")

for label, key in maturities.items():
    # Attempting the most direct path supported in 2026
    url = f"https://data-api.ecb.europa.eu/service/data/FM/{key}"
    
    # Parameters sent as a dictionary to ensure proper encoding
    params = {
        'format': 'csvdata',
        'startPeriod': '2010-01-01',
        'detail': 'dataonly'
    }
    
    try:
        print(f"Fetching {label}...")
        # The ECB server often REJECTS requests that don't have these headers
        headers = {
            'Accept': 'text/csv',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
        }
        
        response = requests.get(url, params=params, headers=headers, timeout=30)
        
        if response.status_code == 200:
            df = pd.read_csv(io.StringIO(response.text))
            
            # Normalize column names
            df.columns = [c.strip().upper() for c in df.columns]
            
            if 'TIME_PERIOD' in df.columns and 'OBS_VALUE' in df.columns:
                df = df[['TIME_PERIOD', 'OBS_VALUE']].copy()
                df.columns = ['Date', 'Rate']
                df['Date'] = pd.to_datetime(df['Date'])
                df['Rate'] = pd.to_numeric(df['Rate'], errors='coerce')
                
                # Formula: Daily Growth Factor
                df[label] = 1 + (df['Rate'] / 100 / 360)
                
                all_series.append(df.set_index('Date')[[label]])
                print(f"✓ {label} Success")
            else:
                print(f"⚠ Column mismatch. Found: {list(df.columns)}")
        else:
            print(f"⚠ {label} Error {response.status_code}: {response.text[:100]}")

    except Exception as e:
        print(f"⚠ Request failed for {label}: {e}")

# 3. Final Merge
if all_series:
    # Join columns, fill weekends, and calculate the cumulative index
    df_combined = pd.concat(all_series, axis=1).sort_index().ffill()
    df_prices = (df_combined.cumprod() * 100).round(5)
    
    df_prices.to_csv(euribor_filename)
    print(f"✅ Final file saved: {euribor_filename}")
else:
    print("❌ No data was retrieved. Script stopped.")
    exit(1)
