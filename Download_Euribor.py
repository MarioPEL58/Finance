import pandas as pd
import requests
import io
import os

# Setup folders
output_dir = 'data/Tassi'
os.makedirs(output_dir, exist_ok=True)
euribor_filename = f"{output_dir}/Euribor.csv"

# ECB Keys
series = {
    "1M": "FM.D.U2.EUR.RT.MM.EURIBOR1MD_.HSTA",
    "3M": "FM.D.U2.EUR.RT.MM.EURIBOR3MD_.HSTA",
    "6M": "FM.D.U2.EUR.RT.MM.EURIBOR6MD_.HSTA",
    "12M": "FM.D.U2.EUR.RT.MM.EURIBOR1YD_.HSTA",
}

all_series = []

print("Starting Euribor Update...")

for tenor, key in series.items():
    # This specific URL structure is required by the new ECB Data Portal
    url = f"https://data-api.ecb.europa.eu/service/data/ECB,FM,1.0/{key}?format=csvdata&startPeriod=2010-01-01"
    
    try:
        print(f"Fetching {tenor}...")
        response = requests.get(url, timeout=30)
        response.raise_for_status() # This will catch 404 or 400 errors
        
        df = pd.read_csv(io.StringIO(response.text))
        df = df[['TIME_PERIOD', 'OBS_VALUE']].copy()
        df.columns = ['Date', 'Rate']
        df['Date'] = pd.to_datetime(df['Date'])
        df['Rate'] = pd.to_numeric(df['Rate'], errors='coerce')
        
        # Math: Daily Factor
        df[tenor] = 1 + (df['Rate'] / 100 / 360)
        
        all_series.append(df.set_index('Date')[[tenor]])
        print(f"✓ {tenor} data received.")

    except Exception as e:
        print(f"⚠ Error for {tenor}: {e}")

if all_series:
    # Combine and calculate Price Index
    df_combined = pd.concat(all_series, axis=1).sort_index().ffill()
    df_prices = (df_combined.cumprod() * 100).round(5)
    
    # Save
    df_prices.to_csv(euribor_filename)
    print(f"✅ Success! Saved to {euribor_filename}")
else:
    print("❌ No data was saved.")
    exit(1)
