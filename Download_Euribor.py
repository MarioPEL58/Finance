import requests
import pandas as pd
import io
import os

# 1. Define the directory
output_dir = 'data/Tassi'
euribor_filename = f"{output_dir}/Euribor.csv"

# 2. Ensure the directory exists
os.makedirs(output_dir, exist_ok=True)

# Configuration
maturities = {
    "1M": "FM.D.U2.EUR.RT.MM.EURIBOR1MD_.HSTA",
    "3M": "FM.D.U2.EUR.RT.MM.EURIBOR3MD_.HSTA",
    "6M": "FM.D.U2.EUR.RT.MM.EURIBOR6MD_.HSTA",
    "12M": "FM.D.U2.EUR.RT.MM.EURIBOR1YD_.HSTA"
}

def fetch_ecb_series(key):
    url = f"https://sdw-wsrest.ecb.europa.eu/service/data/FM/{key}?format=csvdata"
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            # Load and force numeric values for the Rate
            df = pd.read_csv(io.StringIO(response.text))
            df = df[['TIME_PERIOD', 'OBS_VALUE']].copy()
            df.columns = ['Date', 'Rate']
            df['Date'] = pd.to_datetime(df['Date'])
            df['Rate'] = pd.to_numeric(df['Rate'], errors='coerce')
            return df.set_index('Date').sort_index()
    except Exception as e:
        print(f"Error fetching {key}: {e}")
    return pd.DataFrame()

print("Generating Euribor Price Index...")

all_series = []
for label, key in maturities.items():
    print(f"Processing {label}...")
    df_rate = fetch_ecb_series(key)
    if not df_rate.empty:
        # Calculate daily factor: 1 + (Rate / 100 / 360)
        # We use .to_frame() and a new name to avoid slice warnings
        daily_factor = (1 + (df_rate['Rate'] / 100 / 360)).to_frame(name=label)
        all_series.append(daily_factor)

if all_series:
    # Merge all maturities
    df_combined = pd.concat(all_series, axis=1).sort_index()
    
    # Fill missing values (important for weekends/holidays)
    df_combined = df_combined.ffill()
    
    # Calculate Cumulative Product (Price Index)
    # If the rate was 0, factor is 1, price stays same.
    # If rate is negative, factor < 1, price drops.
    df_prices = df_combined.cumprod() * 100
    
    # Final cleanup
    df_prices = df_prices.dropna().round(5)
    
    # Save to CSV
    df_prices.to_csv(euribor_filename)
    print(f"✅ Successfully created {euribor_filename}")
else:
    print("❌ No data collected. Script failed.")
    exit(1)
