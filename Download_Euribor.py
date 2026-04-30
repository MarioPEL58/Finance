import requests
import pandas as pd
import io
import os

# 1. Define the new directory
output_dir = 'data/Tassi'
euribor_filename = f"{output_dir}/Euribor.csv"

# 2. Ensure the directory exists
os.makedirs(output_dir, exist_ok=True)
print(f"Directory {output_dir} is ready.")

# Configuration
maturities = {
    "1M": "FM.D.U2.EUR.RT.MM.EURIBOR1MD_.HSTA",
    "3M": "FM.D.U2.EUR.RT.MM.EURIBOR3MD_.HSTA",
    "6M": "FM.D.U2.EUR.RT.MM.EURIBOR6MD_.HSTA",
    "12M": "FM.D.U2.EUR.RT.MM.EURIBOR1YD_.HSTA"
}

def fetch_ecb_series(key):
    url = f"https://sdw-wsrest.ecb.europa.eu/service/data/FM/{key}?format=csvdata"
    response = requests.get(url)
    if response.status_code == 200:
        df = pd.read_csv(io.StringIO(response.text))[['TIME_PERIOD', 'OBS_VALUE']]
        df.columns = ['Date', 'Rate']
        df['Date'] = pd.to_datetime(df['Date'])
        return df.set_index('Date').sort_index()
    return pd.DataFrame()

print("Generating Euribor Price Index...")

all_series = []
for label, key in maturities.items():
    df_rate = fetch_ecb_series(key)
    if not df_rate.empty:
        # Convert annual percentage rate to daily growth factor
        # Logic: (1 + (Rate / 100 * (1/360)))
        df_rate[label] = 1 + (df_rate['Rate'] / 100 / 360)
        all_series.append(df_rate[[label]])

if all_series:
    # Merge all maturities into one DataFrame
    df_combined = pd.concat(all_series, axis=1).sort_index()
    
    # Fill missing values (weekends/holidays) with the last available rate
    df_combined = df_combined.ffill()
    
    # Calculate the Cumulative Product to create the "Price" index
    # Starting at a base of 100
    df_prices = df_combined.cumprod() * 100
    
    # Round to 5 decimals as per your ETF format
    df_prices = df_prices.round(5)
    
    # Save to CSV
    df_prices.to_csv(euribor_filename)
    print(f"✅ Successfully created {euribor_filename} with 1M, 3M, 6M, 12M columns.")
