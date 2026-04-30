import pandas as pd
import requests
import io
import os

# 1. Setup Folders
output_dir = 'data/Tassi'
os.makedirs(output_dir, exist_ok=True)
euribor_filename = f"{output_dir}/Euribor.csv"

# 2. ECB Configuration
# Using the specific Dataflow ID (FM) and the Series Key
series = {
    "1M": "D.U2.EUR.RT.MM.EURIBOR1MD_.HSTA",
    "3M": "D.U2.EUR.RT.MM.EURIBOR3MD_.HSTA",
    "6M": "D.U2.EUR.RT.MM.EURIBOR6MD_.HSTA",
    "12M": "D.U2.EUR.RT.MM.EURIBOR1YD_.HSTA",
}

def fetch_euribor_data():
    all_series = []
    
    for tenor, key in series.items():
        # The URL must follow this exact pattern: /service/data/FM/{key}
        # We add ?format=csvdata to get the table format
        url = f"https://data-api.ecb.europa.eu/service/data/FM/{key}?format=csvdata&startPeriod=2010-01-01"
        
        try:
            print(f"Processing {tenor}...")
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            
            # Load the CSV data
            df = pd.read_csv(io.StringIO(response.text))
            
            # Filter and rename columns
            df = df[['TIME_PERIOD', 'OBS_VALUE']].copy()
            df.columns = ['Date', 'Rate']
            df['Date'] = pd.to_datetime(df['Date'])
            df['Rate'] = pd.to_numeric(df['Rate'], errors='coerce')
            
            # Convert Annual Rate to Daily Growth Factor: 1 + (Rate / 100 / 360)
            df[tenor] = 1 + (df['Rate'] / 100 / 360)
            
            all_series.append(df.set_index('Date')[[tenor]])
            print(f"✓ Successfully fetched {tenor}")
            
        except Exception as e:
            print(f"⚠ Error fetching {tenor}: {str(e)}")
            continue

    if all_series:
        # Merge all tenors, fill weekends, and calculate the Price Index (starting at 100)
        df_combined = pd.concat(all_series, axis=1).sort_index().ffill()
        df_prices = (df_combined.cumprod() * 100).round(5)
        
        # Save to CSV
        df_prices.to_csv(euribor_filename)
        print(f"✅ File saved successfully: {euribor_filename}")
    else:
        print("❌ No data collected. Check the log for errors.")
        exit(1)

if __name__ == "__main__":
    fetch_euribor_data()
