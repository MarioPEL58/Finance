import os
import requests

ISIN = "IT0005494239"
# Crea la sottocartella locale per emulare la destinazione di streamlit
FILE_PATH = os.path.join("data", "bonds", f"{ISIN}.csv")

def get_closing_price(isin_code):
    url = f"https://euronext.com{isin_code}-MOTX"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "X-Requested-With": "XMLHttpRequest"
    }
    try:
        response = requests.get(url, headers=headers, timeout=15)
        data = response.json()
        if data and "lastPrice" in data:
            price_str = str(data["lastPrice"]).replace(",", ".").strip()
            return float(price_str)
        return None
    except Exception as e:
        print(f"Errore API: {e}")
        return None

def main():
    import pandas as pd
    from datetime import datetime
    
    current_date = datetime.now().strftime("%Y-%m-%d")
    close_price = get_closing_price(ISIN)
    
    if close_price is None:
        return

    new_record = pd.DataFrame([{"Data": current_date, "Chiusura": close_price}])
    os.makedirs(os.path.dirname(FILE_PATH), exist_ok=True)

    # Nota: Poiché siamo in un altro repository, lo script creerà semplicemente il file aggiornato di oggi.
    # Sarà poi il flusso GitHub Actions a fare il merge intelligente clonando il vero portfolio-streamlit.
    new_record.to_csv(FILE_PATH, index=False)
    print(f"Generato file temporaneo odierno per {ISIN} con prezzo {close_price}")

if __name__ == "__main__":
    main()
