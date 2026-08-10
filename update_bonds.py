import os
import requests
import pandas as pd
from datetime import datetime

# Ti basta cambiare questo codice per aggiornare uno qualsiasi dei tuoi bond (es. IT000554740 o IT0005696338)
ISIN = "IT0005494239"

OUTPUT_DIR = "output"
FILE_PATH = os.path.join(OUTPUT_DIR, f"{ISIN}.csv")

# Costruzione automatica dell'URL RAW corretto partendo dai dati della tua repo
USER = "MarioPEL58"
REPO = "portfolio-streamlit"
BRANCH = "dev"
RAW_CSV_URL = f"https://githubusercontent.com{USER}/{REPO}/{BRANCH}/data/bonds/{ISIN}.csv"

def get_closing_price(isin_code):
    """Interroga l'API ufficiale di Euronext per recuperare l'ultimo prezzo di chiusura."""
    url = f"https://euronext.com{isin_code}-MOTX"
    headers = {"User-Agent": "Mozilla/5.0", "X-Requested-With": "XMLHttpRequest"}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        if data and "lastPrice" in data and data["lastPrice"] is not None:
            price_str = str(data["lastPrice"]).replace(",", ".").strip()
            return float(price_str)
        return None
    except Exception as e:
        print(f"Errore API Euronext per {isin_code}: {e}")
        return None

def main():
    # Formato data di Investing Italia: GG.MM.AAAA (es. 10.08.2026)
    current_date = datetime.now().strftime("%d.%m.%Y")
    
    print(f"Avvio aggiornamento dinamico per l'ISIN {ISIN}...")
    close_price = get_closing_price(ISIN)
    
    if close_price is None:
        print("Errore: Impossibile recuperare il prezzo da Euronext.")
        return

    # Scarica lo storico esistente sfruttando l'URL RAW generato in automatico
    try:
        print(f"Lettura dati storici da GitHub: {RAW_CSV_URL}")
        df_old = pd.read_csv(RAW_CSV_URL)
        print("Storico letto con successo.")
    except Exception as e:
        print(f"Impossibile leggere lo storico online ({e}). Genero un file vuoto.")
        df_old = pd.DataFrame(columns=["Data", "Ultimo", "Apertura", "Massimo", "Minimo", "Var. %"])

    # Evita duplicati se lo script viene eseguito due volte nello stesso giorno
    if current_date in df_old["Data"].astype(str).values:
        print(f"I dati di oggi ({current_date}) sono già presenti. Salto l'operazione.")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        df_old.to_csv(FILE_PATH, index=False)
        return

    # Calcolo Var. % rispetto all'ultimo giorno utile (riga 0 nel formato inverso di Investing)
    var_percent = "0,00%"
    if not df_old.empty:
        try:
            last_price_str = str(df_old["Ultimo"].iloc[0]).replace(",", ".")
            last_recorded_price = float(last_price_str)
            diff = ((close_price - last_recorded_price) / last_recorded_price) * 100
            var_percent = f"{diff:+.2f}%".replace(".", ",")
        except Exception as calc_error:
            print(f"Nota: Errore calcolo variazione ({calc_error}). Imposto 0,00%")

    # Formattazione prezzo con la virgola italiana
    close_price_str = f"{close_price:.2f}".replace(".", ",")

    # Creazione della nuova riga da posizionare in cima
    new_row = pd.DataFrame([{
        "Data": current_date,
        "Ultimo": close_price_str,
        "Apertura": close_price_str,
        "Massimo": close_price_str,
        "Minimo": close_price_str,
        "Var. %": var_percent
    }])

    # Unione posizionando il giorno più recente in prima riga (indice 0)
    df_combined = pd.concat([new_row, df_old], ignore_index=True)

    # Salvataggio nella cartella temporanea locale prima dell'invio via YAML
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df_combined.to_csv(FILE_PATH, index=False)
    
    print(f"\n--- FILE GENERATO NELLA CARTELLA OUTPUT ---")
    print(f"Inserito in cima: {current_date} | Prezzo: {close_price_str} | Var: {var_percent}")

if __name__ == "__main__":
    main()
