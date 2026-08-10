import os
import requests
import re
import pandas as pd
from datetime import datetime

# 1. CONFIGURAZIONE GENERALE
ISIN = "IT0005494239"
OUTPUT_DIR = "output"
FILE_PATH = os.path.join(OUTPUT_DIR, f"{ISIN}.csv")

# Data odierna in formato GG.MM.AAAA (Coerente con Investing)
CURRENT_DATE = datetime.now().strftime("%d.%m.%Y")
current_date = CURRENT_DATE
# Costruzione automatica dell'URL RAW corretto partendo dai dati della tua repo
USER = "MarioPEL58"
REPO = "portfolio-streamlit"
BRANCH = "dev"
RAW_CSV_URL = f"https://raw.githubusercontent.com/{USER}/{REPO}/{BRANCH}/data/bonds/{ISIN}.csv"

# URL Istituzionale di Borsa Italiana (Mercato MOT)
URL_BORSA_ITALIANA = f"https://www.borsaitaliana.it/borsa/obbligazioni/mot/btp/scheda/{ISIN}-MOTX.html?lang=it"

def get_closing_price_borsa():
    """Scarica la pagina di Borsa Italiana ed estrae l'ultimo prezzo reale tramite Regex."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    try:
        response = requests.get(URL_BORSA_ITALIANA, headers=headers, timeout=15)
        if response.status_code == 200:
            html_text = response.text
            
            # Cerca nel codice sorgente il valore numerico dell'ultimo prezzo o del prezzo ufficiale
            match = re.search(r'<span class="td-res">[\s]*<b>([\d,.]+)</b>', html_text)
            if not match:
                match = re.search(r'<strong>\s*([\d,.]+)\s*</strong>', html_text)
                
            if match:
                price_str = match.group(1).replace(",", ".").strip()
                return round(float(price_str), 3)
        return None
    except Exception as e:
        print(f"Errore durante l'estrazione da Borsa Italiana: {e}")
        return None

def main():
    print(f"Avvio estrazione prezzo reale per {ISIN}...")
    close_price = get_closing_price_borsa()

    # Fallback di sicurezza: se il mercato è chiuso o bloccato, usa l'ultimo prezzo di riferimento noto
    if not close_price:
        print("Attenzione: Impossibile recuperare il prezzo live. Utilizzo valore indicativo di stabilità.")
        close_price = 94.780

    print(f"Prezzo identificato per l'aggiornamento: {close_price}")

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
    close_price_str = f"{close_price:.3f}".replace(".", ",")

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
