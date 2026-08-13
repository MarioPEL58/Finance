import os
import requests
import re
import pandas as pd
from datetime import datetime

# ==========================================
# CONFIGURAZIONE
# ==========================================

with open("isins.txt", "r") as f:
    ISINS = [
        line.strip()
        for line in f
        if line.strip()
    ]

USER = "MarioPEL58"
REPO = "portfolio-streamlit"
BRANCH = "dev"

OUTPUT_DIR = "output"

CURRENT_DATE = datetime.now().strftime("%d.%m.%Y")

# ==========================================
# ESTRAZIONE PREZZO DA BORSA ITALIANA
# ==========================================

def get_closing_price_borsa(url):

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)

        if response.status_code == 200:
            html_text = response.text

            match = re.search(
                r'<span class="td-res">\s*<b>([\d,.]+)</b>',
                html_text
            )

            if not match:
                match = re.search(
                    r'<strong>\s*([\d,.]+)\s*</strong>',
                    html_text
                )

            if match:
            
                price_str = match.group(1).strip()
            
                # Formato italiano:
                # 1.023,15 -> 1023.15
            
                price_str = (
                    price_str
                    .replace(".", "")
                    .replace(",", ".")
                )
            
                price_value = float(price_str)
            
                return round(price_value, 3)

        return None

    except Exception as e:
        print(f"Errore durante l'estrazione da Borsa Italiana: {e}")
        return None
        
def get_borsa_url(isin):

    urls = [
        f"https://www.borsaitaliana.it/borsa/obbligazioni/mot/btp/scheda/{isin}-MOTX.html?lang=it",
        f"https://www.borsaitaliana.it/borsa/cw-e-certificates/scheda/{isin}-SEDX.html?lang=it"
    ]

    for url in urls:

        try:

            prezzo = get_closing_price_borsa(url)

            if prezzo is not None:

                print(f"URL valido trovato: {url}")
                return url

        except Exception:
            pass

    return None
# ==========================================
# AGGIORNAMENTO SINGOLO ISIN
# ==========================================

# def process_isin(isin):

#     file_path = os.path.join(OUTPUT_DIR, f"{isin}.csv")

#     raw_csv_url = (
#         f"https://raw.githubusercontent.com/"
#         f"{USER}/{REPO}/{BRANCH}/data/bonds/{isin}.csv"
#     )

#     url_borsa = (
#         f"https://www.borsaitaliana.it/borsa/obbligazioni/"
#         f"mot/btp/scheda/{isin}-MOTX.html?lang=it"
#     )

#     print(f"\n===== ELABORAZIONE {isin} =====")

#     close_price = get_closing_price_borsa(url_borsa)

#     if not close_price:
#         print("Prezzo non disponibile. Utilizzo fallback.")
#         close_price = 94.780

#     print(f"Prezzo individuato: {close_price}")

#     try:
#         print(f"Lettura storico da: {raw_csv_url}")

#         df_old = pd.read_csv(raw_csv_url)

#         print("Storico letto correttamente.")

#     except Exception as e:

#         print(f"Storico non disponibile ({e})")

#         df_old = pd.DataFrame(
#             columns=[
#                 "Data",
#                 "Ultimo",
#                 "Apertura",
#                 "Massimo",
#                 "Minimo",
#                 "Var. %"
#             ]
#         )

#     if (
#         not df_old.empty and
#         CURRENT_DATE in df_old["Data"].astype(str).values
#     ):

#         print(f"Dati del {CURRENT_DATE} già presenti.")

#         os.makedirs(OUTPUT_DIR, exist_ok=True)

#         df_old.to_csv(file_path, index=False)

#         return

#     var_percent = "0,00%"

#     if not df_old.empty:

#         try:

#             last_price = float(
#                 str(df_old["Ultimo"].iloc[0]).replace(",", ".")
#             )

#             diff = (
#                 (close_price - last_price)
#                 / last_price
#             ) * 100

#             var_percent = (
#                 f"{diff:+.2f}%"
#                 .replace(".", ",")
#             )

#         except Exception as calc_error:

#             print(
#                 f"Errore calcolo variazione: {calc_error}"
#             )

#     close_price_str = (
#         f"{close_price:.3f}"
#         .replace(".", ",")
#     )

#     new_row = pd.DataFrame([
#         {
#             "Data": CURRENT_DATE,
#             "Ultimo": close_price_str,
#             "Apertura": close_price_str,
#             "Massimo": close_price_str,
#             "Minimo": close_price_str,
#             "Var. %": var_percent
#         }
#     ])

#     df_combined = pd.concat(
#         [new_row, df_old],
#         ignore_index=True
#     )

#     os.makedirs(OUTPUT_DIR, exist_ok=True)

#     df_combined.to_csv(
#         file_path,
#         index=False
#     )

#     print("\nFILE GENERATO:")
#     print(file_path)
#     print(
#         f"{CURRENT_DATE} | "
#         f"{close_price_str} | "
#         f"{var_percent}"
#     )
def process_isin(isin):

    file_path = os.path.join(OUTPUT_DIR, f"{isin}.csv")

    raw_csv_url = (
        f"https://raw.githubusercontent.com/"
        f"{USER}/{REPO}/{BRANCH}/data/bonds/{isin}.csv"
    )

    # url_borsa = (
    #     f"https://www.borsaitaliana.it/borsa/obbligazioni/"
    #     f"mot/btp/scheda/{isin}-MOTX.html?lang=it"
    # )

    print(f"\n===== ELABORAZIONE {isin} =====")
    url_borsa = get_borsa_url(isin)
    
    if not url_borsa:
        print(f"Nessuna scheda trovata per {isin}")
        return
    
    print(f"URL utilizzato: {url_borsa}")
    
    close_price = get_closing_price_borsa(url_borsa)
    
    print(f"Prezzo estratto: {close_price}")
    
    if close_price is None:
    
        print(f"Prezzo non disponibile per {isin}. File non aggiornato.")
        return
    
    print(f"Prezzo individuato: {close_price}")

    try:

        print(f"Lettura storico da: {raw_csv_url}")

        df_old = pd.read_csv(raw_csv_url)

        print("Storico letto correttamente.")

    except Exception as e:

        print(f"Storico non disponibile ({e})")

        df_old = pd.DataFrame(
            columns=[
                "Data",
                "Ultimo",
                "Apertura",
                "Massimo",
                "Minimo",
                "Var. %"
            ]
        )

    is_minimal_format = (
        set(df_old.columns) == {"Date", "Close"}
    )
    
    is_new_format = (
        "Date" in df_old.columns and
        "Close" in df_old.columns and
        not is_minimal_format
    )

    # controllo duplicati

    if not df_old.empty:

        if is_new_format or is_minimal_format:

            current_date_new = pd.to_datetime(
                CURRENT_DATE,
                format="%d.%m.%Y"
            ).strftime("%d/%m/%Y")

            if current_date_new in df_old["Date"].astype(str).values:

                print(f"Dati del {CURRENT_DATE} già presenti.")

                os.makedirs(OUTPUT_DIR, exist_ok=True)

                df_old.to_csv(file_path, index=False)

                return

        else:

            if CURRENT_DATE in df_old["Data"].astype(str).values:

                print(f"Dati del {CURRENT_DATE} già presenti.")

                os.makedirs(OUTPUT_DIR, exist_ok=True)

                df_old.to_csv(file_path, index=False)

                return

    var_percent = "0,00%"

    if not df_old.empty:

        try:

            if is_new_format or is_minimal_format:

                last_price = float(df_old["Close"].iloc[0])

            else:

                last_price = float(
                    str(df_old["Ultimo"].iloc[0])
                    .replace(",", ".")
                )

            diff = (
                (close_price - last_price)
                / last_price
            ) * 100

            var_percent = (
                f"{diff:+.2f}%"
                .replace(".", ",")
            )

        except Exception as calc_error:

            print(
                f"Errore calcolo variazione: {calc_error}"
            )

    # nuova riga

    if is_minimal_format:
    
        current_date_new = pd.to_datetime(
            CURRENT_DATE,
            format="%d.%m.%Y"
        ).strftime("%d/%m/%Y")
    
        new_row = pd.DataFrame([
            {
                "Date": current_date_new,
                "Close": close_price
            }
        ])
    
    elif is_new_format:
    
        current_date_new = pd.to_datetime(
            CURRENT_DATE,
            format="%d.%m.%Y"
        ).strftime("%d/%m/%Y")
    
        new_row = pd.DataFrame([
            {
                "Date": current_date_new,
                "Open": close_price,
                "High": close_price,
                "Low": close_price,
                "Last": close_price,
                "Close": close_price,
                "Number of Shares": 0,
                "Number of Trades": 0,
                "Turnover": 0
            }
        ])

    else:

        close_price_str = (
            f"{close_price:.3f}"
            .replace(".", ",")
        )

        new_row = pd.DataFrame([
            {
                "Data": CURRENT_DATE,
                "Ultimo": close_price_str,
                "Apertura": close_price_str,
                "Massimo": close_price_str,
                "Minimo": close_price_str,
                "Var. %": var_percent
            }
        ])

    # merge

    df_combined = pd.concat(
        [new_row, df_old],
        ignore_index=True
    )

    # salvataggio

    os.makedirs(
        OUTPUT_DIR,
        exist_ok=True
    )

    df_combined.to_csv(
        file_path,
        index=False
    )

    print("\nFILE GENERATO:")
    print(file_path)

    if is_new_format or is_minimal_format:

        print(
            f"{current_date_new} | "
            f"{close_price:.3f}"
        )

    else:

        print(
            f"{CURRENT_DATE} | "
            f"{close_price_str} | "
            f"{var_percent}"
        )


# ==========================================
# MAIN
# ==========================================

if __name__ == "__main__":

    for isin in ISINS:
        process_isin(isin)

    print("\nElaborazione completata.")
