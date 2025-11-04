import streamlit as st
import pandas as pd
import numpy as np
import os, json, requests
from datetime import datetime
import gspread
from google.oauth2 import service_account
import smtplib
from email.message import EmailMessage

st.set_page_config(page_title="Analyse Fidélité La Tribu", layout="wide")
st.title("🎯 Analyse Fidélité - La Tribu")

# ==========================
# CONFIGURATION
# ==========================
HISTO_FILE = "historique_fidelite.csv"
SPREADSHEET_ID = "1xYQ0mjr37Fnmal8yhi0kBE3_96EN6H6qsYrAtQkHpGo"  # à remplacer
SHEET_NAME = "Données"
LOOKER_URL = "https://lookerstudio.google.com/reporting/a0037205-4433-4f9e-b19b-1601fbf24006"  # ton lien Looker Studio

SMTP_SERVER = st.secrets["email"]["smtp_server"]
SMTP_PORT = st.secrets["email"]["smtp_port"]
SMTP_USER = st.secrets["email"]["smtp_user"]
SMTP_PASSWORD = st.secrets["email"]["smtp_password"]
DEFAULT_RECEIVER = st.secrets["email"]["receiver"]

# Authentification Google
file_id = "12O9eFGFmwTu1n6kF4AIDIm0KXKMIgOvg"  # service account JSON stocké sur Drive
url = f"https://drive.google.com/uc?id={file_id}"
response = requests.get(url)
response.raise_for_status()
gcp_service_account_info = json.loads(response.content)

scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = service_account.Credentials.from_service_account_info(gcp_service_account_info, scopes=scopes)
client = gspread.authorize(creds)

# ==========================
# FONCTIONS UTILITAIRES
# ==========================
def _ensure_date_series(s: pd.Series) -> pd.Series:
    ds = pd.to_datetime(s, errors="coerce")
    return ds.dt.strftime("%Y-%m-%d")

def _gsheet_read_as_df(sheet_id: str, tab_name: str):
    try:
        ws = client.open_by_key(sheet_id).worksheet(tab_name)
    except Exception:
        sh = client.open_by_key(sheet_id)
        ws = sh.add_worksheet(title=tab_name, rows=2, cols=20)
    rows = ws.get_all_values()
    if not rows:
        return pd.DataFrame(), ws
    header = rows[0]
    data = rows[1:]
    if not data:
        return pd.DataFrame(columns=header), ws
    df = pd.DataFrame(data, columns=header)
    return df, ws

def _gsheet_upsert_dataframe(sheet_id: str, tab_name: str, df_new: pd.DataFrame) -> pd.DataFrame:
    df_old, ws = _gsheet_read_as_df(sheet_id, tab_name)
    if not df_old.empty:
        for c in df_new.columns:
            if c not in df_old.columns:
                df_old[c] = pd.NA
        for c in df_old.columns:
            if c not in df_new.columns:
                df_new[c] = pd.NA
        df_old = df_old[df_new.columns]
    if "month" in df_new.columns:
        df_new["month"] = _ensure_date_series(df_new["month"])
    df_all = pd.concat([df_old, df_new], ignore_index=True) if not df_old.empty else df_new.copy()
    ws.clear()
    values = [list(df_all.columns)] + df_all.astype(object).where(pd.notnull(df_all), "").values.tolist()
    ws.update("A1", values)
    return df_all

# ==========================
# INTERFACE STREAMLIT
# ==========================
st.sidebar.header("📂 Importer les fichiers")
transactions_file = st.sidebar.file_uploader("Fichier Transactions (CSV Keyneo)", type=["csv"])
coupons_file = st.sidebar.file_uploader("Fichier Bons d’Achat (CSV Keyneo)", type=["csv"])
emails_supp = st.sidebar.text_input("📧 Autres destinataires (séparés par des virgules)")

if transactions_file and coupons_file:
    tx = pd.read_csv(
    transactions_file,
    sep=";",
    encoding="utf-8-sig",
    on_bad_lines="skip",
    engine="python"
)

    coupons = pd.read_csv(coupons_file, sep=";", encoding="utf-8")

    # Nettoyage et analyse
    tx["ValidationDate"] = pd.to_datetime(tx["ValidationDate"], errors="coerce")
    tx["month"] = tx["ValidationDate"].dt.to_period("M").astype(str)
    tx["LineType"] = tx["LineType"].astype(str).str.upper()
    tx["Label"] = tx["Label"].astype(str).str.upper()

    products = tx[tx["LineType"] == "PRODUCT_SALE"].copy()
    tenders = tx[tx["LineType"] == "TENDER"].copy()

    products["Quantity"] = pd.to_numeric(products.get("Quantity", 1), errors="coerce").fillna(1)
    products["LineGrossAmount"] = pd.to_numeric(products.get("LineGrossAmount"), errors="coerce").fillna(0)
    products["lineUnitPurchasingPrice"] = pd.to_numeric(products.get("lineUnitPurchasingPrice"), errors="coerce").fillna(0)
    products["estimated_margin_ht"] = products["LineGrossAmount"] - (products["lineUnitPurchasingPrice"] * products["Quantity"])

    margins = products.groupby(["month","OrganisationId"], dropna=False)["estimated_margin_ht"].sum().reset_index(name="Estimated_Net_Margin_HT")

    ca_ttc = tenders[tenders["Label"] != "FAKE"].groupby(["month","OrganisationId"], dropna=False)["TotalAmount"].sum().reset_index(name="CA_Net_TTC")
    ca_vouchers = tenders[tenders["Label"] == "COUPON"].groupby(["month","OrganisationId"], dropna=False)["TotalAmount"].sum().reset_index(name="CA_Paid_With_Coupons")

    coupons["UseDate"] = pd.to_datetime(coupons["UseDate"], errors="coerce")
    coupons["month"] = coupons["UseDate"].dt.to_period("M").astype(str)
    coupons["used_amount"] = coupons["InitialValue"].fillna(0) - coupons["Amount"].fillna(0)
    coupons_used = coupons[coupons["UseDate"].notna()]
    coupons_monthly = coupons_used.groupby(["month","OrganisationId"], dropna=False)["used_amount"].sum().reset_index(name="Value_Used")

    df = (
        ca_ttc
        .merge(ca_vouchers, on=["month","OrganisationId"], how="outer")
        .merge(coupons_monthly, on=["month","OrganisationId"], how="outer")
        .merge(margins, on=["month","OrganisationId"], how="outer")
        .fillna(0)
    )

    df["Voucher_Share"] = (df["CA_Paid_With_Coupons"] / df["CA_Net_TTC"]).replace([np.inf, -np.inf], np.nan).fillna(0)
    df["Net_Margin_After_Loyalty"] = df["Estimated_Net_Margin_HT"] - df["Value_Used"]
    df["ROI_Proxy"] = np.where(df["Value_Used"] != 0, (df["CA_Paid_With_Coupons"] - df["Value_Used"]) / df["Value_Used"], np.nan)
    df["month"] = df["month"].astype(str)

    # Historisation locale
    if os.path.exists(HISTO_FILE):
        histo_df = pd.read_csv(HISTO_FILE)
        histo_df = pd.concat([histo_df, df], ignore_index=True)
    else:
        histo_df = df.copy()

    histo_df.to_csv(HISTO_FILE, index=False)
    st.success(f"✅ {len(df)} lignes ajoutées à l’historique fidélité.")

    if st.button("📤 Mettre à jour Google Sheets + envoyer lien Looker"):
        try:
            df_all = _gsheet_upsert_dataframe(SPREADSHEET_ID, SHEET_NAME, histo_df)

            recipients_default = [
                "alexandre.audinot@latribu.fr",
                "jm.lelann@latribu.fr",
                "philippe.risso@firea.com"
            ]
            all_recipients = [DEFAULT_RECEIVER] + recipients_default + [e.strip() for e in emails_supp.split(",") if e.strip() != ""]

            msg = EmailMessage()
            msg["Subject"] = f"📊 Rapport fidélité La Tribu - {datetime.today().strftime('%d-%m-%Y')}"
            msg["From"] = SMTP_USER
            msg["To"] = ", ".join(all_recipients)
            msg.set_content(
                f"Bonjour,\n\nVoici le lien vers le tableau de bord dynamique de suivi du programme fidélité :\n👉 {LOOKER_URL}\n"
            )

            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USER, SMTP_PASSWORD)
                server.send_message(msg)

            st.success("📈 Google Sheets mis à jour et lien Looker envoyé par e-mail !")

        except Exception as e:
            st.error("❌ Erreur pendant la mise à jour ou l’envoi de l’e-mail.")
            st.exception(e)

    st.subheader("🗂️ Historique Fidélité actuel")
    st.dataframe(histo_df, use_container_width=True)

else:
    st.info("Veuillez importer les deux fichiers Keyneo (transactions + bons d’achat) pour démarrer.")
