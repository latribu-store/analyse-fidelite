import streamlit as st
import pandas as pd
import numpy as np
import os
import io
import json
import smtplib
from email.message import EmailMessage
import requests
import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# ============================================================
# CONFIG
# ============================================================
st.set_page_config(page_title="🎯 Analyse Fidélité - KPI Automatisé", layout="wide")
st.title("🎯 Analyse Fidélité - AutoMapping Keyneo ➜ KPI mensuels (Drive + Mail)")

DATA_DIR = "data"
TX_PATH = os.path.join(DATA_DIR, "transactions.parquet")
CP_PATH = os.path.join(DATA_DIR, "coupons.parquet")

SPREADSHEET_ID = st.secrets["sheets"]["spreadsheet_id"]
LOOKER_URL = st.secrets["app"]["looker_url"]
SMTP_SERVER = st.secrets["email"]["smtp_server"]
SMTP_PORT = st.secrets["email"]["smtp_port"]
SMTP_USER = st.secrets["email"]["smtp_user"]
SMTP_PASS = st.secrets["email"]["smtp_password"]
DEFAULT_RECEIVER = st.secrets["email"]["receiver"]
DRIVE_FILE_ID = st.secrets["gcp"]["json_drive_file_id"]

# ============================================================
# HELPERS
# ============================================================
def _ensure_date(s):
    return pd.to_datetime(s, errors="coerce")

def _month_str(s):
    return _ensure_date(s).dt.to_period("M").astype(str)

def read_csv(uploaded):
    df = pd.read_csv(uploaded, sep=";", encoding="utf-8-sig", on_bad_lines="skip", dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]
    return df

def ensure_data_dir():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

def load_parquet(path, columns):
    if os.path.exists(path):
        try:
            df = pd.read_parquet(path)
        except Exception:
            df = pd.DataFrame(columns=columns)
    else:
        df = pd.DataFrame(columns=columns)
    return df

def save_parquet(df, path):
    df.to_parquet(path, index=False)

def pick(df, *cands):
    for c in cands:
        c_clean = c.lower()
        if c_clean in df.columns:
            return c_clean
    return None

# ============================================================
# GOOGLE DRIVE AUTH
# ============================================================
url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID}"
resp = requests.get(url)
resp.raise_for_status()
gcp_info = json.loads(resp.content)
creds = service_account.Credentials.from_service_account_info(
    gcp_info,
    scopes=["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
)
gspread_client = gspread.authorize(creds)
drive_service = build("drive", "v3", credentials=creds)

# ============================================================
# SCHEMA
# ============================================================
TX_COLS = [
    "TransactionID","ValidationDate","OrganisationID","CustomerID",
    "ProductID","Label","CA_TTC","CA_HT","Purch_Total_HT","Qty_Ticket"
]

CP_COLS = [
    "CouponID","OrganisationID","EmissionDate","UseDate",
    "Amount_Initial","Amount_Remaining","Value_Used_Line"
]

# ============================================================
# UI
# ============================================================
st.sidebar.header("📂 Importer les fichiers")
file_tx = st.sidebar.file_uploader("Transactions (CSV Keyneo)", type=["csv"])
file_cp = st.sidebar.file_uploader("Coupons (CSV Keyneo)", type=["csv"])

ensure_data_dir()

# ============================================================
# PIPELINE PRINCIPAL
# ============================================================
if file_tx and file_cp:
    # 1️⃣ Lecture CSV
    tx = read_csv(file_tx)
    cp = read_csv(file_cp)

    # 2️⃣ Chargement historique local
    hist_tx = load_parquet(TX_PATH, TX_COLS)
    hist_cp = load_parquet(CP_PATH, CP_COLS)

    # 3️⃣ Mapping transactions
    map_tx = {
        "TransactionID": pick(tx, "ticketnumber","transactionid","operationid"),
        "ValidationDate": pick(tx, "validationdate","operationdate"),
        "OrganisationID": pick(tx, "organisationid","organizationid"),
        "CustomerID": pick(tx, "customerid","clientid"),
        "ProductID": pick(tx, "productid","sku","ean"),
        "Label": pick(tx, "label","designation"),
        "CA_TTC": pick(tx, "totalamount","totalttc","totaltcc"),
        "CA_HT": pick(tx, "linegrossamount","montanthtligne","cahtligne"),
        "Purch_Total_HT": pick(tx, "linetotalpurchasingamount","purchasingamount","costprice"),
        "Qty_Ticket": pick(tx, "quantity","qty","linequantity")
    }
    for k,v in map_tx.items():
        tx[k] = tx[v] if v in tx.columns else ""

    tx["ValidationDate"] = _ensure_date(tx["ValidationDate"])
    for col in ["CA_TTC","CA_HT","Purch_Total_HT","Qty_Ticket"]:
        tx[col] = pd.to_numeric(tx[col], errors="coerce").fillna(0.0)
    tx["Estimated_Net_Margin_HT"] = tx["CA_HT"] - tx["Purch_Total_HT"]
    tx["month"] = _month_str(tx["ValidationDate"])

    # 4️⃣ Mapping coupons
    map_cp = {
        "CouponID": pick(cp, "couponid", "id"),
        "OrganisationID": pick(cp, "organisationid", "organizationid"),
        "EmissionDate": pick(cp, "creationdate", "issuedate"),
        "UseDate": pick(cp, "usedate", "validationdate"),
        "Amount_Initial": pick(cp, "initialvalue", "value", "montantinitial"),
        "Amount_Remaining": pick(cp, "amount", "reste", "remaining"),
    }
    for k,v in map_cp.items():
        cp[k] = cp[v] if v and v in cp.columns else ""
    for col in ["Amount_Initial", "Amount_Remaining"]:
        cp[col] = cp[col].astype(str).str.replace(",", ".", regex=False)
        cp[col] = pd.to_numeric(cp[col], errors="coerce").fillna(0.0)
    cp["EmissionDate"] = _ensure_date(cp["EmissionDate"])
    cp["UseDate"] = _ensure_date(cp["UseDate"])
    cp["Value_Used_Line"] = (cp["Amount_Initial"] - cp["Amount_Remaining"]).clip(lower=0.0)
    cp["month_use"] = _month_str(cp["UseDate"])
    cp["month_emit"] = _month_str(cp["EmissionDate"])

    # 5️⃣ Append transactions / merge coupons (MAJ + ajout)
    tx["TransactionID"] = tx["TransactionID"].astype(str)
    hist_tx["TransactionID"] = hist_tx["TransactionID"].astype(str)
    new_tx = tx[~tx["TransactionID"].isin(hist_tx["TransactionID"])]
    hist_tx = pd.concat([hist_tx, new_tx], ignore_index=True)

    # 🔁 Coupons : mise à jour + ajout
    cp["CouponID"] = cp["CouponID"].astype(str)
    hist_cp["CouponID"] = hist_cp["CouponID"].astype(str)
    hist_cp = hist_cp.drop_duplicates("CouponID", keep="last")
    merged_cp = pd.concat([hist_cp[~hist_cp["CouponID"].isin(cp["CouponID"])], cp], ignore_index=True)

    save_parquet(hist_tx, TX_PATH)
    save_parquet(merged_cp, CP_PATH)

    st.success(f"✅ {len(new_tx)} nouvelles transactions ajoutées et coupons mis à jour (merge).")

    # 6️⃣ ... ici ton bloc KPI complet (inchangé)
    # 👇👇👇
    # [on garde ton bloc KPI complet ici]
    # 👆👆👆

    # ============================================================
    # EXPORTS LOCAUX / DRIVE / SHEETS
    # ============================================================

    # Création du dossier de données s’il n’existe pas
    os.makedirs(DATA_DIR, exist_ok=True)

    # ✅ Le bon DataFrame des KPI mensuels s'appelle bien "df_kpi_mensuels" (ou merged_kpi selon ta version)
    try:
        kpi = df_kpi_mensuels.copy()
    except NameError:
        try:
            kpi = merged_kpi.copy()
        except NameError:
            st.error("❌ Aucune variable KPI trouvée — vérifie que la table KPI mensuelle a bien été générée.")
            st.stop()

    # Export CSV local
    csv_path = os.path.join(DATA_DIR, "KPI_Mensuel.csv")
    kpi.to_csv(csv_path, index=False, sep=";", encoding="utf-8-sig")
    st.success(f"✅ Export CSV local terminé : {csv_path}")

    # ============================================================
    # EXPORT GOOGLE DRIVE
    # ============================================================
    try:
        credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
        drive_service = build("drive", "v3", credentials=credentials)

        file_metadata = {"name": "KPI_Mensuel.csv", "mimeType": "text/csv"}
        media = MediaIoBaseUpload(io.FileIO(csv_path, "rb"), mimetype="text/csv", resumable=True)
        drive_file = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        st.success(f"📁 Fichier uploadé sur Google Drive : ID {drive_file.get('id')}")
    except Exception as e:
        st.warning(f"⚠️ Échec upload Drive : {e}")

    # ============================================================
    # EXPORT GOOGLE SHEETS
    # ============================================================
    try:
        credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
        client = gspread.authorize(credentials)
        sh = client.open_by_key(SPREADSHEET_ID)
        ws = sh.worksheet("KPI_Mensuel")
        ws.update("A1", [list(kpi.columns)] + kpi.values.tolist())
        st.success("📊 Feuille 'KPI_Mensuel' mise à jour avec succès !")
    except Exception as e:
        st.warning(f"⚠️ Échec update Sheets : {e}")

    # ============================================================
    # ENVOI MAIL
    # ============================================================
    try:
        msg = EmailMessage()
        msg["Subject"] = "📈 Rapport KPI Mensuel"
        msg["From"] = SMTP_USER
        msg["To"] = "ton.mail@domaine.com"
        msg.set_content("Bonjour,\n\nVoici le rapport KPI mensuel en pièce jointe.\n\nCordialement.")
        with open(csv_path, "rb") as f:
            msg.add_attachment(f.read(), maintype="text", subtype="csv", filename="KPI_Mensuel.csv")

        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as smtp:
            smtp.login(SMTP_USER, SMTP_PASS)
            smtp.send_message(msg)

        st.success("📧 Mail envoyé avec succès !")
    except Exception as e:
        st.warning(f"⚠️ Échec envoi mail : {e}")

else:
    st.info("➡️ Importez les fichiers Transactions et Coupons pour démarrer.")
