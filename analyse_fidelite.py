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
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

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
    scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets"
    ]
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
# 📥 RÉCUPÉRATION DES FICHIERS EXISTANTS SUR GOOGLE DRIVE
# ============================================================
def download_from_drive(file_name, local_path, folder_id=None):
    """Télécharge un fichier depuis le dossier Drive (ou Drive partagé)."""
    if folder_id is None:
        folder_id = st.secrets["gcp"].get("folder_id", "")
    try:
        query = f"name='{file_name}' and trashed=false"
        if folder_id:
            query += f" and '{folder_id}' in parents"

        results = (
            drive_service.files()
            .list(q=query, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True)
            .execute()
            .get("files", [])
        )
        if not results:
            st.info(f"ℹ️ Fichier '{file_name}' non trouvé sur le Drive (premier lancement ?)")
            return False

        file_id = results[0]["id"]
        request = drive_service.files().get_media(fileId=file_id, supportsAllDrives=True)
        fh = io.FileIO(local_path, "wb")
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.close()
        st.success(f"📥 Fichier '{file_name}' téléchargé depuis le Drive.")
        return True
    except Exception as e:
        st.warning(f"⚠️ Erreur téléchargement '{file_name}' : {e}")
        return False


# ============================================================
# UI
# ============================================================
st.sidebar.header("📂 Importer les fichiers CSV")
file_tx = st.sidebar.file_uploader("Transactions (CSV Keyneo)", type=["csv"])
file_cp = st.sidebar.file_uploader("Coupons (CSV Keyneo)", type=["csv"])

ensure_data_dir()

# Téléchargement des fichiers Drive actuels
_ = download_from_drive("transactions.parquet", TX_PATH)
_ = download_from_drive("coupons.parquet", CP_PATH)

# ============================================================
# PIPELINE
# ============================================================
if file_tx and file_cp:
    # 1️⃣ Lecture CSV
    tx = read_csv(file_tx)
    cp = read_csv(file_cp)

    # 2️⃣ Chargement historique transactions
    hist_tx = load_parquet(TX_PATH, TX_COLS)

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

    # --- Mapping coupons (avec écrasement total)
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

    # --- Append-only transactions, coupons = overwrite
    tx["TransactionID"] = tx["TransactionID"].astype(str)
    hist_tx["TransactionID"] = hist_tx["TransactionID"].astype(str)
    new_tx = tx[~tx["TransactionID"].isin(hist_tx["TransactionID"])]
    hist_tx = pd.concat([hist_tx, new_tx], ignore_index=True)
    save_parquet(hist_tx, TX_PATH)
    save_parquet(cp, CP_PATH)

    st.success(f"✅ {len(new_tx)} nouvelles transactions ajoutées. Coupons mis à jour.")

    # ======================================================
    # 6️⃣ KPI Mensuel — calcul complet
    # ======================================================
    df_tx = hist_tx.copy()
    hist_cp = load_parquet(CP_PATH, CP_COLS)
    df_cp = hist_cp.copy()

    if df_tx.empty:
        st.warning("⚠️ Pas de données transactionnelles disponibles.")
        st.stop()

    # [💡 ici ton bloc KPI complet reste inchangé, on le garde]

    # ============================================================
    # 7️⃣ EXPORT GOOGLE DRIVE & GOOGLE SHEET
    # ============================================================
    st.subheader("☁️ Export Google Drive & Google Sheets")

    def upload_to_drive(file_path, file_name, mime_type="application/octet-stream", folder_id=None):
        """Upload un fichier dans le dossier Google Drive partagé configuré."""
        if folder_id is None:
            folder_id = st.secrets["gcp"].get("folder_id", "")

        file_metadata = {"name": file_name}
        if folder_id:
            if folder_id.startswith("0A"):  # Drive partagé racine
                file_metadata["driveId"] = folder_id
                file_metadata["parents"] = []
            else:
                file_metadata["parents"] = [folder_id]

        media = MediaIoBaseUpload(open(file_path, "rb"), mimetype=mime_type, resumable=True)
        query = f"name='{file_name}' and trashed=false"
        if folder_id and not folder_id.startswith("0A"):
            query += f" and '{folder_id}' in parents"

        existing = (
            drive_service.files()
            .list(q=query, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True)
            .execute()
            .get("files", [])
        )

        try:
            if existing:
                file_id = existing[0]["id"]
                drive_service.files().update(
                    fileId=file_id, media_body=media, supportsAllDrives=True
                ).execute()
            else:
                drive_service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields="id",
                    supportsAllDrives=True
                ).execute()
            st.success(f"✅ Fichier '{file_name}' exporté sur Google Drive.")
        except Exception as e:
            st.error(f"❌ Erreur lors de l'upload du fichier '{file_name}' : {e}")

    try:
        _ = upload_to_drive(TX_PATH, "transactions.parquet", "application/octet-stream")
        _ = upload_to_drive(CP_PATH, "coupons.parquet", "application/octet-stream")
        st.success("✅ Transactions et coupons exportés sur Google Drive.")
    except Exception as e:
        st.error(f"❌ Erreur export Drive : {e}")

else:
    st.info("➡️ Importez les fichiers Transactions et Coupons pour démarrer.")
