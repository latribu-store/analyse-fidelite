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
st.title("🎯 Analyse Fidélité - AutoMapping Keyneo ➜ KPI mensuels (Drive + Sheet)")

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

    for k, v in map_tx.items():
        tx[k] = tx[v] if v in tx.columns else ""

    tx["ValidationDate"] = _ensure_date(tx["ValidationDate"])
    for col in ["CA_TTC","CA_HT","Purch_Total_HT","Qty_Ticket"]:
        tx[col] = pd.to_numeric(tx[col], errors="coerce").fillna(0.0)

    tx["Estimated_Net_Margin_HT"] = tx["CA_HT"] - tx["Purch_Total_HT"]
    tx["month"] = _month_str(tx["ValidationDate"])

    # --- Mapping coupons
    map_cp = {
        "CouponID": pick(cp, "couponid", "id"),
        "OrganisationID": pick(cp, "organisationid", "organizationid"),
        "EmissionDate": pick(cp, "creationdate", "issuedate"),
        "UseDate": pick(cp, "usedate", "validationdate"),
        "Amount_Initial": pick(cp, "initialvalue", "value", "montantinitial"),
        "Amount_Remaining": pick(cp, "amount", "reste", "remaining"),
    }

    for k, v in map_cp.items():
        cp[k] = cp[v] if v and v in cp.columns else ""

    for col in ["Amount_Initial", "Amount_Remaining"]:
        cp[col] = cp[col].astype(str).str.replace(",", ".", regex=False)
        cp[col] = pd.to_numeric(cp[col], errors="coerce").fillna(0.0)

    cp["EmissionDate"] = _ensure_date(cp["EmissionDate"])
    cp["UseDate"] = _ensure_date(cp["UseDate"])
    cp["Value_Used_Line"] = (cp["Amount_Initial"] - cp["Amount_Remaining"]).clip(lower=0.0)
    cp["month_use"] = _month_str(cp["UseDate"])
    cp["month_emit"] = _month_str(cp["EmissionDate"])

    # --- Append transactions, overwrite coupons
    tx["TransactionID"] = tx["TransactionID"].astype(str)
    hist_tx["TransactionID"] = hist_tx["TransactionID"].astype(str)
    new_tx = tx[~tx["TransactionID"].isin(hist_tx["TransactionID"])]
    hist_tx = pd.concat([hist_tx, new_tx], ignore_index=True)

    save_parquet(hist_tx, TX_PATH)
    save_parquet(cp, CP_PATH)

    st.success(f"✅ {len(new_tx)} nouvelles transactions ajoutées. Coupons mis à jour (écrasement complet).")

    # ======================================================
    # 6️⃣ KPI Mensuel
    # ======================================================
    df_tx = hist_tx.copy()
    df_cp = cp.copy()

    if df_tx.empty:
        st.warning("⚠️ Pas de données transactionnelles disponibles.")
        st.stop()

    # --- Nettoyage
    df_tx["ValidationDate"] = _ensure_date(df_tx["ValidationDate"])
    df_tx["month"] = _month_str(df_tx["ValidationDate"])
    for col in ["CA_TTC","CA_HT","Purch_Total_HT","Qty_Ticket"]:
        df_tx[col] = pd.to_numeric(df_tx[col], errors="coerce").fillna(0.0)
    df_tx["Label"] = df_tx["Label"].fillna("").astype(str)
    df_tx["CustomerID"] = df_tx["CustomerID"].fillna("").astype(str)
    df_tx["OrganisationID"] = df_tx["OrganisationID"].fillna("").astype(str)
    df_tx["_is_coupon_line"] = df_tx["Label"].str.upper().eq("COUPON")

    # --- Tickets
    agg_ticket = df_tx.groupby("TransactionID", dropna=False).agg(
        CA_TTC_ticket=("CA_TTC", "max"),
        CA_HT_ticket=("CA_HT", "sum"),
        Cost_ticket=("Purch_Total_HT", "sum"),
        Qty_ticket=("Qty_Ticket", "sum"),
        Has_Coupon=("_is_coupon_line", "max"),
        ValidationDate=("ValidationDate", "max"),
        OrganisationID=("OrganisationID", "last"),
        CustomerID=("CustomerID", "last")
    ).reset_index()

    agg_ticket["month"] = _month_str(agg_ticket["ValidationDate"])
    agg_ticket["Marge_net_HT_ticket"] = agg_ticket["CA_HT_ticket"] - agg_ticket["Cost_ticket"]
    agg_ticket["CA_paid_with_coupons"] = np.where(agg_ticket["Has_Coupon"], agg_ticket["CA_TTC_ticket"], 0.0)

    # --- Base
    base = agg_ticket.groupby(["month","OrganisationID"], dropna=False).agg(
        CA_TTC=("CA_TTC_ticket","sum"),
        CA_HT=("CA_HT_ticket","sum"),
        Marge_net_HT_avant_coupon=("Marge_net_HT_ticket","sum"),
        Transactions=("TransactionID","nunique"),
        Qty_total=("Qty_ticket","sum"),
        CA_paid_with_coupons=("CA_paid_with_coupons","sum"),
        Tickets_avec_coupon=("Has_Coupon","sum")
    ).reset_index()

    # --- Coupons
    coupons_used = df_cp.dropna(subset=["UseDate"]).groupby(["month_use","OrganisationID"]).agg(
        Coupon_utilise=("CouponID","nunique"),
        Montant_coupons_utilise=("Value_Used_Line","sum")
    ).rename(columns={"month_use":"month"}).reset_index()

    coupons_emis = df_cp.dropna(subset=["EmissionDate"]).groupby(["month_emit","OrganisationID"]).agg(
        Coupon_emis=("CouponID","nunique"),
        Montant_coupons_emis=("Amount_Initial","sum")
    ).rename(columns={"month_emit":"month"}).reset_index()

    # --- Fusion finale
    kpi = base.merge(coupons_used, on=["month","OrganisationID"], how="left") \
              .merge(coupons_emis, on=["month","OrganisationID"], how="left")

    kpi["Marge_net_HT_apres_coupon"] = kpi["Marge_net_HT_avant_coupon"] - kpi["Montant_coupons_utilise"].fillna(0)
    kpi["Taux_de_marge_HT_avant_coupon"] = np.where(kpi["CA_HT"]>0, kpi["Marge_net_HT_avant_coupon"]/kpi["CA_HT"], np.nan)
    kpi["Taux_de_marge_HT_apres_coupons"] = np.where(kpi["CA_HT"]>0, kpi["Marge_net_HT_apres_coupon"]/kpi["CA_HT"], np.nan)

    st.subheader("📊 KPI mensuels (complet)")
    st.dataframe(kpi.head(50))

    # --- Export CSV
    csv = kpi.to_csv(index=False, sep=";").encode("utf-8-sig")
    st.download_button("💾 Télécharger le KPI mensuel (CSV)", csv, "KPI_mensuel.csv", "text/csv")

    # ============================================================
    # 7️⃣ EXPORT GOOGLE DRIVE & GOOGLE SHEET
    # ============================================================
    st.subheader("☁️ Export Google Drive & Google Sheets")

    def upload_to_drive(file_path, file_name, mime_type="application/octet-stream", folder_id=None):
        file_metadata = {"name": file_name}
        if folder_id:
            file_metadata["parents"] = [folder_id]
        media = MediaIoBaseUpload(open(file_path, "rb"), mimetype=mime_type, resumable=True)
        existing = (
            drive_service.files()
            .list(q=f"name='{file_name}' and trashed=false", fields="files(id)")
            .execute()
            .get("files", [])
        )
        if existing:
            file_id = existing[0]["id"]
            drive_service.files().update(fileId=file_id, media_body=media).execute()
            return file_id
        else:
            f = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
            return f.get("id")

    try:
        tx_id = upload_to_drive(TX_PATH, "transactions.parquet", "application/octet-stream")
        cp_id = upload_to_drive(CP_PATH, "coupons.parquet", "application/octet-stream")
        st.success("✅ Transactions et coupons exportés sur Google Drive.")
    except Exception as e:
        st.error(f"❌ Erreur export Drive : {e}")

    def update_sheet(spreadsheet_id, sheet_name, df):
        try:
            sh = gspread_client.open_by_key(spreadsheet_id)
            try:
                ws = sh.worksheet(sheet_name)
            except gspread.WorksheetNotFound:
                ws = sh.add_worksheet(title=sheet_name, rows="100", cols="20")

            ws.clear()
            ws.update("A1", [list(df.columns)] + df.astype(str).values.tolist())
            st.success(f"✅ Feuille '{sheet_name}' mise à jour avec {len(df)} lignes.")
        except Exception as e:
            st.error(f"❌ Erreur mise à jour Google Sheets : {e}")

    update_sheet(SPREADSHEET_ID, "KPI_Mensuels", kpi)

else:
    st.info("➡️ Importez les fichiers Transactions et Coupons pour démarrer.")
