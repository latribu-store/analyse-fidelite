import streamlit as st
import pandas as pd
import numpy as np
import duckdb, json, re, smtplib, requests
from datetime import datetime
from email.message import EmailMessage
import gspread
from google.oauth2 import service_account

# ======================================
# CONFIG GÉNÉRALE
# ======================================
st.set_page_config(page_title="Analyse Fidélité — La Tribu", layout="wide")
st.title("🎯 Analyse Fidélité — Historique DuckDB ➜ KPI mensuels (Google Sheets)")

# === Secrets
SPREADSHEET_ID = st.secrets["sheets"]["spreadsheet_id"]
SHEET_KPI = "KPI_Mensuels"

SMTP_SERVER = st.secrets["email"]["smtp_server"]
SMTP_PORT   = int(st.secrets["email"]["smtp_port"])
SMTP_USER   = st.secrets["email"]["smtp_user"]
SMTP_PASS   = st.secrets["email"]["smtp_password"]
DEFAULT_RECEIVER = st.secrets["email"]["receiver"]

LOOKER_URL = st.secrets["app"].get("looker_url", "")
GCP_JSON_DRIVE_FILE_ID = st.secrets["gcp"]["json_drive_file_id"]

DUCKDB_PATH = "historique.duckdb"

# ======================================
# HELPERS
# ======================================
def _ensure_date(s): return pd.to_datetime(s, errors="coerce")
def _month_str(s): return _ensure_date(s).dt.to_period("M").astype(str)
def _norm_cols(df): return df.rename(columns={c: re.sub(r"[^a-z0-9]", "", c.lower()) for c in df.columns})
def _pick(df, *cands):
    for c in cands:
        k = re.sub(r"[^a-z0-9]", "", c.lower())
        if k in df.columns: return k
    return None

# ======================================
# GOOGLE AUTH (via JSON file_id)
# ======================================
def get_gcp_creds():
    url = f"https://drive.google.com/uc?id={GCP_JSON_DRIVE_FILE_ID}"
    resp = requests.get(url)
    resp.raise_for_status()
    gcp_info = json.loads(resp.content)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_info(gcp_info, scopes=scopes)
    return creds

@st.cache_resource
def gs_client():
    return gspread.authorize(get_gcp_creds())

def ws_open_or_create(spreadsheet_id, tab_name):
    sh = gs_client().open_by_key(spreadsheet_id)
    try:
        return sh.worksheet(tab_name)
    except Exception:
        return sh.add_worksheet(title=tab_name, rows=2, cols=80)

def ws_overwrite_small(ws, df):
    safe = df.copy()
    for c in safe.columns:
        if np.issubdtype(safe[c].dtype, np.datetime64):
            safe[c] = pd.to_datetime(safe[c], errors="coerce").dt.strftime("%Y-%m-%d")
    safe = safe.where(pd.notnull(safe), "")
    ws.clear()
    ws.update("A1", [list(safe.columns)] + safe.values.tolist(), value_input_option="USER_ENTERED")

def _read_csv_tolerant(uploaded):
    df = pd.read_csv(uploaded, sep=";", encoding="utf-8-sig", on_bad_lines="skip", engine="python", dtype=str)
    for col in df.columns:
        df[col] = df[col].astype(str).str.replace("'", "", regex=False).str.strip()
    for col in df.columns:
        try_num = pd.to_numeric(df[col].str.replace(",", ".", regex=False), errors="coerce")
        if try_num.notna().mean() > 0.7:
            df[col] = try_num
    return df

# ======================================
# INITIALISATION DUCKDB
# ======================================
def init_duckdb():
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            TransactionID TEXT PRIMARY KEY,
            ValidationDate TIMESTAMP,
            month TEXT,
            OrganisationID TEXT,
            CustomerID TEXT,
            is_client BOOLEAN,
            CA_TTC DOUBLE,
            CA_HT DOUBLE,
            Purch_Total_HT DOUBLE,
            Qty_Ticket DOUBLE,
            Has_Coupon BOOLEAN,
            CA_paid_with_coupons_HT DOUBLE,
            Estimated_Net_Margin_HT DOUBLE
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS coupons (
            CouponID TEXT,
            OrganisationID TEXT,
            UseDate TIMESTAMP,
            EmissionDate TIMESTAMP,
            month_use TEXT,
            month_emit TEXT,
            Amount_Initial DOUBLE,
            Amount_Remaining DOUBLE,
            Value_Used_Line DOUBLE
        );
    """)
    return con

# ======================================
# UPSERT TRANSACTIONS / COUPONS
# ======================================
def upsert_transactions(con, fact_df):
    """Ajoute dans DuckDB uniquement les nouvelles transactions (tolérant et typé)."""
    if fact_df.empty:
        return

    df = fact_df.copy()
    expected_cols = {
        "TransactionID": str,
        "ValidationDate": "datetime64[ns]",
        "month": str,
        "OrganisationID": str,
        "CustomerID": str,
        "is_client": bool,
        "CA_TTC": float,
        "CA_HT": float,
        "Purch_Total_HT": float,
        "Qty_Ticket": float,
        "Has_Coupon": bool,
        "CA_paid_with_coupons_HT": float,
        "Estimated_Net_Margin_HT": float,
    }
    for col, typ in expected_cols.items():
        if col not in df.columns:
            df[col] = np.nan

    for col, typ in expected_cols.items():
        if typ == str:
            df[col] = df[col].astype(str)
        elif typ == bool:
            df[col] = df[col].astype(bool)
        elif typ == "datetime64[ns]":
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif typ == float:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    existing_ids = set(con.execute("SELECT TransactionID FROM transactions").fetchdf()["TransactionID"])
    new_fact = df[~df["TransactionID"].isin(existing_ids)]
    if new_fact.empty:
        return

    con.append("transactions", new_fact)

def append_coupons(con, cp_df):
    if cp_df.empty:
        return
    con.append("coupons", cp_df)

# ======================================
# CONSTRUCTION DES TABLES
# ======================================
def _build_fact_from_transactions(tx):
    c_txid  = _pick(tx, "ticketnumber", "transactionid", "operationid")
    c_total = _pick(tx, "totalamount", "totaltcc", "totalttc")
    c_label = _pick(tx, "label")
    c_valid = _pick(tx, "validationdate", "operationdate", "date")
    c_org   = _pick(tx, "organisationid", "organizationid")
    c_cust  = _pick(tx, "customerid", "clientid")
    c_gross = _pick(tx, "linegrossamount", "montanthtligne", "cahtligne", "montantht")
    c_cost  = _pick(tx, "linetotalpurchasingamount", "purchasingamount", "achatht")
    c_qty   = _pick(tx, "quantity", "qty", "linequantity", "quantite")

    for c in [c_total, c_gross, c_cost, c_qty]:
        if c in tx.columns: tx[c] = pd.to_numeric(tx[c], errors="coerce")

    ca_ttc = tx.groupby(c_txid, dropna=False)[c_total].max().rename("CA_TTC").reset_index()
    ca_ht  = tx.groupby(c_txid, dropna=False)[c_gross].sum().rename("CA_HT").reset_index()
    cost   = tx.groupby(c_txid, dropna=False)[c_cost].sum().rename("Purch_Total_HT").reset_index()

    has_coupon = (
        tx.assign(_lbl=tx[c_label].fillna("").astype(str).str.upper() if c_label else "")
        .groupby(c_txid)["_lbl"].apply(lambda s: s.str.contains("COUPON", regex=False).any())
        .reset_index(name="Has_Coupon")
    )

    ctx = tx[[c_txid, c_valid, c_org, c_cust]].drop_duplicates(subset=[c_txid]).rename(columns={
        c_txid:"TransactionID", c_valid:"ValidationDate", c_org:"OrganisationID", c_cust:"CustomerID"
    })

    fact = ca_ttc.merge(ca_ht, on=c_txid).merge(cost, on=c_txid).merge(has_coupon, on=c_txid).merge(ctx, left_on=c_txid, right_on="TransactionID", how="left")
    fact["ValidationDate"] = _ensure_date(fact["ValidationDate"])
    fact["month"] = _month_str(fact["ValidationDate"])
    fact["CustomerID"] = fact["CustomerID"].astype(str).str.strip().replace(["", "nan", "none", "NaN", "None"], np.nan)
    fact["is_client"] = fact["CustomerID"].notna()
    fact["Estimated_Net_Margin_HT"] = fact["CA_HT"] - fact["Purch_Total_HT"]
    fact["CA_paid_with_coupons_HT"] = np.where(fact["Has_Coupon"], fact["CA_HT"], 0)
    return fact

def _build_coupon_table(cp):
    c_couponid = _pick(cp, "couponid", "id")
    c_init     = _pick(cp, "initialvalue")
    c_rem      = _pick(cp, "amount", "remaining")
    c_usedate  = _pick(cp, "usedate")
    c_emiss    = _pick(cp, "creationdate")
    c_orgc     = _pick(cp, "organisationid")
    cp["UseDate"] = _ensure_date(cp[c_usedate])
    cp["EmissionDate"] = _ensure_date(cp[c_emiss])
    cp["Amount_Initial"] = pd.to_numeric(cp[c_init], errors="coerce").fillna(0)
    cp["Amount_Remaining"] = pd.to_numeric(cp[c_rem], errors="coerce").fillna(0)
    cp["Value_Used_Line"] = (cp["Amount_Initial"] - cp["Amount_Remaining"]).clip(lower=0)
    cp["month_use"]  = _month_str(cp["UseDate"])
    cp["month_emit"] = _month_str(cp["EmissionDate"])
    return cp.rename(columns={c_couponid:"CouponID", c_orgc:"OrganisationID"})

# ======================================
# MAIL
# ======================================
def send_mail(to_list, subject, body):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(to_list)
    msg.set_content(body)
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)

# ======================================
# STREAMLIT UI
# ======================================
st.sidebar.header("📂 Importer les fichiers")
file_tx = st.sidebar.file_uploader("Transactions (CSV Keyneo)", type=["csv"])
file_cp = st.sidebar.file_uploader("Coupons (CSV Keyneo)", type=["csv"])
emails_supp = st.sidebar.text_input("📧 Autres destinataires (séparés par des virgules)")

if file_tx and file_cp:
    tx_csv = _norm_cols(_read_csv_tolerant(file_tx))
    cp_csv = _norm_cols(_read_csv_tolerant(file_cp))

    fact = _build_fact_from_transactions(tx_csv)
    coupons = _build_coupon_table(cp_csv)

    con = init_duckdb()

    before_tx = con.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    upsert_transactions(con, fact)
    after_tx = con.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    new_tx = after_tx - before_tx

    before_cp = con.execute("SELECT COUNT(*) FROM coupons").fetchone()[0]
    append_coupons(con, coupons)
    after_cp = con.execute("SELECT COUNT(*) FROM coupons").fetchone()[0]
    new_cp = after_cp - before_cp

    st.success(f"✅ Base mise à jour : +{new_tx} transactions, +{new_cp} coupons")

    st.info(f"Base historique : {after_tx} transactions / {after_cp} coupons")
    
    # À ce stade, tu peux appeler compute_kpi(con) pour calculer et exporter tes KPI mensuels vers Sheets.
else:
    st.info("➡️ Importez vos CSV Transactions et Coupons pour démarrer.")
