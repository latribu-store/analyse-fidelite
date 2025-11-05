import streamlit as st
import pandas as pd
import numpy as np
import os, json, requests
from datetime import datetime
import gspread
from google.oauth2 import service_account
import smtplib
from email.message import EmailMessage
import re

st.set_page_config(page_title="Analyse Fidélité - La Tribu (v2)", layout="wide")
st.title("🎯 Analyse Fidélité - La Tribu (v2)")

# ==========================
# CONFIG
# ==========================
HISTO_FILE = "historique_fidelite.csv"
SPREADSHEET_ID = "1xYQ0mjr37Fnmal8yhi0kBE3_96EN6H6qsYrAtQkHpGo"
SHEET_TX = "Donnees"
SHEET_KPI = "KPI_Mensuels"
SHEET_COUP = "Coupons"
LOOKER_URL = "https://lookerstudio.google.com/reporting/a0037205-4433-4f9e-b19b-1601fbf24006"

SMTP_SERVER = st.secrets["email"]["smtp_server"]
SMTP_PORT = st.secrets["email"]["smtp_port"]
SMTP_USER = st.secrets["email"]["smtp_user"]
SMTP_PASSWORD = st.secrets["email"]["smtp_password"]
DEFAULT_RECEIVER = st.secrets["email"]["receiver"]

# Auth Google via Service Account stocké sur Drive
file_id = "12O9eFGFmwTu1n6kF4AIDIm0KXKMIgOvg"
url = f"https://drive.google.com/uc?id={file_id}"
response = requests.get(url)
response.raise_for_status()
gcp_service_account_info = json.loads(response.content)
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = service_account.Credentials.from_service_account_info(gcp_service_account_info, scopes=scopes)
client = gspread.authorize(creds)


# ==========================
# HELPERS
# ==========================
def _ensure_date(s):
    return pd.to_datetime(s, errors="coerce")

def _month_str(s):
    s = _ensure_date(s)
    return s.dt.to_period("M").astype(str)

def _norm_cols(df):
    mapping = {}
    for c in df.columns:
        k = re.sub(r"[^a-z0-9]", "", c.lower())
        mapping[c] = k
    df = df.rename(columns=mapping)
    return df

def _pick(df, *cands):
    for c in cands:
        k = re.sub(r"[^a-z0-9]", "", c.lower())
        if k in df.columns:
            return k
    return None

def _read_csv_tolerant(uploaded):
    return pd.read_csv(uploaded, sep=";", encoding="utf-8-sig", on_bad_lines="skip", engine="python")

def _open_or_create(sheet_id: str, tab_name: str):
    sh = client.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(tab_name)
    except Exception:
        ws = sh.add_worksheet(title=tab_name, rows=2, cols=20)
    return ws

def _ws_to_df(ws):
    rows = ws.get_all_values()
    if not rows:
        return pd.DataFrame()
    header = rows[0]
    data = rows[1:]
    if not data:
        return pd.DataFrame(columns=header)
    return pd.DataFrame(data, columns=header)

def _update_ws(ws, df):
    values = [list(df.columns)] + df.astype(object).where(pd.notnull(df), "").values.tolist()
    ws.clear()
    ws.update("A1", values)


# ==========================
# UI
# ==========================
st.sidebar.header("📂 Importer les fichiers")
file_tx = st.sidebar.file_uploader("Fichier Transactions (CSV Keyneo)", type=["csv"])
file_cp = st.sidebar.file_uploader("Fichier Bons d’achat (CSV Keyneo)", type=["csv"])
emails_supp = st.sidebar.text_input("📧 Autres destinataires (séparés par des virgules)")

if file_tx and file_cp:
    # ------------------------------
    # 1️⃣ LECTURE + NORMALISATION
    # ------------------------------
    tx_raw = _read_csv_tolerant(file_tx)
    cp_raw = _read_csv_tolerant(file_cp)

    tx = _norm_cols(tx_raw)
    cp = _norm_cols(cp_raw)

    # ------------------------------
    # 2️⃣ FACT TRANSACTIONS (RÈGLES OFFICIELLES)
    # ------------------------------
    c_txid = _pick(tx, "ticketnumber", "transactionid", "operationid")
    c_total = _pick(tx, "totalamount", "total_ttc")
    c_label = _pick(tx, "label", "paymentmethod", "tenderlabel")
    c_valid = _pick(tx, "validationdate", "datevalidation", "operationdate")
    c_org = _pick(tx, "organisationid", "organizationid")
    c_cust = _pick(tx, "customerid", "clientid")
    c_gross = _pick(tx, "linegrossamount", "gross_ht")
    c_costtot = _pick(tx, "linetotalpurchasingamount", "totalpurchasingamount")

    # Sécurise les colonnes numériques
    for col in [c_total, c_gross, c_costtot]:
        if col and col in tx.columns:
            tx[col] = pd.to_numeric(tx[col], errors="coerce")

    # 1️⃣ CA TTC : une seule occurrence par ticket
    ca_ttc = (
        tx.dropna(subset=[c_txid])
          .groupby(c_txid, dropna=False)[c_total]
          .max()
          .rename("CA_Net_TTC")
          .reset_index()
    )

    # 2️⃣ CA via COUPONS : si une ligne du ticket a label == "COUPON"
    has_coupon = (
        tx.assign(_label=tx[c_label].fillna("").str.upper() if c_label in tx.columns else "")
          .groupby(c_txid, dropna=False)["_label"]
          .apply(lambda s: (s == "COUPON").any())
          .rename("Has_Coupon")
          .reset_index()
    )
    fact_tx = ca_ttc.merge(has_coupon, on=c_txid, how="left")
    fact_tx["CA_Paid_With_Coupons"] = np.where(fact_tx["Has_Coupon"], fact_tx["CA_Net_TTC"], 0.0)

    # 3️⃣ CA HT = somme de linegrossamount par ticket
    ca_ht = (
        tx.groupby(c_txid, dropna=False)[c_gross]
          .sum(min_count=1)
          .fillna(0.0)
          .rename("CA_Net_HT")
          .reset_index()
    )
    fact_tx = fact_tx.merge(ca_ht, on=c_txid, how="left")

    # 4️⃣ Coût marchandises = somme de linetotalpurchasingamount par ticket
    costs = (
        tx.groupby(c_txid, dropna=False)[c_costtot]
          .sum(min_count=1)
          .fillna(0.0)
          .rename("Purch_Total_HT")
          .reset_index()
    )
    fact_tx = fact_tx.merge(costs, on=c_txid, how="left")

    # 5️⃣ Marge HT = CA_HT - Coût
    fact_tx["Estimated_Net_Margin_HT"] = (fact_tx["CA_Net_HT"] - fact_tx["Purch_Total_HT"]).fillna(0.0)

    # Ajoute contexte
    ctx_cols = [c_txid, c_valid, c_org, c_cust]
    ctx = (
        tx.dropna(subset=[c_txid])[ctx_cols]
          .sort_values(by=[c_txid, c_valid] if c_valid in tx.columns else [c_txid])
          .drop_duplicates(subset=[c_txid], keep="first")
          .rename(columns={
              c_valid: "ValidationDate",
              c_org: "OrganisationId",
              c_cust: "CustomerID",
              c_txid: "TransactionID"
          })
    )
    fact_tx = fact_tx.merge(ctx, left_on=c_txid, right_on="TransactionID", how="left")

    fact_tx["ValidationDate"] = _ensure_date(fact_tx["ValidationDate"])
    fact_tx["month"] = _month_str(fact_tx["ValidationDate"])

    fact_tx["OrganisationId"] = fact_tx["OrganisationId"].fillna("")
    fact_tx["CustomerID"] = fact_tx["CustomerID"].fillna("")

    st.success("✅ Transactions agrégées par ticket selon les 5 règles officielles.")
    st.dataframe(fact_tx.head(20))

    # ------------------------------
    # 3️⃣ COUPONS (inchangé)
    # ------------------------------
    c_init = _pick(cp, "initialvalue", "initialamount")
    c_rem = _pick(cp, "amount", "remaining")
    c_usedate = _pick(cp, "usedate")
    c_emiss = _pick(cp, "creationdate", "emissiondate")
    c_orgc = _pick(cp, "organisationid", "organizationid")
    c_couponid = _pick(cp, "couponid", "code")

    if c_init and c_init in cp.columns: cp[c_init] = pd.to_numeric(cp[c_init], errors="coerce").fillna(0)
    if c_rem and c_rem in cp.columns: cp[c_rem] = pd.to_numeric(cp[c_rem], errors="coerce").fillna(0)

    cp["UseDate"] = _ensure_date(cp[c_usedate]) if c_usedate else pd.NaT
    cp["EmissionDate"] = _ensure_date(cp[c_emiss]) if c_emiss else pd.NaT
    cp["Amount_Initial"] = cp[c_init] if c_init else 0
    cp["Amount_Remaining"] = cp[c_rem] if c_rem else 0
    cp["Value_Used_Line"] = (cp["Amount_Initial"] - cp["Amount_Remaining"]).clip(lower=0)
    cp["IsUsed"] = cp["Value_Used_Line"] > 0
    cp["Days_To_Use"] = (cp["UseDate"] - cp["EmissionDate"]).dt.days
    cp["month"] = _month_str(cp["UseDate"])
    if c_orgc: cp["OrganisationId"] = cp[c_orgc]
    if c_couponid: cp["CouponID"] = cp[c_couponid]

    # ------------------------------
    # 4️⃣ EXPORT → Google Sheets
    # ------------------------------
    ws_tx = _open_or_create(SPREADSHEET_ID, SHEET_TX)
    ws_cp = _open_or_create(SPREADSHEET_ID, SHEET_COUP)

    # Transactions : on garde colonnes clés
    fact_tx_out = fact_tx[[
        "month", "OrganisationId", "TransactionID", "ValidationDate", "CustomerID",
        "CA_Net_TTC", "CA_Paid_With_Coupons", "CA_Net_HT", "Purch_Total_HT", "Estimated_Net_Margin_HT"
    ]].copy()
    fact_tx_out["ValidationDate"] = pd.to_datetime(fact_tx_out["ValidationDate"]).dt.strftime("%Y-%m-%d")
    _update_ws(ws_tx, fact_tx_out)

    cp_out = cp[[
        "CouponID","OrganisationId","EmissionDate","UseDate",
        "Amount_Initial","Amount_Remaining","Value_Used_Line","IsUsed","Days_To_Use","month"
    ]].copy()
    cp_out["EmissionDate"] = pd.to_datetime(cp_out["EmissionDate"]).dt.strftime("%Y-%m-%d")
    cp_out["UseDate"] = pd.to_datetime(cp_out["UseDate"]).dt.strftime("%Y-%m-%d")
    _update_ws(ws_cp, cp_out)

    st.success("✅ Données exportées vers Google Sheets (transactions + coupons).")

    # ------------------------------
    # 5️⃣ EMAIL
    # ------------------------------
    if st.button("📤 Envoyer le lien Looker par e-mail"):
        recipients_default = ["charles.risso@latribu.fr"]
        all_recipients = [DEFAULT_RECEIVER] + recipients_default + [e.strip() for e in emails_supp.split(",") if e.strip()]
        msg = EmailMessage()
        msg["Subject"] = f"📊 Rapport fidélité La Tribu - {datetime.today().strftime('%d-%m-%Y')}"
        msg["From"] = SMTP_USER
        msg["To"] = ", ".join(all_recipients)
        msg.set_content(
            f"Bonjour,\n\nVoici le lien vers le tableau de bord de suivi du programme fidélité :\n👉 {LOOKER_URL}\n"
        )
        try:
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USER, SMTP_PASSWORD)
                server.send_message(msg)
            st.success("📈 Lien Looker envoyé par e-mail (Ionos).")
        except Exception as e:
            st.error("❌ Erreur d’envoi e-mail.")
            st.exception(e)

else:
    st.info("Veuillez importer les fichiers CSV **transactions** et **coupons** pour démarrer.")
