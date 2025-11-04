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

# Auth Google
file_id = "12O9eFGFmwTu1n6kF4AIDIm0KXKMIgOvg"
url = f"https://drive.google.com/uc?id={file_id}"
response = requests.get(url)
response.raise_for_status()
creds_info = json.loads(response.content)
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
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
    """Normalise les noms de colonnes (minuscules, suppression accents et caractères spéciaux)."""
    mapping = {}
    for c in df.columns:
        k = re.sub(r"[^a-z0-9]", "", c.lower())
        mapping[c] = k
    return df.rename(columns=mapping)

def _pick(df, *cands):
    """Retourne la première colonne dispo parmi les candidates."""
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
    return pd.DataFrame(data, columns=header) if data else pd.DataFrame(columns=header)

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
    tx = _norm_cols(_read_csv_tolerant(file_tx))
    cp = _norm_cols(_read_csv_tolerant(file_cp))

    st.write("🔍 Colonnes CSV importées :", tx.columns.tolist())

    # colonnes transactions
    c_valid = _pick(tx, "validationdate", "datevalidation", "operationdate")
    c_label = _pick(tx, "label", "paymentmethod", "tenderlabel")
    c_linetype = _pick(tx, "linetype")
    c_total = _pick(tx, "totalamount", "total_ttc")
    c_org = _pick(tx, "organisationid", "organizationid")
    c_cust = _pick(tx, "customerid", "clientid")
    c_txid = _pick(tx, "transactionid", "ticketnumber", "operationid")
    c_qty = _pick(tx, "quantity", "qty")
    c_gross = _pick(tx, "linegrossamount", "gross_ht")
    c_cost = _pick(tx, "lineunitpurchasingprice", "unitpurchaseprice")

    # colonnes coupons
    c_init = _pick(cp, "initialvalue", "initialamount")
    c_rem = _pick(cp, "amount", "remaining")
    c_usedate = _pick(cp, "usedate")
    c_emiss = _pick(cp, "creationdate", "emissiondate")
    c_orgc = _pick(cp, "organisationid", "organizationid")
    c_couponid = _pick(cp, "couponid", "id", "code")

    # conversions numériques
    for c in [c_qty, c_gross, c_cost, c_total]:
        if c and c in tx.columns:
            tx[c] = pd.to_numeric(tx[c], errors="coerce").fillna(0)
    for c in [c_init, c_rem]:
        if c and c in cp.columns:
            cp[c] = pd.to_numeric(cp[c], errors="coerce").fillna(0)

    # ------------------------------
    # 2️⃣ FACT TRANSACTIONS
    # ------------------------------
    tx["__is_tender"] = tx[c_linetype].str.upper().eq("TENDER") if c_linetype else False
    tx["__is_product"] = tx[c_linetype].str.upper().eq("PRODUCT_SALE") if c_linetype else False

    prod = tx[tx["__is_product"]].copy()
    if c_cost and c_gross and c_qty:
        prod["estimated_margin_ht_line"] = prod[c_gross] - (prod[c_cost] * prod[c_qty])
    else:
        prod["estimated_margin_ht_line"] = 0

    key_cols = [x for x in [c_txid, c_valid, c_org, c_cust] if x]
    if not key_cols:
        st.error("Impossible d’identifier un identifiant de transaction (TransactionID / TicketNumber / etc.).")
        st.stop()

    margin_tx = prod.groupby(key_cols, dropna=False)["estimated_margin_ht_line"] \
                    .sum().reset_index().rename(columns={"estimated_margin_ht_line": "Estimated_Net_Margin_HT"})

    tend = tx[tx["__is_tender"]].copy()
    tend["is_coupon"] = tend[c_label].str.upper().eq("COUPON") if c_label else False

    totals = tend.groupby(key_cols, dropna=False)[c_total] \
                 .sum().reset_index().rename(columns={c_total: "CA_Net_TTC"})
    coupons_paid = tend[tend["is_coupon"]].groupby(key_cols, dropna=False)[c_total] \
                    .sum().reset_index().rename(columns={c_total: "CA_Paid_With_Coupons"})

    fact_tx = totals.merge(coupons_paid, on=key_cols, how="left") \
                    .merge(margin_tx, on=key_cols, how="left")
    fact_tx["CA_Paid_With_Coupons"] = fact_tx["CA_Paid_With_Coupons"].fillna(0)
    fact_tx["Estimated_Net_Margin_HT"] = fact_tx["Estimated_Net_Margin_HT"].fillna(0)

    # Ajout colonnes principales
    fact_tx["ValidationDate"] = _ensure_date(tx[c_valid]) if c_valid in tx.columns else pd.NaT
    fact_tx["month"] = _month_str(fact_tx["ValidationDate"])
    fact_tx["OrganisationId"] = tx[c_org] if c_org in tx.columns else ""
    fact_tx["CustomerID"] = tx[c_cust] if c_cust in tx.columns else ""
    fact_tx["TransactionID"] = tx[c_txid] if c_txid in tx.columns else ""

    # ------------------------------
    # 3️⃣ COUPONS
    # ------------------------------
    cp["UseDate"] = _ensure_date(cp[c_usedate]) if c_usedate in cp.columns else pd.NaT
    cp["EmissionDate"] = _ensure_date(cp[c_emiss]) if c_emiss in cp.columns else pd.NaT
    cp["Amount_Initial"] = cp[c_init] if c_init else 0
    cp["Amount_Remaining"] = cp[c_rem] if c_rem else 0
    cp["Value_Used_Line"] = (cp["Amount_Initial"] - cp["Amount_Remaining"]).clip(lower=0)
    cp["IsUsed"] = cp["Value_Used_Line"] > 0
    cp["Days_To_Use"] = (cp["UseDate"] - cp["EmissionDate"]).dt.days
    cp["month"] = _month_str(cp["UseDate"])
    cp["OrganisationId"] = cp[c_orgc] if c_orgc else ""
    cp["CouponID"] = cp[c_couponid] if c_couponid else ""

    coupons_month = cp.dropna(subset=["UseDate"]).groupby(["month", "OrganisationId"], dropna=False)["Value_Used_Line"] \
                       .sum().reset_index().rename(columns={"Value_Used_Line": "Value_Used"})

    # ------------------------------
    # 4️⃣ KPI MENSUELS
    # ------------------------------
    kpi_tx = fact_tx.groupby(["month", "OrganisationId"], dropna=False).agg(
        CA_Net_TTC=("CA_Net_TTC", "sum"),
        CA_Paid_With_Coupons=("CA_Paid_With_Coupons", "sum"),
        Estimated_Net_Margin_HT=("Estimated_Net_Margin_HT", "sum"),
    ).reset_index()

    kpi = kpi_tx.merge(coupons_month, on=["month", "OrganisationId"], how="left").fillna(0)
    kpi["Voucher_Share"] = np.where(kpi["CA_Net_TTC"] > 0, kpi["CA_Paid_With_Coupons"] / kpi["CA_Net_TTC"], np.nan)
    kpi["Net_Margin_After_Loyalty"] = kpi["Estimated_Net_Margin_HT"] - kpi["Value_Used"]
    kpi["ROI_Proxy"] = np.where(kpi["Value_Used"] > 0, (kpi["CA_Paid_With_Coupons"] - kpi["Value_Used"]) / kpi["Value_Used"], np.nan)

    # ------------------------------
    # 5️⃣ EXPORT GOOGLE SHEETS
    # ------------------------------
    ws_tx = _open_or_create(SPREADSHEET_ID, SHEET_TX)
    ws_kpi = _open_or_create(SPREADSHEET_ID, SHEET_KPI)
    ws_cp = _open_or_create(SPREADSHEET_ID, SHEET_COUP)

    current_tx = _ws_to_df(ws_tx)
    fact_tx_out = fact_tx[["month", "OrganisationId", "TransactionID", "ValidationDate", "CustomerID",
                           "CA_Net_TTC", "CA_Paid_With_Coupons", "Estimated_Net_Margin_HT"]].copy()
    fact_tx_out["ValidationDate"] = pd.to_datetime(fact_tx_out["ValidationDate"]).dt.strftime("%Y-%m-%d")

    if current_tx.empty:
        merged_tx = fact_tx_out.copy()
    else:
        current_tx = current_tx[fact_tx_out.columns.intersection(current_tx.columns)]
        merged_tx = pd.concat([current_tx, fact_tx_out], ignore_index=True)
        merged_tx = merged_tx.sort_values("ValidationDate").drop_duplicates(subset=["TransactionID"], keep="last")

    _update_ws(ws_tx, merged_tx)
    _update_ws(ws_kpi, kpi)
    _update_ws(ws_cp, cp)

    st.success("✅ Données envoyées sur Google Sheets (Donnees, KPI_Mensuels, Coupons).")

    # ------------------------------
    # 6️⃣ EMAIL
    # ------------------------------
    if st.button("📤 Envoyer le lien Looker par e-mail"):
        recipients_default = ["charles.risso@latribu.fr"]
        all_recipients = [DEFAULT_RECEIVER] + recipients_default + [e.strip() for e in emails_supp.split(",") if e.strip()]
        msg = EmailMessage()
        msg["Subject"] = f"📊 Rapport fidélité La Tribu - {datetime.today().strftime('%d-%m-%Y')}"
        msg["From"] = SMTP_USER
        msg["To"] = ", ".join(all_recipients)
        msg.set_content(f"Bonjour,\n\nVoici le lien vers le tableau de bord fidélité :\n👉 {LOOKER_URL}\n")

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
    st.info("Veuillez importer le CSV **transactions** et le CSV **coupons** pour démarrer.")
