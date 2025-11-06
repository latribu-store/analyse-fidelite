import streamlit as st
import pandas as pd
import numpy as np
import json, requests, re, smtplib
from datetime import datetime
from email.message import EmailMessage
import gspread
from google.oauth2 import service_account

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
st.set_page_config(page_title="Analyse Fidélité - KPI Simplifié", layout="wide")
st.title("🎯 Analyse Fidélité - KPI mensuels simplifiés (avec envoi mail)")

SPREADSHEET_ID = "1xYQ0mjr37Fnmal8yhi0kBE3_96EN6H6qsYrAtQkHpGo"
SHEET_KPI = "KPI_Mensuels"
LOOKER_URL = "https://lookerstudio.google.com/reporting/a0037205-4433-4f9e-b19b-1601fbf24006"

SMTP_SERVER = st.secrets["email"]["smtp_server"]
SMTP_PORT = st.secrets["email"]["smtp_port"]
SMTP_USER = st.secrets["email"]["smtp_user"]
SMTP_PASS = st.secrets["email"]["smtp_password"]
DEFAULT_RECEIVER = st.secrets["email"]["receiver"]

# Auth Google
file_id = "12O9eFGFmwTu1n6kF4AIDIm0KXKMIgOvg"
url = f"https://drive.google.com/uc?id={file_id}"
resp = requests.get(url); resp.raise_for_status()
gcp_service_account_info = json.loads(resp.content)
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = service_account.Credentials.from_service_account_info(gcp_service_account_info, scopes=scopes)
client = gspread.authorize(creds)

# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------
def _ensure_date(s): return pd.to_datetime(s, errors="coerce")
def _month_str(s): return _ensure_date(s).dt.to_period("M").astype(str)
def _norm_cols(df): return df.rename(columns={c: re.sub(r"[^a-z0-9]", "", c.lower()) for c in df.columns})
def _pick(df, *cands):
    for c in cands:
        k = re.sub(r"[^a-z0-9]", "", c.lower())
        if k in df.columns: return k
    return None
def _open_or_create(sheet_id, tab_name):
    sh = client.open_by_key(sheet_id)
    try: return sh.worksheet(tab_name)
    except Exception: return sh.add_worksheet(title=tab_name, rows=2, cols=120)
def _update_ws(ws, df):
    safe = df.copy().astype(object).where(pd.notnull(df), "")
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

# ------------------------------------------------------------
# UI
# ------------------------------------------------------------
st.sidebar.header("📂 Importer les fichiers")
file_tx = st.sidebar.file_uploader("Transactions (CSV Keyneo)", type=["csv"])
file_cp = st.sidebar.file_uploader("Coupons (CSV Keyneo)", type=["csv"])
emails_supp = st.sidebar.text_input("📧 Autres destinataires (séparés par des virgules)")

if file_tx and file_cp:
    # 1️⃣ Lecture
    tx = _norm_cols(_read_csv_tolerant(file_tx))
    cp = _norm_cols(_read_csv_tolerant(file_cp))

    c_txid  = _pick(tx, "ticketnumber", "transactionid", "operationid")
    c_total = _pick(tx, "totalamount", "totaltcc", "totalttc")
    c_label = _pick(tx, "label", "libelle")
    c_valid = _pick(tx, "validationdate", "operationdate", "date")
    c_org   = _pick(tx, "organisationid", "organizationid")
    c_cust  = _pick(tx, "customerid", "clientid")
    c_gross = _pick(tx, "linegrossamount", "montanthtligne", "cahtligne", "montantht")
    c_cost  = _pick(tx, "linetotalpurchasingamount", "purchasingamount", "achatht")
    c_qty   = _pick(tx, "quantity", "qty", "linequantity", "quantite")

    for c in [c_total, c_gross, c_cost, c_qty]:
        if c in tx.columns:
            tx[c] = pd.to_numeric(tx[c], errors="coerce")

    # 2️⃣ Fact table
    ca_ttc = tx.groupby(c_txid)[c_total].max().rename("CA_TTC").reset_index()
    ca_ht  = tx.groupby(c_txid)[c_gross].sum().rename("CA_HT").reset_index()
    cost   = tx.groupby(c_txid)[c_cost].sum().rename("Cost_HT").reset_index()
    qty    = tx.groupby(c_txid)[c_qty].sum().rename("Qty").reset_index() if c_qty else None

    has_coupon = (
        tx.assign(lbl=tx[c_label].fillna("").astype(str).str.upper())
          .groupby(c_txid)["lbl"].apply(lambda s: s.str.contains("COUPON").any())
          .reset_index(name="Has_Coupon")
    )

    ctx = tx[[c_txid, c_valid, c_org, c_cust]].drop_duplicates(subset=[c_txid])
    ctx = ctx.rename(columns={c_txid:"TransactionID", c_valid:"ValidationDate", c_org:"OrganisationID", c_cust:"CustomerID"})

    fact = ca_ttc.merge(ca_ht, on=c_txid).merge(cost, on=c_txid).merge(has_coupon, on=c_txid).merge(ctx, left_on=c_txid, right_on="TransactionID", how="left")
    if qty is not None: fact = fact.merge(qty, on=c_txid, how="left")

    fact["ValidationDate"] = _ensure_date(fact["ValidationDate"])
    fact["month"] = _month_str(fact["ValidationDate"])

    # 🔧 Nettoyage fort des ID clients
    fact["CustomerID"] = fact["CustomerID"].astype(str).str.strip().replace(["", "nan", "none", "NaN", "None"], np.nan)
    fact["is_client"] = fact["CustomerID"].notna()
    fact["CA_paid_with_coupons"] = np.where(fact["Has_Coupon"], fact["CA_HT"], 0)
    fact["Marge_HT"] = fact["CA_HT"] - fact["Cost_HT"]

    # 3️⃣ Coupons
    c_couponid = _pick(cp, "couponid", "id")
    c_init     = _pick(cp, "initialvalue")
    c_rem      = _pick(cp, "amount")
    c_usedate  = _pick(cp, "usedate")
    c_emiss    = _pick(cp, "creationdate")
    c_orgc     = _pick(cp, "organisationid")

    cp["UseDate"] = _ensure_date(cp[c_usedate])
    cp["EmissionDate"] = _ensure_date(cp[c_emiss])
    cp["Amount_Initial"] = pd.to_numeric(cp[c_init], errors="coerce").fillna(0)
    cp["Amount_Remaining"] = pd.to_numeric(cp[c_rem], errors="coerce").fillna(0)
    cp["Used"] = (cp["Amount_Initial"] - cp["Amount_Remaining"]).clip(lower=0)
    cp["month"] = _month_str(cp["UseDate"])
    cp["month_emit"] = _month_str(cp["EmissionDate"])
    cp = cp.rename(columns={c_couponid:"CouponID", c_orgc:"OrganisationID"})

    coupons_used = cp.groupby(["month","OrganisationID"]).agg(Coupons_utilisés=("CouponID","nunique"), Montant_coupons_utilisés=("Used","sum")).reset_index()
    coupons_emis = cp.groupby(["month_emit","OrganisationID"]).agg(Coupons_émis=("CouponID","nunique"), Montant_coupons_émis=("Amount_Initial","sum")).reset_index().rename(columns={"month_emit":"month"})

    # 4️⃣ KPI mensuels
    grp = ["month","OrganisationID"]
    base = fact.groupby(grp).agg(
        CA_TTC=("CA_TTC","sum"),
        CA_HT=("CA_HT","sum"),
        Marge_HT_avant=("Marge_HT","sum"),
        Qty_total=("Qty","sum"),
        Transactions=("TransactionID","nunique")
    ).reset_index()

    tx_client = fact[fact["is_client"]].groupby(grp)["TransactionID"].nunique().reset_index(name="Transactions_client")
    clients = fact[fact["is_client"]].groupby(grp)["CustomerID"].nunique().reset_index(name="Clients")

    kpi = base.merge(tx_client,on=grp,how="left").merge(clients,on=grp,how="left") \
              .merge(coupons_used,on=grp,how="left").merge(coupons_emis,on=grp,how="left")

    # Paniers moyens
    kpi["Panier_moyen"] = np.where(kpi["Transactions"]>0, kpi["CA_HT"]/kpi["Transactions"], np.nan)
    pm_client = fact[fact["is_client"]].groupby(grp).agg(CA_HT=("CA_HT","sum"), TX=("TransactionID","nunique")).reset_index()
    pm_client["Panier_client"] = pm_client["CA_HT"]/pm_client["TX"]
    pm_non = fact[~fact["is_client"]].groupby(grp).agg(CA_HT=("CA_HT","sum"), TX=("TransactionID","nunique")).reset_index()
    pm_non["Panier_non_client"] = pm_non["CA_HT"]/pm_non["TX"]
    pm_coupon = fact.groupby(grp+["Has_Coupon"]).agg(CA_HT=("CA_HT","sum"), TX=("TransactionID","nunique")).reset_index()
    pm_avec = pm_coupon[pm_coupon["Has_Coupon"]==True].assign(Panier_avec_coupon=lambda d: d["CA_HT"]/d["TX"])[grp+["Panier_avec_coupon"]]
    pm_sans = pm_coupon[pm_coupon["Has_Coupon"]==False].assign(Panier_sans_coupon=lambda d: d["CA_HT"]/d["TX"])[grp+["Panier_sans_coupon"]]
    kpi = kpi.merge(pm_client,on=grp,how="left").merge(pm_non,on=grp,how="left").merge(pm_avec,on=grp,how="left").merge(pm_sans,on=grp,how="left")

    # Marges et taux
    kpi["Marge_HT_après"] = kpi["Marge_HT_avant"] - kpi["Montant_coupons_utilisés"].fillna(0)
    kpi["Taux_marge_avant"] = kpi["Marge_HT_avant"]/kpi["CA_HT"]
    kpi["Taux_marge_après"] = kpi["Marge_HT_après"]/kpi["CA_HT"]
    kpi["ROI_proxy"] = np.where(kpi["Montant_coupons_utilisés"]>0,(kpi["CA_HT"]-kpi["Montant_coupons_utilisés"])/kpi["Montant_coupons_utilisés"],np.nan)

    kpi["Date"] = pd.to_datetime(kpi["month"], format="%Y-%m", errors="coerce").dt.strftime("%d/%m/%Y")

    # 5️⃣ Export Google Sheets
    ws = _open_or_create(SPREADSHEET_ID, SHEET_KPI)
    _update_ws(ws, kpi)
    st.success(f"✅ KPI_Mensuels mis à jour ({len(kpi)} lignes).")
    st.dataframe(kpi.head(10))

    # 6️⃣ Envoi e-mail Looker
    if st.button("📤 Envoyer le lien Looker par e-mail"):
        recipients = [DEFAULT_RECEIVER] + [e.strip() for e in emails_supp.split(",") if e.strip()]
        msg = EmailMessage()
        msg["Subject"] = f"📊 Rapport fidélité La Tribu - {datetime.today().strftime('%d/%m/%Y')}"
        msg["From"] = SMTP_USER
        msg["To"] = ", ".join(recipients)
        msg.set_content(f"""Bonjour 👋,

Le tableau de bord fidélité **La Tribu** a été mis à jour.

👉 Accédez au rapport ici :
{LOOKER_URL}

Bien à vous,
L’équipe La Tribu""")
        try:
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USER, SMTP_PASS)
                server.send_message(msg)
            st.success("📧 Mail Looker envoyé avec succès.")
        except Exception as e:
            st.error(f"❌ Erreur d’envoi mail : {e}")

else:
    st.info("➡️ Importez vos fichiers Transactions et Coupons pour démarrer.")
