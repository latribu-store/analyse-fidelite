import streamlit as st
import pandas as pd
import numpy as np
import json, requests, re, smtplib, time
from datetime import datetime
from email.message import EmailMessage
import gspread
from google.oauth2 import service_account
from io import BytesIO

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
st.set_page_config(page_title="Analyse Fidélité - La Tribu (KPI only)", layout="wide")
st.title("🎯 Analyse Fidélité - La Tribu — Version optimisée (KPI uniquement)")

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
resp = requests.get(url)
resp.raise_for_status()
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
        if k in df.columns:
            return k
    return None
def _open_or_create(sheet_id, tab_name):
    sh = client.open_by_key(sheet_id)
    try: return sh.worksheet(tab_name)
    except Exception: return sh.add_worksheet(title=tab_name, rows=2, cols=80)

def _sanitize_df_for_sheets(df):
    safe = df.copy()
    for col in safe.columns:
        if np.issubdtype(safe[col].dtype, np.datetime64):
            safe[col] = pd.to_datetime(safe[col], errors="coerce").dt.strftime("%Y-%m-%d")
    safe = safe.where(pd.notnull(safe), "")
    return safe

def _update_ws(ws, df):
    """Clear + update header + append (KPI only => peu de lignes)"""
    safe = _sanitize_df_for_sheets(df)
    ws.clear()
    ws.update("A1", [list(safe.columns)] + safe.values.tolist(), value_input_option="USER_ENTERED")

def _read_csv_tolerant(uploaded):
    df = pd.read_csv(uploaded, sep=";", encoding="utf-8-sig", on_bad_lines="skip", engine="python", dtype=str)
    for col in df.columns:
        df[col] = (
            df[col].astype(str)
            .str.replace("'", "", regex=False)
            .str.replace(",", ".", regex=False)
            .str.strip()
        )
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="ignore")
    return df

# ------------------------------------------------------------
# UI
# ------------------------------------------------------------
st.sidebar.header("📂 Importer les fichiers")
file_tx = st.sidebar.file_uploader("Transactions (CSV Keyneo)", type=["csv"])
file_cp = st.sidebar.file_uploader("Coupons (CSV Keyneo)", type=["csv"])
emails_supp = st.sidebar.text_input("📧 Autres destinataires (séparés par des virgules)")

if file_tx and file_cp:
    # =====================================================
    # 1️⃣ LECTURE & NORMALISATION
    # =====================================================
    tx_raw = _norm_cols(_read_csv_tolerant(file_tx))
    cp = _norm_cols(_read_csv_tolerant(file_cp))

    # colonnes clés
    c_txid = _pick(tx_raw, "ticketnumber", "transactionid", "operationid")
    c_total = _pick(tx_raw, "totalamount", "totaltcc", "totalttc")
    c_label = _pick(tx_raw, "label")
    c_valid = _pick(tx_raw, "validationdate", "operationdate")
    c_org = _pick(tx_raw, "organisationid", "organizationid")
    c_cust = _pick(tx_raw, "customerid", "clientid")
    c_gross = _pick(tx_raw, "linegrossamount", "montanthtligne", "cahtligne")
    c_cost = _pick(tx_raw, "linetotalpurchasingamount", "purchasingamount")
    c_qty = _pick(tx_raw, "quantity", "qty", "linequantity")

    for c in [c_total, c_gross, c_cost, c_qty]:
        if c in tx_raw.columns:
            tx_raw[c] = pd.to_numeric(tx_raw[c], errors="coerce")

    # =====================================================
    # 2️⃣ AGGREGATION DES DONNÉES
    # =====================================================
    ca_ttc = tx_raw.groupby(c_txid, dropna=False)[c_total].max().rename("CA_TTC").reset_index()
    ca_ht = tx_raw.groupby(c_txid, dropna=False)[c_gross].sum().rename("CA_HT").reset_index()
    cost = tx_raw.groupby(c_txid, dropna=False)[c_cost].sum().rename("Purch_Total_HT").reset_index()

    has_coupon = (
        tx_raw.assign(_lbl=tx_raw[c_label].fillna("").astype(str).str.upper())
        .groupby(c_txid)["_lbl"]
        .apply(lambda s: s.str.contains("COUPON", regex=False).any())
        .reset_index(name="Has_Coupon")
    )
    fact_tx = ca_ttc.merge(ca_ht, on=c_txid).merge(cost, on=c_txid).merge(has_coupon, on=c_txid, how="left")
    fact_tx["CA_Paid_With_Coupons"] = np.where(fact_tx["Has_Coupon"], fact_tx["CA_TTC"], 0.0)
    fact_tx["Estimated_Net_Margin_HT"] = fact_tx["CA_HT"] - fact_tx["Purch_Total_HT"]

    ctx = tx_raw[[c_txid, c_valid, c_org, c_cust]].drop_duplicates(subset=[c_txid])
    ctx = ctx.rename(columns={c_txid:"TransactionID", c_valid:"ValidationDate", c_org:"OrganisationId", c_cust:"CustomerID"})
    fact_tx = fact_tx.merge(ctx, left_on=c_txid, right_on="TransactionID", how="left")
    fact_tx["ValidationDate"] = _ensure_date(fact_tx["ValidationDate"])
    fact_tx["month"] = _month_str(fact_tx["ValidationDate"])

    qty = tx_raw.groupby(c_txid)[c_qty].sum().rename("Qty_Ticket").reset_index()
    fact_tx = fact_tx.merge(qty, on=c_txid, how="left")

    # Sauvegarde CSV local au lieu de push Google Sheet
    fact_tx.to_csv("Donnees_Full.csv", index=False, encoding="utf-8-sig")
    tx_raw.to_csv("Transactions_Full.csv", index=False, encoding="utf-8-sig")

    # =====================================================
    # 3️⃣ COUPONS
    # =====================================================
    c_couponid = _pick(cp, "couponid", "id")
    c_init = _pick(cp, "initialvalue")
    c_rem = _pick(cp, "amount")
    c_usedate = _pick(cp, "usedate")
    c_emiss = _pick(cp, "creationdate")
    c_orgc = _pick(cp, "organisationid")

    cp["UseDate"] = _ensure_date(cp[c_usedate])
    cp["EmissionDate"] = _ensure_date(cp[c_emiss])
    cp["Amount_Initial"] = pd.to_numeric(cp[c_init], errors="coerce").fillna(0)
    cp["Amount_Remaining"] = pd.to_numeric(cp[c_rem], errors="coerce").fillna(0)
    cp["Value_Used_Line"] = (cp["Amount_Initial"] - cp["Amount_Remaining"]).clip(lower=0)
    cp["IsUsed"] = cp["Value_Used_Line"] > 0
    cp["month"] = _month_str(cp["UseDate"])
    cp = cp.rename(columns={c_couponid:"CouponID", c_orgc:"OrganisationId"})
    cp.to_csv("Coupons_Full.csv", index=False, encoding="utf-8-sig")

    # =====================================================
    # 4️⃣ KPI MENSUELS COMPLETS
    # =====================================================
    tx_clients = fact_tx.dropna(subset=["CustomerID"]).copy()
    tx_clients["CustomerID"] = tx_clients["CustomerID"].astype(str).str.strip().str.lower()

    churn = (
        tx_clients.groupby(["month","OrganisationId"])
        .agg(Transactions_Client=("TransactionID","nunique"), Client=("CustomerID","nunique"))
        .reset_index()
    )

    first_seen = tx_clients.groupby("CustomerID")["ValidationDate"].min().reset_index(name="FirstDate")
    tx_clients = tx_clients.merge(first_seen, on="CustomerID", how="left")
    tx_clients["IsNewThisMonth"] = tx_clients["ValidationDate"].dt.to_period("M") == tx_clients["FirstDate"].dt.to_period("M")

    new_cust = (
        tx_clients[tx_clients["IsNewThisMonth"]]
        .groupby(["month","OrganisationId"])["CustomerID"]
        .nunique().reset_index(name="Nouveau_client")
    )
    churn = churn.merge(new_cust, on=["month","OrganisationId"], how="left").fillna(0)
    churn["Client_qui_reviennent"] = churn["Client"] - churn["Nouveau_client"]
    churn["Recurrence"] = np.where(churn["Client"]>0, churn["Transactions_Client"]/churn["Client"], np.nan)

    cust_sets = (
        tx_clients.groupby(["OrganisationId","month"])["CustomerID"]
        .apply(lambda s: set(s.unique())).reset_index().rename(columns={"CustomerID":"CustSet"})
    )
    cust_sets["_order"] = pd.PeriodIndex(cust_sets["month"], freq="M").to_timestamp()
    cust_sets = cust_sets.sort_values(["OrganisationId","_order"])
    cust_sets["Prev"] = cust_sets.groupby("OrganisationId")["CustSet"].shift(1)
    cust_sets["Retention_rate"] = cust_sets.apply(
        lambda r: len(r["Prev"].intersection(r["CustSet"])) / len(r["Prev"]) if isinstance(r["Prev"], set) and len(r["Prev"])>0 else np.nan,
        axis=1
    )
    retention = cust_sets[["month","OrganisationId","Retention_rate"]]

    tx_all = fact_tx.groupby(["month","OrganisationId"])["TransactionID"].nunique().reset_index(name="Transaction")
    assoc = churn.merge(tx_all, on=["month","OrganisationId"], how="left")
    assoc["Taux_association_client"] = np.where(
        assoc["Transaction"]>0, assoc["Transactions_Client"]/assoc["Transaction"], np.nan
    )

    kpi_tx = fact_tx.groupby(["month","OrganisationId"]).agg(
        CA_TTC=("CA_TTC","sum"),
        CA_HT=("CA_HT","sum"),
        CA_paid_with_coupons=("CA_Paid_With_Coupons","sum"),
        Marge_net_HT_avant_coupon=("Estimated_Net_Margin_HT","sum"),
        Qty_total=("Qty_Ticket","sum"),
    ).reset_index()

    qte_moy = fact_tx.groupby(["month","OrganisationId"])["Qty_Ticket"].mean().reset_index(name="Quantité_moy_article_par_transaction")

    coupons_used = cp.dropna(subset=["UseDate"]).groupby(["month","OrganisationId"]).agg(
        Coupon_utilisé=("CouponID","nunique"),
        Montant_coupons_utilisé=("Value_Used_Line","sum")
    ).reset_index()
    coupons_emis = cp.assign(month=_month_str(cp["EmissionDate"])).groupby(["month","OrganisationId"]).agg(
        Coupon_émis=("CouponID","nunique"),
        Montant_coupons_émis=("Amount_Initial","sum")
    ).reset_index()

    kpi = (kpi_tx.merge(churn,on=["month","OrganisationId"],how="left")
           .merge(retention,on=["month","OrganisationId"],how="left")
           .merge(assoc,on=["month","OrganisationId"],how="left")
           .merge(coupons_used,on=["month","OrganisationId"],how="left")
           .merge(coupons_emis,on=["month","OrganisationId"],how="left")
           .merge(qte_moy,on=["month","OrganisationId"],how="left")
           )

    # Ajout des colonnes KPI complètes demandées
    kpi["Marge_net_HT_apres_coupon"] = kpi["Marge_net_HT_avant_coupon"] - kpi["Montant_coupons_utilisé"].fillna(0)
    kpi["Taux_de_marge_HT_avant_coupon"] = np.where(kpi["CA_HT"]>0,kpi["Marge_net_HT_avant_coupon"]/kpi["CA_HT"],np.nan)
    kpi["Taux_de_marge_HT_apres_coupons"] = np.where(kpi["CA_HT"]>0,kpi["Marge_net_HT_apres_coupon"]/kpi["CA_HT"],np.nan)
    kpi["ROI_Proxy"] = np.where(kpi["Montant_coupons_utilisé"]>0,(kpi["CA_paid_with_coupons"]-kpi["Montant_coupons_utilisé"])/kpi["Montant_coupons_utilisé"],np.nan)
    kpi["Panier_moyen_HT"] = np.where(kpi["Transaction"]>0,kpi["CA_HT"]/kpi["Transaction"],np.nan)
    kpi["Taux_utilisation_bons_montant"] = np.where(kpi["Montant_coupons_émis"]>0,kpi["Montant_coupons_utilisé"]/kpi["Montant_coupons_émis"],np.nan)
    kpi["Taux_utilisation_bons_quantité"] = np.where(kpi["Coupon_émis"]>0,kpi["Coupon_utilisé"]/kpi["Coupon_émis"],np.nan)
    kpi["Taux_CA_généré_par_bons_sur_CA_HT"] = np.where(kpi["CA_HT"]>0,kpi["CA_paid_with_coupons"]/kpi["CA_HT"],np.nan)
    kpi["Prix_moyen_article_vendu_HT"] = np.where(kpi["Qty_total"]>0,kpi["CA_HT"]/kpi["Qty_total"],np.nan)
    kpi["Voucher_share"] = np.where(kpi["CA_HT"]>0,kpi["CA_paid_with_coupons"]/kpi["CA_HT"],np.nan)
    kpi["Date"] = pd.to_datetime(kpi["month"],format="%Y-%m").dt.strftime("%d/%m/%Y")

    # =====================================================
    # 5️⃣ EXPORT FINAL : KPI -> GOOGLE SHEETS
    # =====================================================
    ws_kpi = _open_or_create(SPREADSHEET_ID, SHEET_KPI)
    _update_ws(ws_kpi, kpi)
    st.success(f"✅ KPI_Mensuels mis à jour ({len(kpi)} lignes). Données brutes exportées en CSV local.")

    with st.expander("👀 Aperçu KPI"):
        st.dataframe(kpi.head(10))

    # =====================================================
    # 6️⃣ EMAIL LOOKER
    # =====================================================
    if st.button("📤 Envoyer le lien Looker par e-mail"):
        recipients = [DEFAULT_RECEIVER] + [e.strip() for e in emails_supp.split(",") if e.strip()]
        msg = EmailMessage()
        msg["Subject"] = f"📊 Rapport fidélité La Tribu - {datetime.today().strftime('%d/%m/%Y')}"
        msg["From"] = SMTP_USER
        msg["To"] = ", ".join(recipients)
        msg.set_content(f"Bonjour,\n\nLe tableau de bord fidélité La Tribu a été mis à jour.\n\n👉 Accédez-y ici : {LOOKER_URL}\n\nBien à vous,\nL’équipe La Tribu")
        try:
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USER, SMTP_PASS)
                server.send_message(msg)
            st.success("📧 Mail Looker envoyé avec succès.")
        except Exception as e:
            st.error(f"❌ Erreur d’envoi mail : {e}")

else:
    st.info("➡️ Importez les fichiers Transactions et Coupons pour démarrer.")
