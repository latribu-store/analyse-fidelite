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
st.set_page_config(page_title="Analyse Fidélité - KPI complet", layout="wide")
st.title("🎯 Analyse Fidélité — KPI mensuels (historique persistant)")

SPREADSHEET_ID = "1xYQ0mjr37Fnmal8yhi0kBE3_96EN6H6qsYrAtQkHpGo"
SHEET_DONNEES  = "Donnees"
SHEET_KPI      = "KPI_Mensuels"
LOOKER_URL     = "https://lookerstudio.google.com/reporting/a0037205-4433-4f9e-b19b-1601fbf24006"

SMTP_SERVER = st.secrets["email"]["smtp_server"]
SMTP_PORT   = st.secrets["email"]["smtp_port"]
SMTP_USER   = st.secrets["email"]["smtp_user"]
SMTP_PASS   = st.secrets["email"]["smtp_password"]
DEFAULT_RECEIVER = st.secrets["email"]["receiver"]

# === Auth Google ===
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
        if k in df.columns: return k
    return None

def _open_or_create(sheet_id, tab_name):
    sh = client.open_by_key(sheet_id)
    try:
        return sh.worksheet(tab_name)
    except Exception:
        return sh.add_worksheet(title=tab_name, rows=2, cols=160)

def _ws_to_df(ws):
    rows = ws.get_all_values()
    if not rows: return pd.DataFrame()
    header, data = rows[0], rows[1:]
    return pd.DataFrame(data, columns=header) if data else pd.DataFrame(columns=header)

def _update_ws(ws, df):
    safe = df.copy().astype(object).where(pd.notnull(df), "")
    ws.clear()
    ws.update("A1", [list(safe.columns)] + safe.values.tolist(), value_input_option="USER_ENTERED")

def _read_csv_tolerant(uploaded):
    df = pd.read_csv(uploaded, sep=";", encoding="utf-8-sig", on_bad_lines="skip", engine="python", dtype=str)
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip().str.replace("'", "", regex=False)
    for c in df.columns:
        num = pd.to_numeric(df[c].str.replace(",", ".", regex=False), errors="coerce")
        if num.notna().mean() > 0.7:
            df[c] = num
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
    # 1️⃣ Lecture & normalisation
    # =====================================================
    tx_raw = _norm_cols(_read_csv_tolerant(file_tx))
    cp     = _norm_cols(_read_csv_tolerant(file_cp))

    c_txid  = _pick(tx_raw, "ticketnumber", "transactionid", "operationid")
    c_total = _pick(tx_raw, "totalamount", "totaltcc", "totalttc")
    c_label = _pick(tx_raw, "label", "libelle", "designation")
    c_valid = _pick(tx_raw, "validationdate", "operationdate", "date")
    c_org   = _pick(tx_raw, "organisationid", "organizationid")
    c_cust  = _pick(tx_raw, "customerid", "clientid")
    c_gross = _pick(tx_raw, "linegrossamount", "montanthtligne", "cahtligne", "montantht")
    c_cost  = _pick(tx_raw, "linetotalpurchasingamount", "purchasingamount", "achatht")
    c_qty   = _pick(tx_raw, "quantity", "qty", "linequantity", "quantite")

    for c in [c_total, c_gross, c_cost, c_qty]:
        if c in tx_raw.columns:
            tx_raw[c] = pd.to_numeric(tx_raw[c], errors="coerce")

    # =====================================================
    # 2️⃣ Construction fact_tx
    # =====================================================
    ca_ttc = tx_raw.groupby(c_txid)[c_total].max().rename("CA_TTC").reset_index()
    ca_ht  = tx_raw.groupby(c_txid)[c_gross].sum().rename("CA_HT").reset_index()
    cost   = tx_raw.groupby(c_txid)[c_cost].sum().rename("Cost_HT").reset_index()
    qty    = tx_raw.groupby(c_txid)[c_qty].sum().rename("Qty").reset_index() if c_qty else None

    has_coupon = (
        tx_raw.assign(lbl=tx_raw[c_label].fillna("").astype(str).str.upper())
              .groupby(c_txid)["lbl"].apply(lambda s: s.str.contains("COUPON").any())
              .reset_index(name="Has_Coupon")
    )

    ctx = tx_raw[[c_txid, c_valid, c_org, c_cust]].drop_duplicates(subset=[c_txid])
    ctx = ctx.rename(columns={c_txid:"TransactionID", c_valid:"ValidationDate", c_org:"OrganisationID", c_cust:"CustomerID"})

    fact_tx = ca_ttc.merge(ca_ht, on=c_txid).merge(cost, on=c_txid).merge(has_coupon, on=c_txid).merge(ctx, left_on=c_txid, right_on="TransactionID", how="left")
    if qty is not None: fact_tx = fact_tx.merge(qty, on=c_txid, how="left")

    fact_tx["ValidationDate"] = _ensure_date(fact_tx["ValidationDate"])
    fact_tx["month"] = _month_str(fact_tx["ValidationDate"])
    fact_tx["CustomerID"] = fact_tx["CustomerID"].astype(str).str.strip().replace(["", "nan", "none", "NaN", "None"], np.nan)
    fact_tx["is_client"] = fact_tx["CustomerID"].notna()
    fact_tx["CA_paid_with_coupons_HT"] = np.where(fact_tx["Has_Coupon"], fact_tx["CA_HT"], 0.0)
    fact_tx["Marge_HT"] = fact_tx["CA_HT"] - fact_tx["Cost_HT"]

    # =====================================================
    # 3️⃣ Historisation dans "Donnees" (append-only)
    # =====================================================
    ws_donnees = _open_or_create(SPREADSHEET_ID, SHEET_DONNEES)
    existing = _ws_to_df(ws_donnees)
    if existing.empty:
        merged = fact_tx.copy()
    else:
        # Harmonisation colonnes
        for c in fact_tx.columns:
            if c not in existing.columns:
                existing[c] = ""
        for c in existing.columns:
            if c not in fact_tx.columns:
                fact_tx[c] = ""
        # Ajout uniquement des nouvelles transactions
        existing_ids = set(existing["TransactionID"].astype(str))
        new_tx = fact_tx[~fact_tx["TransactionID"].astype(str).isin(existing_ids)]
        merged = pd.concat([existing, new_tx], ignore_index=True) if not new_tx.empty else existing

    _update_ws(ws_donnees, merged)
    st.success(f"📥 Transactions mises à jour ({len(merged)} lignes cumulées).")

    # =====================================================
    # 4️⃣ Coupons agrégés
    # =====================================================
    cp["UseDate"] = _ensure_date(cp["usedate"]) if "usedate" in cp.columns else pd.NaT
    cp["EmissionDate"] = _ensure_date(cp["creationdate"]) if "creationdate" in cp.columns else pd.NaT
    cp["Amount_Initial"] = pd.to_numeric(cp["initialvalue"], errors="coerce").fillna(0) if "initialvalue" in cp.columns else 0
    cp["Amount_Remaining"] = pd.to_numeric(cp["amount"], errors="coerce").fillna(0) if "amount" in cp.columns else 0
    cp["Used"] = (cp["Amount_Initial"] - cp["Amount_Remaining"]).clip(lower=0)
    cp["month_use"] = _month_str(cp["UseDate"])
    cp["month_emit"] = _month_str(cp["EmissionDate"])
    cp = cp.rename(columns={"couponid":"CouponID", "organisationid":"OrganisationID"})

    coupons_used = cp.dropna(subset=["UseDate"]).groupby(["month_use","OrganisationID"]).agg(
        Coupons_utilises=("CouponID","nunique"),
        Montant_coupons_utilises=("Used","sum")
    ).reset_index().rename(columns={"month_use":"month"})

    coupons_emis = cp.dropna(subset=["EmissionDate"]).groupby(["month_emit","OrganisationID"]).agg(
        Coupons_emis=("CouponID","nunique"),
        Montant_coupons_emis=("Amount_Initial","sum")
    ).reset_index().rename(columns={"month_emit":"month"})

    # =====================================================
    # 5️⃣ KPI mensuels (à partir du merged)
    # =====================================================
    fact = merged.copy()
    grp = ["month","OrganisationID"]

    # Transactions
    base = fact.groupby(grp).agg(
        CA_TTC=("CA_TTC","sum"),
        CA_HT=("CA_HT","sum"),
        Transactions=("TransactionID","nunique"),
        Qty_total=("Qty","sum")
    ).reset_index()

    tx_client = fact[fact["is_client"]].groupby(grp)["TransactionID"].nunique().reset_index(name="Transaction_client")

    # Clients et nouveaux clients
    tx_clients = fact[fact["is_client"]].copy()
    first_seen = tx_clients.groupby("CustomerID")["ValidationDate"].min().reset_index(name="FirstDate")
    tx_clients = tx_clients.merge(first_seen, on="CustomerID", how="left")
    tx_clients["IsNew"] = tx_clients["ValidationDate"].dt.to_period("M") == tx_clients["FirstDate"].dt.to_period("M")

    clients = tx_clients.groupby(grp)["CustomerID"].nunique().reset_index(name="Client")
    newc = tx_clients[tx_clients["IsNew"]].groupby(grp)["CustomerID"].nunique().reset_index(name="Nouveau_client")
    churn = clients.merge(newc, on=grp, how="left").fillna(0)
    churn["Client_revenant"] = churn["Client"] - churn["Nouveau_client"]
    churn["Recurrence"] = np.where(churn["Client"]>0, fact[fact["is_client"]].groupby(grp)["TransactionID"].nunique().values / churn["Client"], np.nan)

    # Rétention
    cs = tx_clients.groupby(grp)["CustomerID"].apply(lambda s: set(s.unique())).reset_index(name="CustSet")
    cs["_order"] = pd.PeriodIndex(cs["month"], freq="M").to_timestamp()
    cs = cs.sort_values(["OrganisationID","_order"])
    cs["Prev"] = cs.groupby("OrganisationID")["CustSet"].shift(1)
    cs["Retention_rate"] = cs.apply(lambda r: len(r["Prev"].intersection(r["CustSet"])) / len(r["Prev"]) if isinstance(r["Prev"], set) and len(r["Prev"])>0 else np.nan, axis=1)
    retention = cs[grp+["Retention_rate"]]

    # Marges et paniers
    marge_avant = fact.groupby(grp).apply(lambda d: (d["CA_HT"] - d["Cost_HT"]).sum()).reset_index(name="Marge_avant")
    ca_coupon = fact.groupby(grp)["CA_paid_with_coupons_HT"].sum().reset_index(name="CA_paid_with_coupons")
    prix_art = fact.groupby(grp).agg(Qty_total=("Qty","sum"), CA_HT=("CA_HT","sum")).reset_index()
    prix_art["Prix_moyen_HT"] = np.where(prix_art["Qty_total"]>0, prix_art["CA_HT"]/prix_art["Qty_total"], np.nan)
    prix_art = prix_art[grp+["Prix_moyen_HT"]]

    # Panier moyen global / client / non client
    pm_global = base.assign(Panier_moyen_HT=base["CA_HT"]/base["Transactions"])
    pm_client = fact[fact["is_client"]].groupby(grp).agg(CA_HT=("CA_HT","sum"), TX=("TransactionID","nunique")).reset_index()
    pm_client["Panier_client"] = pm_client["CA_HT"]/pm_client["TX"]
    pm_non = fact[~fact["is_client"]].groupby(grp).agg(CA_HT=("CA_HT","sum"), TX=("TransactionID","nunique")).reset_index()
    pm_non["Panier_non_client"] = pm_non["CA_HT"]/pm_non["TX"]

    # =====================================================
    # 6️⃣ Fusion finale KPI
    # =====================================================
    kpi = base.merge(tx_client,on=grp,how="left")\
              .merge(churn,on=grp,how="left")\
              .merge(retention,on=grp,how="left")\
              .merge(ca_coupon,on=grp,how="left")\
              .merge(coupons_used,on=grp,how="left")\
              .merge(coupons_emis,on=grp,how="left")\
              .merge(marge_avant,on=grp,how="left")\
              .merge(pm_global[grp+["Panier_moyen_HT"]],on=grp,how="left")\
              .merge(pm_client[grp+["Panier_client"]],on=grp,how="left")\
              .merge(pm_non[grp+["Panier_non_client"]],on=grp,how="left")\
              .merge(prix_art,on=grp,how="left")

    kpi["Marge_apres"] = kpi["Marge_avant"] - kpi["Montant_coupons_utilises"].fillna(0)
    kpi["Taux_marge_avant"] = kpi["Marge_avant"]/kpi["CA_HT"]
    kpi["Taux_marge_apres"] = kpi["Marge_apres"]/kpi["CA_HT"]
    kpi["Taux_assoc_client"] = kpi["Transaction_client"]/kpi["Transactions"]
    kpi["ROI_proxy"] = (kpi["CA_paid_with_coupons"] - kpi["Montant_coupons_utilises"])/kpi["Montant_coupons_utilises"]
    kpi["Taux_util_bons_montant"] = kpi["Montant_coupons_utilises"]/kpi["Montant_coupons_emis"]
    kpi["Taux_util_bons_qte"] = kpi["Coupons_utilises"]/kpi["Coupons_emis"]
    kpi["Taux_CA_bons"] = kpi["CA_paid_with_coupons"]/kpi["CA_HT"]
    kpi["Date"] = pd.to_datetime(kpi["month"], format="%Y-%m", errors="coerce").dt.strftime("%d/%m/%Y")

    ws_kpi = _open_or_create(SPREADSHEET_ID, SHEET_KPI)
    _update_ws(ws_kpi, kpi)
    st.success(f"✅ KPI_Mensuels mis à jour ({len(kpi)} lignes) + Donnees persistantes ({len(merged)} lignes).")

    # =====================================================
    # 7️⃣ Envoi mail Looker
    # =====================================================
    if st.button("📤 Envoyer le lien Looker par e-mail"):
        recipients = [DEFAULT_RECEIVER] + [e.strip() for e in emails_supp.split(",") if e.strip()]
        msg = EmailMessage()
        msg["Subject"] = f"📊 Rapport fidélité La Tribu - {datetime.today().strftime('%d/%m/%Y')}"
        msg["From"] = SMTP_USER
        msg["To"] = ", ".join(recipients)
        msg.set_content(f"""Bonjour,

Le tableau de bord fidélité La Tribu a été mis à jour.

👉 Accès direct : {LOOKER_URL}

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
