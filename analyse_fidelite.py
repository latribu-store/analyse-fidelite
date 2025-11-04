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
HISTO_FILE = "historique_fidelite.csv"   # sauvegarde locale (optionnelle)
SPREADSHEET_ID = "1xYQ0mjr37Fnmal8yhi0kBE3_96EN6H6qsYrAtQkHp"   
SHEET_TX = "Donnees"                     # transactions enrichies (niveau transaction)
SHEET_KPI = "KPI_Mensuels"               # agrégats pour Looker
SHEET_COUP = "Coupons"                   # snapshot des bons
LOOKER_URL = "https://lookerstudio.google.com/reporting/a0037205-4433-4f9e-b19b-1601fbf24006"  

SMTP_SERVER = st.secrets["email"]["smtp_server"]
SMTP_PORT = st.secrets["email"]["smtp_port"]
SMTP_USER = st.secrets["email"]["smtp_user"]
SMTP_PASSWORD = st.secrets["email"]["smtp_password"]
DEFAULT_RECEIVER = st.secrets["email"]["receiver"]

# Auth Google via Service Account stocké sur Drive
file_id = "12O9eFGFmwTu1n6kF4AIDIm0KXKMIgOvg"  # <-- ton JSON de service account sur Drive
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
    """Normalise les noms de colonnes (insensible à la casse / underscores)."""
    mapping = {}
    for c in df.columns:
        k = re.sub(r"[^a-z0-9]", "", c.lower())
        mapping[c] = k
    df = df.rename(columns=mapping)
    return df

def _pick(df, *cands):
    """Retourne la première colonne dispo parmi cands (après normalisation)."""
    for c in cands:
        k = re.sub(r"[^a-z0-9]", "", c.lower())
        if k in df.columns:
            return k
    return None

def _read_csv_tolerant(uploaded):
    return pd.read_csv(
        uploaded,
        sep=";",
        encoding="utf-8-sig",
        on_bad_lines="skip",
        engine="python",
    )

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
    # 1) LECTURE + NORMALISATION
    # ------------------------------
    tx_raw = _read_csv_tolerant(file_tx)
    cp_raw = _read_csv_tolerant(file_cp)

    tx = _norm_cols(tx_raw)
    cp = _norm_cols(cp_raw)

    # colonnes clés (transactions)
    c_valid = _pick(tx, "ValidationDate", "validation_date", "date", "operationdate")
    c_label = _pick(tx, "Label", "paymentmethod", "tenderlabel")
    c_linetype = _pick(tx, "LineType", "linetype")
    c_total = _pick(tx, "TotalAmount", "total_ttc", "totalamount")
    c_org = _pick(tx, "OrganisationId", "OrganisationID", "organizationid", "organisationid")
    c_cust = _pick(tx, "CustomerID", "CustomerId", "clientid", "customer")
    c_txid = _pick(tx, "TransactionID", "TransactionId", "TicketId", "OperationId", "DocumentId", "DocNumber")
    c_qty = _pick(tx, "Quantity", "qty")
    c_gross = _pick(tx, "LineGrossAmount", "gross_ht", "linegrossamount")
    c_cost = _pick(tx, "lineUnitPurchasingPrice", "unit_purchase", "lineunitpurchasingprice")

    # colonnes clés (coupons)
    c_init = _pick(cp, "InitialValue", "initialamount", "initialvalue")
    c_rem = _pick(cp, "Amount", "remaining", "amount")
    c_usedate = _pick(cp, "UseDate", "usedate")
    c_emiss = _pick(cp, "CreationDate", "EmissionDate", "creationdate", "emissiondate")
    c_orgc = _pick(cp, "OrganisationId", "OrganisationID", "organizationid", "organisationid")
    c_couponid = _pick(cp, "CouponID", "CouponId", "Id", "Code")

    # Sécurise types
    if c_qty and c_qty in tx.columns: tx[c_qty] = pd.to_numeric(tx[c_qty], errors="coerce").fillna(1)
    if c_gross and c_gross in tx.columns: tx[c_gross] = pd.to_numeric(tx[c_gross], errors="coerce").fillna(0)
    if c_cost and c_cost in tx.columns: tx[c_cost] = pd.to_numeric(tx[c_cost], errors="coerce").fillna(0)
    if c_total and c_total in tx.columns: tx[c_total] = pd.to_numeric(tx[c_total], errors="coerce").fillna(0)
    if c_init and c_init in cp.columns: cp[c_init] = pd.to_numeric(cp[c_init], errors="coerce").fillna(0)
    if c_rem and c_rem in cp.columns: cp[c_rem] = pd.to_numeric(cp[c_rem], errors="coerce").fillna(0)

    # ------------------------------
    # 2) FACT TRANSACTIONS (niveau transaction)
    # ------------------------------
    tx["__is_tender"] = (tx[c_linetype].str.upper() == "TENDER") if c_linetype else False
    tx["__is_product"] = (tx[c_linetype].str.upper() == "PRODUCT_SALE") if c_linetype else False

    # marge estimée sur lignes produit
    prod = tx[tx["__is_product"]].copy()
    if c_cost and c_gross and c_qty:
        prod["estimated_margin_ht_line"] = prod[c_gross] - (prod[c_cost] * prod[c_qty])
    else:
        prod["estimated_margin_ht_line"] = 0

    # clé transaction robuste
    key_cols = [x for x in [c_txid, c_valid, c_org, c_cust] if x]
    if not key_cols:
        st.error("Impossible d’identifier un identifiant de transaction. Vérifie les colonnes TransactionID / TicketId / OperationId.")
        st.stop()

    # agrège marge par transaction
    margin_tx = prod.groupby([k for k in key_cols if k], dropna=False)["estimated_margin_ht_line"] \
                    .sum().reset_index().rename(columns={"estimated_margin_ht_line":"Estimated_Net_Margin_HT"})

    # lignes TENDER = total TTC et mode de paiement
    tend = tx[tx["__is_tender"]].copy()
    # montant payé en bon (Label == COUPON)
    tend["is_coupon"] = tend[c_label].str.upper().eq("COUPON") if c_label else False

    totals = tend.groupby([k for k in key_cols if k], dropna=False)[c_total] \
                 .sum().reset_index().rename(columns={c_total:"CA_Net_TTC"})
    coupons_paid = tend[tend["is_coupon"]].groupby([k for k in key_cols if k], dropna=False)[c_total] \
                    .sum().reset_index().rename(columns={c_total:"CA_Paid_With_Coupons"})

    # merge au niveau transaction
    fact_tx = totals.merge(coupons_paid, on=key_cols, how="left") \
                    .merge(margin_tx, on=key_cols, how="left")
    for col in ["CA_Paid_With_Coupons", "Estimated_Net_Margin_HT"]:
        if col not in fact_tx.columns: fact_tx[col] = 0
    # ajoute colonnes utiles
    fact_tx["ValidationDate"] = _ensure_date(tx.set_index(key_cols)[c_valid]).reset_index(drop=True) if c_valid else pd.NaT
    fact_tx["month"] = _month_str(fact_tx["ValidationDate"])
    if c_org: fact_tx["OrganisationId"] = fact_tx[c_org]
    if c_cust: fact_tx["CustomerID"] = fact_tx[c_cust]
    if c_txid: fact_tx["TransactionID"] = fact_tx[c_txid]

    # ------------------------------
    # 3) COUPONS (snapshot + métriques)
    # ------------------------------
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

    # valeur utilisée mensuelle par magasin
    coupons_month = cp.dropna(subset=["UseDate"]).groupby(["month","OrganisationId"], dropna=False)["Value_Used_Line"] \
                       .sum().reset_index().rename(columns={"Value_Used_Line":"Value_Used"})

    # ------------------------------
    # 4) ENRICHISSEMENTS CLIENT (récurrence)
    # ------------------------------
    # base transactionnelle pour calcul client/mois
    base = fact_tx.copy()
    base["has_coupon"] = base["CA_Paid_With_Coupons"].fillna(0) > 0

    # Panier moyen (TTC) par client/mois
    pm = base.groupby(["month","OrganisationId","CustomerID"], dropna=False)["CA_Net_TTC"] \
             .mean().reset_index().rename(columns={"CA_Net_TTC":"Avg_Basket_Value"})
    pmc = base[base["has_coupon"]].groupby(["month","OrganisationId","CustomerID"], dropna=False)["CA_Net_TTC"] \
              .mean().reset_index().rename(columns={"CA_Net_TTC":"Avg_Basket_Value_With_Coupon"})

    # nouveaux vs revenants par mois
    base_sorted = base.sort_values(["CustomerID","ValidationDate"])
    first_seen = base_sorted.groupby("CustomerID", dropna=True)["ValidationDate"].min().reset_index(name="FirstDate")
    base2 = base_sorted.merge(first_seen, on="CustomerID", how="left")
    base2["IsNewThisMonth"] = base2["ValidationDate"].dt.to_period("M") == base2["FirstDate"].dt.to_period("M")

    churn = base2.groupby(["month","OrganisationId"], dropna=False).agg(
        Transactions=("TransactionID","nunique"),
        Customers=("CustomerID","nunique"),
        New_Customers=("IsNewThisMonth","sum")
    ).reset_index()
    churn["Returning_Customers"] = churn["Customers"] - churn["New_Customers"]
    churn["Recurrence"] = np.where(churn["Customers"]>0, churn["Transactions"]/churn["Customers"], np.nan)
    churn["Retention_Rate"] = np.nan  # (à calculer inter-mois en Looker avec window si besoin)

    # ------------------------------
    # 5) JOINTURES KPI MENSUELS
    # ------------------------------
    # Agrégats transaction côté CA & marge
    kpi_tx = fact_tx.groupby(["month","OrganisationId"], dropna=False).agg(
        CA_Net_TTC=("CA_Net_TTC","sum"),
        CA_Paid_With_Coupons=("CA_Paid_With_Coupons","sum"),
        Estimated_Net_Margin_HT=("Estimated_Net_Margin_HT","sum"),
    ).reset_index()

    kpi = kpi_tx.merge(coupons_month, on=["month","OrganisationId"], how="left").fillna(0)
    kpi["Voucher_Share"] = np.where(kpi["CA_Net_TTC"]>0, kpi["CA_Paid_With_Coupons"]/kpi["CA_Net_TTC"], np.nan)
    kpi["Net_Margin_After_Loyalty"] = kpi["Estimated_Net_Margin_HT"] - kpi["Value_Used"]
    kpi["ROI_Proxy"] = np.where(kpi["Value_Used"]>0, (kpi["CA_Paid_With_Coupons"] - kpi["Value_Used"]) / kpi["Value_Used"], np.nan)

    # ajoute agrégats clients
    pm2 = pm.groupby(["month","OrganisationId"], dropna=False)["Avg_Basket_Value"].mean().reset_index()
    pmc2 = pmc.groupby(["month","OrganisationId"], dropna=False)["Avg_Basket_Value_With_Coupon"].mean().reset_index()
    kpi = kpi.merge(churn, on=["month","OrganisationId"], how="left") \
             .merge(pm2, on=["month","OrganisationId"], how="left") \
             .merge(pmc2, on=["month","OrganisationId"], how="left")

    # ------------------------------
    # 6) INCRÉMENTAL → Google Sheets
    # ------------------------------
    ws_tx = _open_or_create(SPREADSHEET_ID, SHEET_TX)
    ws_kpi = _open_or_create(SPREADSHEET_ID, SHEET_KPI)
    ws_cp = _open_or_create(SPREADSHEET_ID, SHEET_COUP)

    # a) Transactions (APPEND INTELLIGENT par TransactionID)
    current_tx = _ws_to_df(ws_tx)
    # colonnes finales à conserver
    wanted_cols = [
        "month","OrganisationId","TransactionID","ValidationDate","CustomerID",
        "CA_Net_TTC","CA_Paid_With_Coupons","Estimated_Net_Margin_HT",
    ]
    for col in wanted_cols:
        if col not in fact_tx.columns:
            fact_tx[col] = pd.NA
    fact_tx_out = fact_tx[wanted_cols].copy()
    # casts
    fact_tx_out["ValidationDate"] = pd.to_datetime(fact_tx_out["ValidationDate"]).dt.strftime("%Y-%m-%d")

    if current_tx.empty:
        merged_tx = fact_tx_out.copy()
    else:
        # aligne colonnes
        for col in fact_tx_out.columns:
            if col not in current_tx.columns:
                current_tx[col] = pd.NA
        for col in current_tx.columns:
            if col not in fact_tx_out.columns:
                fact_tx_out[col] = pd.NA
        current_tx = current_tx[fact_tx_out.columns]
        merged_tx = pd.concat([current_tx, fact_tx_out], ignore_index=True)
        # dédoublonne par TransactionID si dispo, sinon fallback
        if "TransactionID" in merged_tx.columns and merged_tx["TransactionID"].notna().any():
            merged_tx = merged_tx.sort_values("ValidationDate").drop_duplicates(subset=["TransactionID"], keep="last")
        else:
            fallback_key = ["ValidationDate","OrganisationId","CustomerID","CA_Net_TTC"]
            merged_tx = merged_tx.sort_values("ValidationDate").drop_duplicates(subset=fallback_key, keep="last")

    _update_ws(ws_tx, merged_tx)

    # b) KPI_Mensuels (remplacement contrôlé)
    kpi_out = kpi.copy()
    _update_ws(ws_kpi, kpi_out)

    # c) Coupons (snapshot complet à chaque exé)
    cp_out = cp[[
        "CouponID","OrganisationId","EmissionDate","UseDate",
        "Amount_Initial","Amount_Remaining","Value_Used_Line","IsUsed","Days_To_Use","month"
    ]].copy()
    cp_out["EmissionDate"] = pd.to_datetime(cp_out["EmissionDate"]).dt.strftime("%Y-%m-%d")
    cp_out["UseDate"] = pd.to_datetime(cp_out["UseDate"]).dt.strftime("%Y-%m-%d")
    _update_ws(ws_cp, cp_out)

    # ------------------------------
    # 7) SAUVEGARDE LOCALE (optionnelle)
    # ------------------------------
    try:
        merged_tx.to_csv(HISTO_FILE, index=False)
    except Exception:
        pass

    st.success("✅ Données envoyées sur Google Sheets (Donnees, KPI_Mensuels, Coupons).")

    # ------------------------------
    # 8) EMAIL
    # ------------------------------
    if st.button("📤 Envoyer le lien Looker par e-mail"):
        recipients_default = [
            "charles.risso@latribu.fr"
        ]
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

    # APERÇUS
    with st.expander("📦 Aperçu Transactions (Donnees)"):
        st.dataframe(merged_tx.head(100), use_container_width=True)
    with st.expander("📦 Aperçu KPI_Mensuels"):
        st.dataframe(kpi_out, use_container_width=True)
    with st.expander("📦 Aperçu Coupons (snapshot)"):
        st.dataframe(cp_out.head(100), use_container_width=True)

else:
    st.info("Veuillez importer le CSV **transactions** et le CSV **coupons** pour démarrer.")
