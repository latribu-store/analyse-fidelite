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
st.title("🎯 Analyse Fidélité - La Tribu (v2) - Script complet")

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
gcp_service_account_info = json.loads(response.content)
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = service_account.Credentials.from_service_account_info(gcp_service_account_info, scopes=scopes)
client = gspread.authorize(creds)

# ==========================
# HELPERS
# ==========================
def _ensure_date(s): return pd.to_datetime(s, errors="coerce")
def _month_str(s): return _ensure_date(s).dt.to_period("M").astype(str)
def _norm_cols(df):
    mapping = {c: re.sub(r"[^a-z0-9]", "", c.lower()) for c in df.columns}
    return df.rename(columns=mapping)
def _pick(df, *cands):
    for c in cands:
        k = re.sub(r"[^a-z0-9]", "", c.lower())
        if k in df.columns: return k
    return None
def _read_csv_tolerant(uploaded):
    return pd.read_csv(uploaded, sep=";", encoding="utf-8-sig", on_bad_lines="skip", engine="python")
def _open_or_create(sheet_id, tab_name):
    sh = client.open_by_key(sheet_id)
    try: ws = sh.worksheet(tab_name)
    except Exception: ws = sh.add_worksheet(title=tab_name, rows=2, cols=20)
    return ws
def _ws_to_df(ws):
    rows = ws.get_all_values()
    if not rows: return pd.DataFrame()
    header, data = rows[0], rows[1:]
    return pd.DataFrame(data, columns=header) if data else pd.DataFrame(columns=header)
def _update_ws(ws, df):
    safe_df = df.astype(object).where(pd.notnull(df), "").applymap(lambda x: str(x) if not isinstance(x, str) else x)
    values = [list(safe_df.columns)] + safe_df.values.tolist()
    ws.clear(); ws.update("A1", values)

# ==========================
# UI
# ==========================
st.sidebar.header("📂 Importer les fichiers")
file_tx = st.sidebar.file_uploader("Fichier Transactions (CSV Keyneo)", type=["csv"])
file_cp = st.sidebar.file_uploader("Fichier Bons d’achat (CSV Keyneo)", type=["csv"])
emails_supp = st.sidebar.text_input("📧 Autres destinataires (séparés par des virgules)")

if file_tx and file_cp:
    # --- Lecture & normalisation ---
    tx = _norm_cols(_read_csv_tolerant(file_tx))
    cp = _norm_cols(_read_csv_tolerant(file_cp))

    c_txid = _pick(tx, "ticketnumber", "transactionid", "operationid")
    c_total = _pick(tx, "totalamount")
    c_label = _pick(tx, "label")
    c_valid = _pick(tx, "validationdate", "operationdate")
    c_org = _pick(tx, "organisationid", "organizationid")
    c_cust = _pick(tx, "customerid", "clientid")
    c_gross = _pick(tx, "linegrossamount")
    c_costtot = _pick(tx, "linetotalpurchasingamount")

    for col in [c_total, c_gross, c_costtot]:
        if col and col in tx.columns:
            tx[col] = pd.to_numeric(tx[col], errors="coerce")

    # --- Construction des transactions ---
    ca_ttc = tx.groupby(c_txid, dropna=False)[c_total].max().rename("CA_Net_TTC").reset_index()
    has_coupon = tx.assign(_label=tx[c_label].fillna("").str.upper()) \
        .groupby(c_txid, dropna=False)["_label"].apply(lambda s: (s == "COUPON").any()).rename("Has_Coupon").reset_index()
    fact_tx = ca_ttc.merge(has_coupon, on=c_txid, how="left")
    fact_tx["CA_Paid_With_Coupons"] = np.where(fact_tx["Has_Coupon"], fact_tx["CA_Net_TTC"], 0.0)
    ca_ht = tx.groupby(c_txid, dropna=False)[c_gross].sum().rename("CA_Net_HT").reset_index()
    costs = tx.groupby(c_txid, dropna=False)[c_costtot].sum().rename("Purch_Total_HT").reset_index()
    fact_tx = fact_tx.merge(ca_ht, on=c_txid, how="left").merge(costs, on=c_txid, how="left")
    fact_tx["Estimated_Net_Margin_HT"] = fact_tx["CA_Net_HT"] - fact_tx["Purch_Total_HT"]

    ctx = tx.dropna(subset=[c_txid])[[c_txid, c_valid, c_org, c_cust]].drop_duplicates(subset=[c_txid])
    ctx = ctx.rename(columns={c_txid: "TransactionID", c_valid: "ValidationDate", c_org: "OrganisationId", c_cust: "CustomerID"})
    fact_tx = fact_tx.merge(ctx, left_on=c_txid, right_on="TransactionID", how="left")
    fact_tx["ValidationDate"] = _ensure_date(fact_tx["ValidationDate"])
    fact_tx["month"] = _month_str(fact_tx["ValidationDate"])

    # --- Incrémental corrigé ---
    ws_tx = _open_or_create(SPREADSHEET_ID, SHEET_TX)
    current_tx = _ws_to_df(ws_tx)

    if current_tx.empty:
        merged_tx = fact_tx.copy()
        new_tx = fact_tx.copy()
    else:
        for col in fact_tx.columns:
            if col not in current_tx.columns: current_tx[col] = pd.NA
        for col in current_tx.columns:
            if col not in fact_tx.columns: fact_tx[col] = pd.NA

        existing_ids = set(current_tx["TransactionID"].dropna().unique())
        new_tx = fact_tx[~fact_tx["TransactionID"].isin(existing_ids)]
        merged_tx = pd.concat([current_tx, new_tx], ignore_index=True)

    _update_ws(ws_tx, merged_tx)

    # --- Recharge après mise à jour ---
    fact_tx = _ws_to_df(ws_tx)
    fact_tx["ValidationDate"] = _ensure_date(fact_tx["ValidationDate"])
    fact_tx["month"] = _month_str(fact_tx["ValidationDate"])
    fact_tx["CA_Net_TTC"] = pd.to_numeric(fact_tx["CA_Net_TTC"], errors="coerce")
    fact_tx["CA_Net_HT"] = pd.to_numeric(fact_tx["CA_Net_HT"], errors="coerce")
    fact_tx["Estimated_Net_Margin_HT"] = pd.to_numeric(fact_tx["Estimated_Net_Margin_HT"], errors="coerce")
    fact_tx["CA_Paid_With_Coupons"] = pd.to_numeric(fact_tx["CA_Paid_With_Coupons"], errors="coerce")

    # --- COUPONS ---
    c_init = _pick(cp, "initialvalue")
    c_rem = _pick(cp, "amount")
    c_usedate = _pick(cp, "usedate")
    c_emiss = _pick(cp, "creationdate")
    c_orgc = _pick(cp, "organisationid")
    c_couponid = _pick(cp, "couponid", "id")

    cp["UseDate"] = _ensure_date(cp[c_usedate]) if c_usedate else pd.NaT
    cp["EmissionDate"] = _ensure_date(cp[c_emiss]) if c_emiss else pd.NaT
    cp["Amount_Initial"] = pd.to_numeric(cp[c_init], errors="coerce").fillna(0)
    cp["Amount_Remaining"] = pd.to_numeric(cp[c_rem], errors="coerce").fillna(0)
    cp["Value_Used_Line"] = (cp["Amount_Initial"] - cp["Amount_Remaining"]).clip(lower=0)
    cp["IsUsed"] = cp["Value_Used_Line"] > 0
    cp["Days_To_Use"] = (cp["UseDate"] - cp["EmissionDate"]).dt.days
    cp["month"] = _month_str(cp["UseDate"])
    cp = cp.rename(columns={c_orgc: "OrganisationId", c_couponid: "CouponID"})

    coupons_qte = cp.copy().assign(month_emit=_month_str(cp["EmissionDate"]), month_use=_month_str(cp["UseDate"]))
    nb_emis = coupons_qte.groupby(["month_emit", "OrganisationId"], dropna=False)["CouponID"].nunique().reset_index().rename(columns={"month_emit": "month", "CouponID": "Coupons_Emitted"})
    nb_used = coupons_qte[coupons_qte["IsUsed"]].groupby(["month_use", "OrganisationId"], dropna=False)["CouponID"].nunique().reset_index().rename(columns={"month_use": "month", "CouponID": "Coupons_Used"})
    coupons_stats = nb_emis.merge(nb_used, on=["month", "OrganisationId"], how="left").fillna({"Coupons_Used": 0})

    cp_out = cp[["CouponID", "OrganisationId", "EmissionDate", "UseDate", "Amount_Initial", "Amount_Remaining", "Value_Used_Line", "IsUsed", "Days_To_Use", "month"]]
    ws_cp = _open_or_create(SPREADSHEET_ID, SHEET_COUP)
    _update_ws(ws_cp, cp_out)

    # ==========================
    # --- KPI COMPLET CORRIGÉ ---
    # ==========================

    import pandas as pd
    import numpy as np

    # --- 0) Lecture & nettoyage CSV robuste ---
    def _read_csv_tolerant(uploaded):
        df = pd.read_csv(
            uploaded,
            sep=";",
            encoding="utf-8-sig",
            on_bad_lines="skip",
            engine="python",
            dtype=str
        )
        for col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace("'", "", regex=False)
                .str.replace(",", ".", regex=False)
                .str.strip()
            )
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="ignore")
        return df

    # =====================================================
    # --- 1) Calculs KPI principaux avec corrections ---
    # =====================================================

    # 🔹 Transactions clients et récurrence réelle
    tx_clients = fact_tx.dropna(subset=["CustomerID"]).copy()
    tx_clients["CustomerID"] = tx_clients["CustomerID"].astype(str).str.strip().str.lower()

    # Nombre de transactions et clients identifiés
    churn = (
        tx_clients.groupby(["month", "OrganisationId"], dropna=False)
        .agg(
            Transactions_Client=("TransactionID", "nunique"),
            Customers=("CustomerID", "nunique"),
        )
        .reset_index()
    )

    # Nouveaux clients
    first_seen = (
        tx_clients.groupby("CustomerID")["ValidationDate"]
        .min()
        .reset_index(name="FirstDate")
    )
    tx_clients = tx_clients.merge(first_seen, on="CustomerID", how="left")
    tx_clients["IsNewThisMonth"] = (
        tx_clients["ValidationDate"].dt.to_period("M") == tx_clients["FirstDate"].dt.to_period("M")
    )
    new_cust = (
        tx_clients[tx_clients["IsNewThisMonth"]]
        .groupby(["month", "OrganisationId"], dropna=False)["CustomerID"]
        .nunique()
        .reset_index(name="New_Customers")
    )
    churn = churn.merge(new_cust, on=["month", "OrganisationId"], how="left").fillna(0)

    # Clients revenants et récurrence (uniquement clients connus)
    churn["Returning_Customers"] = churn["Customers"] - churn["New_Customers"]
    churn["Recurrence"] = np.where(
        churn["Customers"] > 0,
        churn["Transactions_Client"] / churn["Customers"],
        np.nan,
    )

    # 🔹 Transactions totales (clients + anonymes)
    tx_all = (
        fact_tx.groupby(["month", "OrganisationId"], dropna=False)["TransactionID"]
        .nunique()
        .reset_index(name="Transactions_All")
    )

    # 🔹 Fusion pour taux d'association
    assoc = churn.merge(tx_all, on=["month", "OrganisationId"], how="left")
    assoc["Taux_Association_Client_Ticket"] = np.where(
        assoc["Transactions_All"] > 0,
        assoc["Transactions_Client"] / assoc["Transactions_All"],
        np.nan,
    )

    # 🔹 Rétention clients
    cust_sets = (
        tx_clients.groupby(["OrganisationId", "month"], dropna=False)["CustomerID"]
        .apply(lambda s: set(s.unique()))
        .reset_index()
        .rename(columns={"CustomerID": "CustSet"})
    )
    cust_sets["_month_order"] = pd.PeriodIndex(cust_sets["month"], freq="M").to_timestamp()
    cust_sets = cust_sets.sort_values(["OrganisationId", "_month_order"])
    cust_sets["PrevCustSet"] = cust_sets.groupby("OrganisationId")["CustSet"].shift(1)

    def _retention(row):
        prev, cur = row["PrevCustSet"], row["CustSet"]
        if not isinstance(prev, set) or len(prev) == 0:
            return np.nan
        return len(prev.intersection(cur)) / len(prev)

    cust_sets["Retention_Rate"] = cust_sets.apply(_retention, axis=1)
    retention = cust_sets[["month", "OrganisationId", "Retention_Rate"]]

    # 🔹 KPI financiers
    kpi_tx = (
        fact_tx.groupby(["month", "OrganisationId"], dropna=False)
        .agg(
            CA_Net_TTC=("CA_Net_TTC", "sum"),
            CA_Net_HT=("CA_Net_HT", "sum"),
            CA_Paid_With_Coupons=("CA_Paid_With_Coupons", "sum"),
            Estimated_Net_Margin_HT=("Estimated_Net_Margin_HT", "sum"),
        )
        .reset_index()
    )

    # 🔹 Bons émis et utilisés
    coupons_month = (
        cp.dropna(subset=["UseDate"])
        .groupby(["month", "OrganisationId"], dropna=False)["Value_Used_Line"]
        .sum()
        .reset_index()
        .rename(columns={"Value_Used_Line": "Value_Used"})
    )
    coupons_emis = (
        cp.copy()
        .assign(month=_month_str(cp["EmissionDate"]))
        .groupby(["month", "OrganisationId"], dropna=False)["Amount_Initial"]
        .sum()
        .reset_index()
        .rename(columns={"Amount_Initial": "Value_Emitted"})
    )

    # 🔹 Statistiques sur les coupons
    coupons_stats = (
        cp.groupby(["month", "OrganisationId"], dropna=False)
        .agg(
            Coupons_Emitted=("CouponID", "count"),
            Coupons_Used=("IsUsed", "sum"),
        )
        .reset_index()
    )

    # 🔹 Paniers moyens
    pm_global = (
        fact_tx.groupby(["month", "OrganisationId"], dropna=False)["CA_Net_TTC"]
        .mean()
        .reset_index(name="PanierMoyen_Global")
    )
    pm_coupon = (
        fact_tx[fact_tx["CA_Paid_With_Coupons"] > 0]
        .groupby(["month", "OrganisationId"], dropna=False)["CA_Net_TTC"]
        .mean()
        .reset_index(name="PanierMoyen_AvecCoupon")
    )
    pm_sans_coupon = (
        fact_tx[fact_tx["CA_Paid_With_Coupons"] == 0]
        .groupby(["month", "OrganisationId"], dropna=False)["CA_Net_TTC"]
        .mean()
        .reset_index(name="PanierMoyen_SansCoupon")
    )
    pm_client = (
        fact_tx[fact_tx["CustomerID"].notna()]
        .groupby(["month", "OrganisationId"], dropna=False)["CA_Net_TTC"]
        .mean()
        .reset_index(name="PanierMoyen_Client")
    )
    pm_sans_client = (
        fact_tx[fact_tx["CustomerID"].isna()]
        .groupby(["month", "OrganisationId"], dropna=False)["CA_Net_TTC"]
        .mean()
        .reset_index(name="PanierMoyen_SansClient")
    )

    # =====================================================
    # --- 2) Fusion finale ---
    # =====================================================

    kpi = (
        kpi_tx.merge(coupons_month, on=["month", "OrganisationId"], how="left")
        .merge(coupons_emis, on=["month", "OrganisationId"], how="left")
        .merge(churn, on=["month", "OrganisationId"], how="left")
        .merge(retention, on=["month", "OrganisationId"], how="left")
        .merge(coupons_stats, on=["month", "OrganisationId"], how="left")
        .merge(assoc, on=["month", "OrganisationId"], how="left")
        .merge(pm_global, on=["month", "OrganisationId"], how="left")
        .merge(pm_coupon, on=["month", "OrganisationId"], how="left")
        .merge(pm_sans_coupon, on=["month", "OrganisationId"], how="left")
        .merge(pm_client, on=["month", "OrganisationId"], how="left")
        .merge(pm_sans_client, on=["month", "OrganisationId"], how="left")
        .fillna(0)
    )

    # =====================================================
    # --- 3) Calculs dérivés ---
    # =====================================================

    kpi["Voucher_Share"] = np.where(
        kpi["CA_Net_TTC"] > 0, kpi["CA_Paid_With_Coupons"] / kpi["CA_Net_TTC"], np.nan
    )
    kpi["Net_Margin_After_Loyalty"] = kpi["Estimated_Net_Margin_HT"] - kpi["Value_Used"]
    kpi["Taux_Marge_Avant_Loyalty"] = np.where(
        kpi["CA_Net_HT"] > 0, kpi["Estimated_Net_Margin_HT"] / kpi["CA_Net_HT"], np.nan
    )
    kpi["Taux_Marge_Apres_Loyalty"] = np.where(
        kpi["CA_Net_HT"] > 0, kpi["Net_Margin_After_Loyalty"] / kpi["CA_Net_HT"], np.nan
    )
    kpi["ROI_Proxy"] = np.where(
        kpi["Value_Used"] > 0,
        (kpi["CA_Paid_With_Coupons"] - kpi["Value_Used"]) / kpi["Value_Used"],
        np.nan,
    )
    kpi["Taux_Utilisation_Coupons_Qté"] = np.where(
        kpi["Coupons_Emitted"] > 0,
        kpi["Coupons_Used"] / kpi["Coupons_Emitted"],
        np.nan,
    )
    kpi["Taux_Utilisation_Coupons_Montant"] = np.where(
        kpi["Value_Emitted"] > 0,
        kpi["Value_Used"] / kpi["Value_Emitted"],
        np.nan,
    )

    # =====================================================
    # --- 4) Colonne Date lisible pour Looker Studio ---
    # =====================================================

    # Conversion au format texte français JJ/MM/AAAA
    kpi["Date"] = pd.to_datetime(kpi["month"], format="%Y-%m").dt.strftime("%d/%m/%Y")

    # =====================================================
    # --- 5) Export & affichage ---
    # =====================================================

    ws_kpi = _open_or_create(SPREADSHEET_ID, SHEET_KPI)
    _update_ws(ws_kpi, kpi)

    st.success(f"✅ {len(new_tx)} nouvelles transactions ajoutées et KPI mis à jour.")
    with st.expander("👀 Aperçu KPI (10 premières lignes)"):
        st.dataframe(kpi.head(10))




    # --- Envoi mail ---
    if st.button("📤 Envoyer le lien Looker par e-mail"):
        recipients = [DEFAULT_RECEIVER, "charles.risso@latribu.fr"] + [
            e.strip() for e in emails_supp.split(",") if e.strip()
        ]
        msg = EmailMessage()
        msg["Subject"] = f"📊 Rapport fidélité La Tribu - {datetime.today().strftime('%d/%m/%Y')}"
        msg["From"] = SMTP_USER
        msg["To"] = ", ".join(recipients)
        msg.set_content(
            f"Bonjour,\n\nVoici le lien vers le tableau de bord fidélité mis à jour :\n👉 {LOOKER_URL}\n\nBien à vous,\nL'équipe La Tribu"
        )

        try:
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USER, SMTP_PASSWORD)
                server.send_message(msg)
            st.success("📈 Lien Looker envoyé par e-mail (via Ionos).")
        except Exception as e:
            st.error("❌ Erreur lors de l’envoi de l’e-mail.")
            st.exception(e)
else:
    st.info("Veuillez importer les CSV **transactions** et **coupons** pour démarrer.")
