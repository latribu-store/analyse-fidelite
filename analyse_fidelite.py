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
    return _ensure_date(s).dt.to_period("M").astype(str)


def _norm_cols(df):
    mapping = {c: re.sub(r"[^a-z0-9]", "", c.lower()) for c in df.columns}
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


def _open_or_create(sheet_id, tab_name):
    sh = client.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(tab_name)
    except Exception:
        ws = sh.add_worksheet(title=tab_name, rows=2, cols=50)
    return ws


def _ws_to_df(ws):
    rows = ws.get_all_values()
    if not rows:
        return pd.DataFrame()
    header, data = rows[0], rows[1:]
    return pd.DataFrame(data, columns=header) if data else pd.DataFrame(columns=header)


def _update_ws(ws, df):
    """Nettoie et envoie un DataFrame vers Google Sheets (tout en texte)."""
    safe_df = (
        df.astype(object)
        .where(pd.notnull(df), "")
        .applymap(lambda x: str(x) if not isinstance(x, str) else x)
    )
    values = [list(safe_df.columns)] + safe_df.values.tolist()
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

    # Colonnes principales
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

    # ------------------------------
    # 2️⃣ MÉTRIQUES TRANSACTIONS
    # ------------------------------
    # 1. CA TTC (par transaction)
    ca_ttc = tx.groupby(c_txid, dropna=False)[c_total].max().rename("CA_Net_TTC").reset_index()

    # 2. CA généré par des coupons (si au moins une ligne libellée COUPON dans le ticket)
    has_coupon = (
        tx.assign(_label=tx[c_label].fillna("").str.upper())
        .groupby(c_txid, dropna=False)["_label"]
        .apply(lambda s: (s == "COUPON").any())
        .rename("Has_Coupon")
        .reset_index()
    )
    fact_tx = ca_ttc.merge(has_coupon, on=c_txid, how="left")
    fact_tx["CA_Paid_With_Coupons"] = np.where(fact_tx["Has_Coupon"], fact_tx["CA_Net_TTC"], 0.0)

    # 3. CA HT (somme des lignes) et coût marchandises
    ca_ht = tx.groupby(c_txid, dropna=False)[c_gross].sum().rename("CA_Net_HT").reset_index()
    costs = tx.groupby(c_txid, dropna=False)[c_costtot].sum().rename("Purch_Total_HT").reset_index()
    fact_tx = fact_tx.merge(ca_ht, on=c_txid, how="left").merge(costs, on=c_txid, how="left")
    fact_tx["Estimated_Net_Margin_HT"] = fact_tx["CA_Net_HT"] - fact_tx["Purch_Total_HT"]

    # Infos contextuelles transaction
    ctx = tx.dropna(subset=[c_txid])[[c_txid, c_valid, c_org, c_cust]].drop_duplicates(subset=[c_txid])
    ctx = ctx.rename(columns={
        c_txid: "TransactionID",
        c_valid: "ValidationDate",
        c_org: "OrganisationId",
        c_cust: "CustomerID",
    })
    fact_tx = fact_tx.merge(ctx, left_on=c_txid, right_on="TransactionID", how="left")
    fact_tx["ValidationDate"] = _ensure_date(fact_tx["ValidationDate"])
    fact_tx["month"] = _month_str(fact_tx["ValidationDate"])

    # ------------------------------
    # 3️⃣ INCRÉMENTAL (uniquement nouvelles transactions)
    # ------------------------------
    ws_tx = _open_or_create(SPREADSHEET_ID, SHEET_TX)
    current_tx = _ws_to_df(ws_tx)

    if current_tx.empty:
        merged_tx, new_tx = fact_tx.copy(), fact_tx.copy()
    else:
        # Harmonisation des colonnes
        for col in fact_tx.columns:
            if col not in current_tx.columns:
                current_tx[col] = pd.NA
        for col in current_tx.columns:
            if col not in fact_tx.columns:
                fact_tx[col] = pd.NA
        current_tx = current_tx[fact_tx.columns]
        existing_ids = set(current_tx["TransactionID"].dropna().unique())
        new_tx = fact_tx[~fact_tx["TransactionID"].isin(existing_ids)]
        merged_tx = pd.concat([current_tx, new_tx], ignore_index=True)

    # 🔄 MAJ de l'onglet Donnees
    _update_ws(ws_tx, merged_tx)

    # ------------------------------
    # 4️⃣ COUPONS (snapshot complet)
    # ------------------------------
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
    cp["month"] = _month_str(cp["UseDate"])  # mois d'utilisation

    # ✅ Normalisation des noms pour jointure
    rename_cp = {}
    if c_orgc:
        rename_cp[c_orgc] = "OrganisationId"
    if c_couponid:
        rename_cp[c_couponid] = "CouponID"
    cp = cp.rename(columns=rename_cp)

    cp_out = cp[[
        "CouponID" if "CouponID" in cp.columns else c_couponid,
        "OrganisationId" if "OrganisationId" in cp.columns else c_orgc,
        "EmissionDate",
        "UseDate",
        "Amount_Initial",
        "Amount_Remaining",
        "Value_Used_Line",
        "IsUsed",
        "Days_To_Use",
        "month",
    ]]

    ws_cp = _open_or_create(SPREADSHEET_ID, SHEET_COUP)
    _update_ws(ws_cp, cp_out)

    # ------------------------------
    # 5) JOINTURES KPI MENSUELS
    # ------------------------------

    # 🔹 On ne garde que les transactions avec un CustomerID pour les stats client
    base_clients = fact_tx.dropna(subset=["CustomerID"]).copy()

    # Normalisation CustomerID (évite doublons type MAJ/min ou espaces)
    base_clients["CustomerID"] = base_clients["CustomerID"].astype(str).str.strip().str.lower()

    # Tri des données par client/date pour identifier les nouveaux clients
    base_clients = base_clients.sort_values(["CustomerID", "ValidationDate"])
    first_seen = base_clients.groupby("CustomerID", dropna=True)["ValidationDate"].min().reset_index(name="FirstDate")
    base2 = base_clients.merge(first_seen, on="CustomerID", how="left")
    base2["IsNewThisMonth"] = base2["ValidationDate"].dt.to_period("M") == base2["FirstDate"].dt.to_period("M")

    # Compte les clients uniques nouveaux par mois (pas les transactions)
    new_cust = (
    base2[base2["IsNewThisMonth"]]
    .groupby(["month", "OrganisationId"], dropna=False)["CustomerID"]
    .nunique()
    .reset_index(name="New_Customers")
    )

    # Agrégats principaux
    churn = (
    base2.groupby(["month", "OrganisationId"], dropna=False)
    .agg(
        Transactions=("TransactionID", "nunique"),
        Customers=("CustomerID", "nunique")
    )
    .reset_index()
    .merge(new_cust, on=["month", "OrganisationId"], how="left")
    .fillna({"New_Customers": 0})
    )

    churn["Returning_Customers"] = churn["Customers"] - churn["New_Customers"]
    churn["Recurrence"] = np.where(
    churn["Customers"] > 0, churn["Transactions"] / churn["Customers"], np.nan
    )


    # ✅ Rétention M-1 ➜ M par magasin (share des clients de M-1 qui reviennent en M)
    # Liste des clients par (mois, org)
    cust_sets = (
        base2.groupby(["OrganisationId", "month"], dropna=False)["CustomerID"]
        .apply(lambda s: set(s.unique()))
        .reset_index()
        .rename(columns={"CustomerID": "CustSet"})
    )
    # On ordonne les mois correctement
    cust_sets["_month_order"] = pd.PeriodIndex(cust_sets["month"], freq="M").to_timestamp()
    cust_sets = cust_sets.sort_values(["OrganisationId", "_month_order"]) 

    # Décalage pour retrouver le set de M-1
    cust_sets["PrevCustSet"] = cust_sets.groupby("OrganisationId")["CustSet"].shift(1)

    def _retention(row):
        prev_set = row["PrevCustSet"]
        cur_set = row["CustSet"]
        if not isinstance(prev_set, set) or len(prev_set) == 0:
            return np.nan
        return len(prev_set.intersection(cur_set)) / len(prev_set)

    cust_sets["Retention_Rate"] = cust_sets.apply(_retention, axis=1)
    retention = cust_sets[["month", "OrganisationId", "Retention_Rate"]]

    # Agrégats transactionnels (CA + marge)
    kpi_tx = fact_tx.groupby(["month", "OrganisationId"], dropna=False).agg(
        CA_Net_TTC=("CA_Net_TTC", "sum"),
        CA_Net_HT=("CA_Net_HT", "sum"),  # ✅ correction du nom de colonne
        CA_Paid_With_Coupons=("CA_Paid_With_Coupons", "sum"),
        Estimated_Net_Margin_HT=("Estimated_Net_Margin_HT", "sum"),
    ).reset_index()

    # Valeur des bons utilisés (somme par mois/org sur la base des utilisations)
    coupons_month = (
        cp.dropna(subset=["UseDate"]) 
        .groupby(["month", "OrganisationId"], dropna=False)["Value_Used_Line"]
        .sum()
        .reset_index()
        .rename(columns={"Value_Used_Line": "Value_Used"})
    )

    # Montant des bons émis (mois d'émission)
    coupons_emis = (
        cp.copy()
        .assign(month=_month_str(cp["EmissionDate"]))
        .groupby(["month", "OrganisationId"], dropna=False)["Amount_Initial"]
        .sum()
        .reset_index()
        .rename(columns={"Amount_Initial": "Value_Emitted"})
    )

    # Fusion des KPI
    kpi = (
        kpi_tx
        .merge(coupons_month, on=["month", "OrganisationId"], how="left")
        .merge(coupons_emis, on=["month", "OrganisationId"], how="left")
        .merge(churn, on=["month", "OrganisationId"], how="left")
        .merge(retention, on=["month", "OrganisationId"], how="left")
    )

    kpi = kpi.fillna({
        "Value_Used": 0,
        "Value_Emitted": 0,
        "Transactions": 0,
        "Customers": 0,
        "New_Customers": 0,
        "Returning_Customers": 0,
    })

    # Ratios & indicateurs de performance (basés sur le CA HT)
    kpi["Voucher_Share"] = np.where(
        kpi["CA_Net_TTC"] > 0, kpi["CA_Paid_With_Coupons"] / kpi["CA_Net_TTC"], np.nan
    )
    kpi["Net_Margin_After_Loyalty"] = kpi["Estimated_Net_Margin_HT"] - kpi["Value_Used"]

    # 🧮 Calculs corrigés : taux de marge basés sur le CA HT
    kpi["Taux_Marge_Avant_Loyalty"] = np.where(
        kpi["CA_Net_HT"] > 0, kpi["Estimated_Net_Margin_HT"] / kpi["CA_Net_HT"], np.nan
    )

    kpi["Taux_Marge_Apres_Loyalty"] = np.where(
        kpi["CA_Net_HT"] > 0, kpi["Net_Margin_After_Loyalty"] / kpi["CA_Net_HT"], np.nan
    )

    # Proxy de ROI côté coupons (surcoût vs CA généré avec coupon)
    kpi["ROI_Proxy"] = np.where(
        kpi["Value_Used"] > 0,
        (kpi["CA_Paid_With_Coupons"] - kpi["Value_Used"]) / kpi["Value_Used"],
        np.nan,
    )

    # Panier moyen global et avec coupon (par client ➜ moyenne des clients)
    base_clients["has_coupon"] = base_clients["CA_Paid_With_Coupons"].fillna(0) > 0

    pm = (
        base_clients.groupby(["month", "OrganisationId", "CustomerID"], dropna=False)["CA_Net_TTC"]
        .mean()
        .reset_index()
        .rename(columns={"CA_Net_TTC": "Avg_Basket_Value"})
    )
    pmc = (
        base_clients[base_clients["has_coupon"]]
        .groupby(["month", "OrganisationId", "CustomerID"], dropna=False)["CA_Net_TTC"]
        .mean()
        .reset_index()
        .rename(columns={"CA_Net_TTC": "Avg_Basket_Value_With_Coupon"})
    )

    pm2 = pm.groupby(["month", "OrganisationId"], dropna=False)["Avg_Basket_Value"].mean().reset_index()
    pmc2 = (
        pmc.groupby(["month", "OrganisationId"], dropna=False)["Avg_Basket_Value_With_Coupon"]
        .mean()
        .reset_index()
    )

    # Ajout des paniers moyens dans le KPI final
    kpi = (
        kpi.merge(pm2, on=["month", "OrganisationId"], how="left")
        .merge(pmc2, on=["month", "OrganisationId"], how="left")
    )

    # Ordre de colonnes (inclure les demandes spécifiques)
    ordered_cols = [
        "month",
        "OrganisationId",
        # Volumes & clients
        "Transactions",
        "Customers",
        "New_Customers",
        "Returning_Customers",
        "Recurrence",
        "Retention_Rate",
        # Chiffres
        "CA_Net_TTC",
        "CA_Net_HT",
        "CA_Paid_With_Coupons",
        "Value_Used",       # Montant des bons UTILISÉS
        "Value_Emitted",    # Montant des bons ÉMIS (🆕 à côté)
        # Marges & ratios
        "Estimated_Net_Margin_HT",
        "Net_Margin_After_Loyalty",
        "Taux_Marge_Avant_Loyalty",
        "Taux_Marge_Apres_Loyalty",
        "Voucher_Share",
        "ROI_Proxy",
        # Paniers moyens
        "Avg_Basket_Value",
        "Avg_Basket_Value_With_Coupon",
    ]
    # Ajoute les colonnes manquantes sans casser si certaines n'existent pas (défensif)
    ordered_cols = [c for c in ordered_cols if c in kpi.columns] + [c for c in kpi.columns if c not in ordered_cols]
    kpi = kpi[ordered_cols]

    # ------------------------------
    # 6️⃣ ÉCRITURE KPI ➜ Google Sheets
    # ------------------------------
    ws_kpi = _open_or_create(SPREADSHEET_ID, SHEET_KPI)
    _update_ws(ws_kpi, kpi)

    # ------------------------------
    # 7️⃣ ENVOI EMAIL (optionnel)
    # ------------------------------
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

    st.success(f"✅ {len(new_tx)} nouvelles transactions ajoutées et KPI mis à jour.")

    with st.expander("👀 Aperçu KPI (10 premières lignes)"):
        st.dataframe(kpi.head(10))

else:
    st.info("Veuillez importer les CSV **transactions** et **coupons** pour démarrer.")
