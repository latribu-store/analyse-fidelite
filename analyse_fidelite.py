import streamlit as st
import pandas as pd
import numpy as np
import duckdb
import os
from datetime import datetime

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
st.set_page_config(page_title="🎯 Analyse Fidélité — Historique DuckDB ➜ KPI mensuels", layout="wide")
st.title("🎯 Analyse Fidélité — Historique DuckDB ➜ KPI mensuels")

DUCKDB_PATH = "historique.duckdb"

# ------------------------------------------------------------
# INIT DUCKDB (robuste)
# ------------------------------------------------------------
def init_duckdb():
    if os.path.exists(DUCKDB_PATH):
        try:
            con_tmp = duckdb.connect(DUCKDB_PATH)
            cols = [r[0] for r in con_tmp.execute("PRAGMA table_info('transactions')").fetchall()]
            if "TransactionID" not in cols:
                os.remove(DUCKDB_PATH)
                st.warning("⚠️ Fichier DuckDB corrompu : recréé automatiquement.")
        except Exception:
            os.remove(DUCKDB_PATH)
            st.warning("⚠️ Fichier DuckDB supprimé et recréé (erreur d’ouverture).")

    con = duckdb.connect(DUCKDB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            TransactionID TEXT PRIMARY KEY,
            ValidationDate TIMESTAMP,
            month TEXT,
            OrganisationID TEXT,
            CustomerID TEXT,
            CA_TTC DOUBLE,
            CA_HT DOUBLE,
            Purch_Total_HT DOUBLE,
            Qty_Ticket DOUBLE,
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

con = init_duckdb()

# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------
def _ensure_date(s): return pd.to_datetime(s, errors="coerce")
def _month_str(s): return _ensure_date(s).dt.to_period("M").astype(str)
def read_csv(uploaded):
    df = pd.read_csv(uploaded, sep=";", encoding="utf-8-sig", on_bad_lines="skip")
    df.columns = [c.strip() for c in df.columns]
    return df

# ------------------------------------------------------------
# UI
# ------------------------------------------------------------
st.sidebar.header("📂 Importer les fichiers")
file_tx = st.sidebar.file_uploader("Transactions (CSV Keyneo)", type=["csv"])
file_cp = st.sidebar.file_uploader("Coupons (CSV Keyneo)", type=["csv"])

if file_tx and file_cp:
    # =====================================================
    # 1️⃣ LECTURE
    # =====================================================
    tx = read_csv(file_tx)
    cp = read_csv(file_cp)

    # Colonnes Transactions (fixes Keyneo)
    tx.rename(columns={
        "OperationID": "TransactionID",
        "ValidationDate": "ValidationDate",
        "OrganisationID": "OrganisationID",
        "CustomerID": "CustomerID",
        "LineGrossAmount": "CA_HT",
        "LineTotalPurchasingAmount": "Purch_Total_HT",
        "Quantity": "Qty_Ticket",
        "TotalAmountTTC": "CA_TTC"
    }, inplace=True)

    tx["ValidationDate"] = _ensure_date(tx["ValidationDate"])
    tx["month"] = _month_str(tx["ValidationDate"])
    tx["Estimated_Net_Margin_HT"] = tx["CA_HT"] - tx["Purch_Total_HT"]

    # Colonnes coupons (fixes Keyneo)
    cp.rename(columns={
        "CouponID": "CouponID",
        "CreationDate": "EmissionDate",
        "UseDate": "UseDate",
        "InitialValue": "Amount_Initial",
        "Amount": "Amount_Remaining",
        "OrganisationID": "OrganisationID"
    }, inplace=True)
    cp["UseDate"] = _ensure_date(cp["UseDate"])
    cp["EmissionDate"] = _ensure_date(cp["EmissionDate"])
    cp["month_use"] = _month_str(cp["UseDate"])
    cp["month_emit"] = _month_str(cp["EmissionDate"])
    cp["Value_Used_Line"] = (cp["Amount_Initial"] - cp["Amount_Remaining"]).clip(lower=0)

    # =====================================================
    # 2️⃣ UPSERT DANS DUCKDB
    # =====================================================
    existing = con.execute("SELECT TransactionID FROM transactions").fetchdf()
    existing_ids = set(existing["TransactionID"]) if not existing.empty else set()
    new_tx = tx[~tx["TransactionID"].astype(str).isin(existing_ids)]
    if not new_tx.empty:
        con.append("transactions", new_tx)
        st.success(f"✅ {len(new_tx)} nouvelles transactions ajoutées à l’historique.")
    else:
        st.info("ℹ️ Aucune nouvelle transaction à insérer.")

    existing_cp = con.execute("SELECT CouponID FROM coupons").fetchdf()
    existing_cp_ids = set(existing_cp["CouponID"]) if not existing_cp.empty else set()
    new_cp = cp[~cp["CouponID"].astype(str).isin(existing_cp_ids)]
    if not new_cp.empty:
        con.append("coupons", new_cp)
        st.success(f"✅ {len(new_cp)} nouveaux coupons ajoutés à l’historique.")
    else:
        st.info("ℹ️ Aucun nouveau coupon à insérer.")

    # =====================================================
    # 3️⃣ KPI MENSUELS
    # =====================================================
    df_tx = con.execute("SELECT * FROM transactions").fetchdf()
    df_cp = con.execute("SELECT * FROM coupons").fetchdf()

    base = df_tx.groupby(["month","OrganisationID"]).agg(
        CA_TTC=("CA_TTC","sum"),
        CA_HT=("CA_HT","sum"),
        Marge_net_HT_avant_coupon=("Estimated_Net_Margin_HT","sum"),
        Transactions=("TransactionID","nunique"),
        Qty_total=("Qty_Ticket","sum"),
        Clients=("CustomerID","nunique")
    ).reset_index()

    coupons_used = df_cp.dropna(subset=["UseDate"]).groupby(["month_use","OrganisationID"]).agg(
        Coupons_utilise=("CouponID","nunique"),
        Montant_coupons_utilise=("Value_Used_Line","sum")
    ).rename(columns={"month_use":"month"}).reset_index()

    coupons_emis = df_cp.dropna(subset=["EmissionDate"]).groupby(["month_emit","OrganisationID"]).agg(
        Coupons_emis=("CouponID","nunique"),
        Montant_coupons_emis=("Amount_Initial","sum")
    ).rename(columns={"month_emit":"month"}).reset_index()

    kpi = base.merge(coupons_used, on=["month","OrganisationID"], how="left") \
              .merge(coupons_emis, on=["month","OrganisationID"], how="left")

    # Calculs
    kpi["Marge_net_HT_apres_coupon"] = kpi["Marge_net_HT_avant_coupon"] - kpi["Montant_coupons_utilise"].fillna(0)
    kpi["Taux_marge_HT_avant_coupon"] = kpi["Marge_net_HT_avant_coupon"] / kpi["CA_HT"]
    kpi["Taux_marge_HT_apres_coupon"] = kpi["Marge_net_HT_apres_coupon"] / kpi["CA_HT"]
    kpi["Taux_utilisation_bons_montant"] = kpi["Montant_coupons_utilise"] / kpi["Montant_coupons_emis"]
    kpi["Taux_CA_genere_par_bons_sur_CA_HT"] = kpi["Montant_coupons_utilise"] / kpi["CA_HT"]
    kpi["Panier_moyen_HT"] = kpi["CA_HT"] / kpi["Transactions"]
    kpi["Prix_moyen_article_vendu_HT"] = np.where(kpi["Qty_total"]>0, kpi["CA_HT"]/kpi["Qty_total"], np.nan)
    kpi["Date"] = pd.to_datetime(kpi["month"]).dt.strftime("%d/%m/%Y")

    st.subheader("📊 Aperçu KPI mensuel")
    st.dataframe(kpi.head(20))

else:
    st.info("➡️ Importez les fichiers Transactions et Coupons pour démarrer.")
