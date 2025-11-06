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
# DUCKDB INIT (robuste)
# ------------------------------------------------------------
def init_duckdb():
    # Supprime si mal formé
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

    # On identifie les colonnes automatiquement
    c_txid = next((c for c in tx.columns if c.lower() in ["transactionid", "ticketnumber", "operationid"]), None)
    c_total = next((c for c in tx.columns if "ttc" in c.lower() or "total" in c.lower()), None)
    c_ht = next((c for c in tx.columns if "ht" in c.lower() and "total" in c.lower()), None)
    c_val = next((c for c in tx.columns if "validation" in c.lower() or "date" in c.lower()), None)
    c_org = next((c for c in tx.columns if "organisation" in c.lower() or "organization" in c.lower()), None)
    c_cust = next((c for c in tx.columns if "customer" in c.lower() or "client" in c.lower()), None)
    c_cost = next((c for c in tx.columns if "purch" in c.lower() or "achat" in c.lower()), None)
    c_qty = next((c for c in tx.columns if "quantity" in c.lower() or "qty" in c.lower()), None)

    if not c_txid:
        st.error("❌ Impossible d’identifier la colonne TransactionID dans le fichier transactions.")
        st.stop()

    # =====================================================
    # 2️⃣ FACT
    # =====================================================
    tx[c_val] = _ensure_date(tx[c_val])
    tx["month"] = _month_str(tx[c_val])
    tx["TransactionID"] = tx[c_txid].astype(str)
    tx["OrganisationID"] = tx[c_org].astype(str)
    tx["CustomerID"] = tx[c_cust].astype(str)
    tx["is_client"] = tx["CustomerID"].notna()

    tx["CA_TTC"] = pd.to_numeric(tx[c_total], errors="coerce")
    tx["CA_HT"] = pd.to_numeric(tx[c_ht], errors="coerce")
    tx["Purch_Total_HT"] = pd.to_numeric(tx[c_cost], errors="coerce")
    tx["Qty_Ticket"] = pd.to_numeric(tx[c_qty], errors="coerce")

    tx["Estimated_Net_Margin_HT"] = tx["CA_HT"] - tx["Purch_Total_HT"]
    fact = tx[[
        "TransactionID","ValidationDate","month","OrganisationID","CustomerID",
        "is_client","CA_TTC","CA_HT","Purch_Total_HT","Qty_Ticket",
        "Estimated_Net_Margin_HT"
    ]].drop_duplicates("TransactionID")

    # =====================================================
    # 3️⃣ UPSERT DANS DUCKDB
    # =====================================================
    existing = con.execute("SELECT TransactionID FROM transactions").fetchdf()
    existing_ids = set(existing["TransactionID"]) if not existing.empty else set()
    new_tx = fact[~fact["TransactionID"].isin(existing_ids)]
    if not new_tx.empty:
        con.append("transactions", new_tx)
        st.success(f"✅ {len(new_tx)} nouvelles transactions ajoutées à l’historique.")
    else:
        st.info("ℹ️ Aucune nouvelle transaction à insérer.")

    # Coupons
    c_id = next((c for c in cp.columns if "coupon" in c.lower() and "id" in c.lower()), None)
    c_emit = next((c for c in cp.columns if "creation" in c.lower() or "emiss" in c.lower()), None)
    c_use = next((c for c in cp.columns if "usedate" in c.lower() or "utilisation" in c.lower()), None)
    c_valinit = next((c for c in cp.columns if "initial" in c.lower()), None)
    c_valrem = next((c for c in cp.columns if "remain" in c.lower() or "rest" in c.lower()), None)
    c_orgc = next((c for c in cp.columns if "organisation" in c.lower()), None)

    cp["UseDate"] = _ensure_date(cp[c_use])
    cp["EmissionDate"] = _ensure_date(cp[c_emit])
    cp["month_use"] = _month_str(cp["UseDate"])
    cp["month_emit"] = _month_str(cp["EmissionDate"])
    cp["CouponID"] = cp[c_id].astype(str)
    cp["OrganisationID"] = cp[c_orgc].astype(str)
    cp["Amount_Initial"] = pd.to_numeric(cp[c_valinit], errors="coerce")
    cp["Amount_Remaining"] = pd.to_numeric(cp[c_valrem], errors="coerce")
    cp["Value_Used_Line"] = (cp["Amount_Initial"] - cp["Amount_Remaining"]).clip(lower=0)

    existing_cp = con.execute("SELECT CouponID FROM coupons").fetchdf()
    existing_cp_ids = set(existing_cp["CouponID"]) if not existing_cp.empty else set()
    new_cp = cp[~cp["CouponID"].isin(existing_cp_ids)]
    if not new_cp.empty:
        con.append("coupons", new_cp)
        st.success(f"✅ {len(new_cp)} nouveaux coupons ajoutés à l’historique.")
    else:
        st.info("ℹ️ Aucun nouveau coupon à insérer.")

    # =====================================================
    # 4️⃣ KPI MENSUELS
    # =====================================================
    df_tx = con.execute("SELECT * FROM transactions").fetchdf()
    df_cp = con.execute("SELECT * FROM coupons").fetchdf()

    # Base mensuelle
    base = df_tx.groupby(["month","OrganisationID"]).agg(
        CA_TTC=("CA_TTC","sum"),
        CA_HT=("CA_HT","sum"),
        CA_paid_with_coupons=("CA_paid_with_coupons_HT","sum"),
        Marge_net_HT_avant_coupon=("Estimated_Net_Margin_HT","sum"),
        Transaction=("TransactionID","nunique"),
        Transaction_associe_client=("is_client","sum"),
        Client=("CustomerID","nunique")
    ).reset_index()

    # Coupons
    coupons_used = df_cp.dropna(subset=["UseDate"]).groupby(["month_use","OrganisationID"]).agg(
        Coupon_utilise=("CouponID","nunique"),
        Montant_coupons_utilise=("Value_Used_Line","sum")
    ).rename(columns={"month_use":"month"}).reset_index()

    coupons_emis = df_cp.dropna(subset=["EmissionDate"]).groupby(["month_emit","OrganisationID"]).agg(
        Coupon_emis=("CouponID","nunique"),
        Montant_coupons_emis=("Amount_Initial","sum")
    ).rename(columns={"month_emit":"month"}).reset_index()

    kpi = base.merge(coupons_used, on=["month","OrganisationID"], how="left") \
              .merge(coupons_emis, on=["month","OrganisationID"], how="left")

    kpi["Marge_net_HT_apres_coupon"] = kpi["Marge_net_HT_avant_coupon"] - kpi["Montant_coupons_utilise"].fillna(0)
    kpi["Taux_marge_HT_avant_coupon"] = kpi["Marge_net_HT_avant_coupon"] / kpi["CA_HT"]
    kpi["Taux_marge_HT_apres_coupon"] = kpi["Marge_net_HT_apres_coupon"] / kpi["CA_HT"]

    kpi["Taux_association_client"] = kpi["Transaction_associe_client"] / kpi["Transaction"]
    kpi["Taux_utilisation_bons_montant"] = kpi["Montant_coupons_utilise"] / kpi["Montant_coupons_emis"]
    kpi["Taux_CA_genere_par_bons_sur_CA_HT"] = kpi["CA_paid_with_coupons"] / kpi["CA_HT"]

    kpi["Panier_moyen_HT"] = kpi["CA_HT"] / kpi["Transaction"]
    kpi["Date"] = pd.to_datetime(kpi["month"]).dt.strftime("%d/%m/%Y")

    st.subheader("📊 Aperçu du KPI mensuel")
    st.dataframe(kpi.head(20))

else:
    st.info("➡️ Importez les fichiers Transactions et Coupons pour démarrer.")
