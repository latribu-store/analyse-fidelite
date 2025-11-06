import streamlit as st
import pandas as pd
import numpy as np
import duckdb
import os

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
st.set_page_config(page_title="🎯 Analyse Fidélité — Historique DuckDB ➜ KPI", layout="wide")
st.title("🎯 Analyse Fidélité — Historique DuckDB ➜ KPI mensuels")

DUCKDB_PATH = "historique.duckdb"

# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------
def _ensure_date(s): 
    return pd.to_datetime(s, errors="coerce")

def _month_str(s): 
    return _ensure_date(s).dt.to_period("M").astype(str)

def read_csv(uploaded):
    df = pd.read_csv(uploaded, sep=";", encoding="utf-8-sig", on_bad_lines="skip")
    df.columns = [c.strip() for c in df.columns]
    return df

def pick_col(df, *candidates_lower):
    lc_map = {c.lower(): c for c in df.columns}
    for cand in candidates_lower:
        if cand in lc_map:
            return lc_map[cand]
    return None

# ------------------------------------------------------------
# INIT DUCKDB
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

    # Colonnes Transactions (Keyneo)
    c_txid  = pick_col(tx, "operationid", "ticketnumber", "transactionid")
    c_date  = pick_col(tx, "validationdate", "operationdate", "date")
    c_org   = pick_col(tx, "organisationid", "organizationid")
    c_cust  = pick_col(tx, "customerid", "clientid")
    c_caht  = pick_col(tx, "linegrossamount", "montanthtligne", "cahtligne")
    c_cost  = pick_col(tx, "linetotalpurchasingamount", "linetotalpurchasingamount")
    c_qty   = pick_col(tx, "quantity", "qty", "linequantity")
    c_cattc = pick_col(tx, "totalamountttc", "totalamount", "totaltcc", "totalttc")

    if not c_txid or not c_date or not c_org or not c_caht or not c_cost or not c_cattc:
        st.error("❌ Colonnes manquantes dans Transactions.\n"
                 f"Colonnes reçues : {list(tx.columns)}")
        st.stop()

    # Normalisation
    tx["TransactionID"]   = tx[c_txid].astype(str)
    tx["ValidationDate"]  = _ensure_date(tx[c_date])
    tx["OrganisationID"]  = tx[c_org].astype(str)
    tx["CustomerID"]      = tx[c_cust].astype(str) if c_cust else ""
    tx["month"]           = _month_str(tx["ValidationDate"])
    tx["CA_HT"]           = pd.to_numeric(tx[c_caht], errors="coerce").fillna(0.0)
    tx["Purch_Total_HT"]  = pd.to_numeric(tx[c_cost], errors="coerce").fillna(0.0)
    tx["CA_TTC"]          = pd.to_numeric(tx[c_cattc], errors="coerce").fillna(0.0)
    tx["Qty_Ticket"]      = pd.to_numeric(tx[c_qty], errors="coerce").fillna(0.0) if c_qty else 0.0
    tx["Estimated_Net_Margin_HT"] = tx["CA_HT"] - tx["Purch_Total_HT"]

    fact = tx[[
        "TransactionID","ValidationDate","month","OrganisationID","CustomerID",
        "CA_TTC","CA_HT","Purch_Total_HT","Qty_Ticket","Estimated_Net_Margin_HT"
    ]].drop_duplicates("TransactionID")

    # =====================================================
    # 2️⃣ UPSERT TRANSACTIONS (ROBUSTE)
    # =====================================================
    existing = con.execute("SELECT TransactionID FROM transactions").fetchdf()
    existing_ids = set(existing["TransactionID"]) if not existing.empty else set()
    new_tx = fact[~fact["TransactionID"].isin(existing_ids)]

    if not new_tx.empty:
        schema_db = [r[0] for r in con.execute("PRAGMA table_info('transactions')").fetchall()]
        for col in schema_db:
            if col not in new_tx.columns:
                new_tx[col] = np.nan
        new_tx = new_tx[schema_db]
        new_tx = new_tx.astype({
            "TransactionID": "string",
            "ValidationDate": "datetime64[ns]",
            "month": "string",
            "OrganisationID": "string",
            "CustomerID": "string",
            "CA_TTC": "float64",
            "CA_HT": "float64",
            "Purch_Total_HT": "float64",
            "Qty_Ticket": "float64",
            "Estimated_Net_Margin_HT": "float64",
        }, errors="ignore")
        con.append("transactions", new_tx)
        st.success(f"✅ {len(new_tx)} nouvelles transactions ajoutées.")
    else:
        st.info("ℹ️ Aucune nouvelle transaction à insérer.")

    # =====================================================
    # 2️⃣ UPSERT TRANSACTIONS (ROBUSTE & TOLÉRANT)
    # =====================================================
    existing = con.execute("SELECT TransactionID FROM transactions").fetchdf()
    existing_ids = set(existing["TransactionID"]) if not existing.empty else set()
    new_tx = fact[~fact["TransactionID"].isin(existing_ids)]

    if not new_tx.empty:
        # Harmonise le schéma selon la table DuckDB
        schema_db = [r[0] for r in con.execute("PRAGMA table_info('transactions')").fetchall()]
        for col in schema_db:
            if col not in new_tx.columns:
                new_tx[col] = np.nan

        # Réordonne les colonnes selon la table
        new_tx = new_tx[[c for c in schema_db if c in new_tx.columns]]

        # Typage conditionnel (ne crash pas si colonne absente)
        type_map = {
            "TransactionID": "string",
            "ValidationDate": "datetime64[ns]",
            "month": "string",
            "OrganisationID": "string",
            "CustomerID": "string",
            "CA_TTC": "float64",
            "CA_HT": "float64",
            "Purch_Total_HT": "float64",
            "Qty_Ticket": "float64",
            "Estimated_Net_Margin_HT": "float64",
        }
        for col, t in type_map.items():
            if col in new_tx.columns:
                try:
                    new_tx[col] = new_tx[col].astype(t)
                except Exception:
                    pass  # On ignore les erreurs de conversion mineures

        con.append("transactions", new_tx)
        st.success(f"✅ {len(new_tx)} nouvelles transactions ajoutées.")
    else:
        st.info("ℹ️ Aucune nouvelle transaction à insérer.")


    # =====================================================
    # 4️⃣ KPI MENSUELS
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

    if not df_cp.empty:
        coupons_used = df_cp.dropna(subset=["UseDate"]).groupby(["month_use","OrganisationID"]).agg(
            Coupons_utilise=("CouponID","nunique"),
            Montant_coupons_utilise=("Value_Used_Line","sum")
        ).rename(columns={"month_use":"month"}).reset_index()

        coupons_emis = df_cp.dropna(subset=["EmissionDate"]).groupby(["month_emit","OrganisationID"]).agg(
            Coupons_emis=("CouponID","nunique"),
            Montant_coupons_emis=("Amount_Initial","sum")
        ).rename(columns={"month_emit":"month"}).reset_index()
    else:
        coupons_used = pd.DataFrame(columns=["month","OrganisationID","Coupons_utilise","Montant_coupons_utilise"])
        coupons_emis = pd.DataFrame(columns=["month","OrganisationID","Coupons_emis","Montant_coupons_emis"])

    kpi = (base.merge(coupons_used, on=["month","OrganisationID"], how="left")
                .merge(coupons_emis, on=["month","OrganisationID"], how="left"))

    kpi["Marge_net_HT_apres_coupon"] = kpi["Marge_net_HT_avant_coupon"] - kpi["Montant_coupons_utilise"].fillna(0)
    kpi["Taux_marge_HT_avant_coupon"] = np.where(kpi["CA_HT"]>0, kpi["Marge_net_HT_avant_coupon"]/kpi["CA_HT"], np.nan)
    kpi["Taux_marge_HT_apres_coupon"] = np.where(kpi["CA_HT"]>0, kpi["Marge_net_HT_apres_coupon"]/kpi["CA_HT"], np.nan)
    kpi["Taux_utilisation_bons_montant"] = np.where(kpi["Montant_coupons_emis"]>0,
                                                    kpi["Montant_coupons_utilise"]/kpi["Montant_coupons_emis"], np.nan)
    kpi["Taux_CA_genere_par_bons_sur_CA_HT"] = np.where(kpi["CA_HT"]>0,
                                                        kpi["Montant_coupons_utilise"]/kpi["CA_HT"], np.nan)
    kpi["Panier_moyen_HT"] = np.where(kpi["Transactions"]>0, kpi["CA_HT"]/kpi["Transactions"], np.nan)
    kpi["Prix_moyen_article_vendu_HT"] = np.where(kpi["Qty_total"]>0, kpi["CA_HT"]/kpi["Qty_total"], np.nan)
    kpi["Date"] = pd.to_datetime(kpi["month"]).dt.strftime("%d/%m/%Y")

    st.subheader("📊 Aperçu KPI mensuel")
    st.dataframe(kpi.head(20))

else:
    st.info("➡️ Importez les fichiers Transactions et Coupons pour démarrer.")
