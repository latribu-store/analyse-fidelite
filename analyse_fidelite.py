import streamlit as st
import pandas as pd
import numpy as np
import duckdb
import os

# ============================================================
# CONFIG
# ============================================================
st.set_page_config(page_title="🎯 Analyse Fidélité - Version stable (DuckDB)", layout="wide")
st.title("🎯 Analyse Fidélité - Version stable (historique DuckDB + KPI mensuel)")

DUCKDB_PATH = "historique.duckdb"

# ============================================================
# HELPERS
# ============================================================
def _ensure_date(s):
    return pd.to_datetime(s, errors="coerce")

def _month_str(s):
    return _ensure_date(s).dt.to_period("M").astype(str)

def read_csv(uploaded):
    df = pd.read_csv(uploaded, sep=";", encoding="utf-8-sig", on_bad_lines="skip", dtype=str)
    df.columns = [c.strip() for c in df.columns]
    return df

# ============================================================
# INIT DUCKDB
# ============================================================
def init_duckdb():
    if os.path.exists(DUCKDB_PATH):
        try:
            con_tmp = duckdb.connect(DUCKDB_PATH)
            cols = [r[0] for r in con_tmp.execute("PRAGMA table_info('transactions')").fetchall()]
            if "TransactionID" not in cols:
                os.remove(DUCKDB_PATH)
                st.warning("⚠️ Fichier DuckDB corrompu — recréé automatiquement.")
        except Exception:
            os.remove(DUCKDB_PATH)
            st.warning("⚠️ Fichier DuckDB supprimé et recréé (erreur d’ouverture).")

    con = duckdb.connect(DUCKDB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            TransactionID TEXT,
            ValidationDate TIMESTAMP,
            OrganisationID TEXT,
            CustomerID TEXT,
            ProductID TEXT,
            Label TEXT,
            CA_TTC DOUBLE,
            CA_HT DOUBLE,
            Purch_Total_HT DOUBLE,
            Qty_Ticket DOUBLE
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS coupons (
            CouponID TEXT,
            OrganisationID TEXT,
            EmissionDate TIMESTAMP,
            UseDate TIMESTAMP,
            Amount_Initial DOUBLE,
            Amount_Remaining DOUBLE,
            Value_Used_Line DOUBLE
        );
    """)
    return con

con = init_duckdb()

# ============================================================
# UI IMPORT
# ============================================================
st.sidebar.header("📂 Importer les fichiers")
file_tx = st.sidebar.file_uploader("Transactions (CSV Keyneo)", type=["csv"])
file_cp = st.sidebar.file_uploader("Coupons (CSV Keyneo)", type=["csv"])

# ============================================================
# PIPELINE PRINCIPAL
# ============================================================
if file_tx and file_cp:

    # ------------------------
    # 1️⃣ LECTURE & PREP TX
    # ------------------------
    tx = read_csv(file_tx)
    cp = read_csv(file_cp)

    # Colonnes obligatoires (on force la présence)
    needed_tx = [
        "TransactionID", "ValidationDate", "OrganisationID", "CustomerID",
        "ProductID", "Label", "CA_TTC", "CA_HT", "Purch_Total_HT", "Qty_Ticket"
    ]
    for c in needed_tx:
        if c not in tx.columns:
            tx[c] = ""

    # Conversion typée
    tx = tx[needed_tx].copy()
    tx["ValidationDate"] = _ensure_date(tx["ValidationDate"])
    tx["CA_TTC"] = pd.to_numeric(tx["CA_TTC"], errors="coerce").fillna(0.0)
    tx["CA_HT"] = pd.to_numeric(tx["CA_HT"], errors="coerce").fillna(0.0)
    tx["Purch_Total_HT"] = pd.to_numeric(tx["Purch_Total_HT"], errors="coerce").fillna(0.0)
    tx["Qty_Ticket"] = pd.to_numeric(tx["Qty_Ticket"], errors="coerce").fillna(0.0)
    tx["month"] = _month_str(tx["ValidationDate"])
    tx["Estimated_Net_Margin_HT"] = tx["CA_HT"] - tx["Purch_Total_HT"]

    # ------------------------
    # 2️⃣ UPSERT TRANSACTIONS
    # ------------------------
    existing_ids = set(con.execute("SELECT TransactionID FROM transactions").fetchdf()["TransactionID"].astype(str))
    new_tx = tx[~tx["TransactionID"].astype(str).isin(existing_ids)].copy()

    if not new_tx.empty:
        new_tx = new_tx.fillna("")
        for col in ["CA_TTC", "CA_HT", "Purch_Total_HT", "Qty_Ticket"]:
            new_tx[col] = pd.to_numeric(new_tx[col], errors="coerce").fillna(0.0)
        new_tx["ValidationDate"] = _ensure_date(new_tx["ValidationDate"]).fillna(pd.Timestamp("1970-01-01"))

        con.register("temp_tx", new_tx)
        con.execute("INSERT INTO transactions SELECT * FROM temp_tx")
        con.unregister("temp_tx")

        st.success(f"✅ {len(new_tx)} nouvelles transactions ajoutées à l’historique.")
    else:
        st.info("ℹ️ Aucune nouvelle transaction à insérer.")

    # ------------------------
    # 3️⃣ UPSERT COUPONS
    # ------------------------
    needed_cp = [
        "CouponID", "OrganisationID", "EmissionDate", "UseDate",
        "Amount_Initial", "Amount_Remaining"
    ]
    for c in needed_cp:
        if c not in cp.columns:
            cp[c] = ""

    cp = cp[needed_cp].copy()
    cp["EmissionDate"] = _ensure_date(cp["EmissionDate"])
    cp["UseDate"] = _ensure_date(cp["UseDate"])
    cp["Amount_Initial"] = pd.to_numeric(cp["Amount_Initial"], errors="coerce").fillna(0.0)
    cp["Amount_Remaining"] = pd.to_numeric(cp["Amount_Remaining"], errors="coerce").fillna(0.0)
    cp["Value_Used_Line"] = (cp["Amount_Initial"] - cp["Amount_Remaining"]).clip(lower=0.0)

    existing_cp_ids = set(con.execute("SELECT CouponID FROM coupons").fetchdf()["CouponID"].astype(str))
    new_cp = cp[~cp["CouponID"].astype(str).isin(existing_cp_ids)].copy()

    if not new_cp.empty:
        con.register("temp_cp", new_cp)
        con.execute("INSERT INTO coupons SELECT * FROM temp_cp")
        con.unregister("temp_cp")
        st.success(f"✅ {len(new_cp)} nouveaux coupons ajoutés à l’historique.")
    else:
        st.info("ℹ️ Aucun nouveau coupon à insérer.")

    # ------------------------
    # 4️⃣ KPI MENSUEL
    # ------------------------
    df_tx = con.execute("SELECT * FROM transactions").fetchdf()
    df_cp = con.execute("SELECT * FROM coupons").fetchdf()

    if df_tx.empty:
        st.warning("⚠️ Pas de transactions en base.")
        st.stop()

    df_tx["month"] = _month_str(df_tx["ValidationDate"])

    base = df_tx.groupby(["month", "OrganisationID"]).agg(
        CA_TTC=("CA_TTC", "sum"),
        CA_HT=("CA_HT", "sum"),
        Marge_net_HT_avant_coupon=("CA_HT", "sum"),
        Transactions=("TransactionID", "nunique"),
        Clients=("CustomerID", "nunique"),
        Qty_total=("Qty_Ticket", "sum")
    ).reset_index()

    if not df_cp.empty:
        df_cp["month_use"] = _month_str(df_cp["UseDate"])
        df_cp["month_emit"] = _month_str(df_cp["EmissionDate"])

        coupons_used = df_cp.groupby(["month_use", "OrganisationID"]).agg(
            Coupons_utilise=("CouponID", "nunique"),
            Montant_coupons_utilise=("Value_Used_Line", "sum")
        ).rename(columns={"month_use": "month"}).reset_index()

        coupons_emis = df_cp.groupby(["month_emit", "OrganisationID"]).agg(
            Coupons_emis=("CouponID", "nunique"),
            Montant_coupons_emis=("Amount_Initial", "sum")
        ).rename(columns={"month_emit": "month"}).reset_index()
    else:
        coupons_used = pd.DataFrame(columns=["month", "OrganisationID", "Coupons_utilise", "Montant_coupons_utilise"])
        coupons_emis = pd.DataFrame(columns=["month", "OrganisationID", "Coupons_emis", "Montant_coupons_emis"])

    kpi = (base.merge(coupons_used, on=["month", "OrganisationID"], how="left")
                .merge(coupons_emis, on=["month", "OrganisationID"], how="left"))

    kpi["Marge_net_HT_apres_coupon"] = kpi["Marge_net_HT_avant_coupon"] - kpi["Montant_coupons_utilise"].fillna(0)
    kpi["Taux_marge_HT_avant_coupon"] = np.where(kpi["CA_HT"]>0, kpi["Marge_net_HT_avant_coupon"]/kpi["CA_HT"], np.nan)
    kpi["Taux_marge_HT_apres_coupon"] = np.where(kpi["CA_HT"]>0, kpi["Marge_net_HT_apres_coupon"]/kpi["CA_HT"], np.nan)
    kpi["Taux_utilisation_bons_montant"] = np.where(kpi["Montant_coupons_emis"]>0,
                                                    kpi["Montant_coupons_utilise"]/kpi["Montant_coupons_emis"], np.nan)
    kpi["Panier_moyen_HT"] = np.where(kpi["Transactions"]>0, kpi["CA_HT"]/kpi["Transactions"], np.nan)
    kpi["Prix_moyen_article_vendu_HT"] = np.where(kpi["Qty_total"]>0, kpi["CA_HT"]/kpi["Qty_total"], np.nan)
    kpi["Date"] = pd.to_datetime(kpi["month"]).dt.strftime("%d/%m/%Y")

    st.subheader("📊 Aperçu KPI mensuel")
    st.dataframe(kpi.head(20))

else:
    st.info("➡️ Importez les fichiers Transactions et Coupons pour démarrer.")
