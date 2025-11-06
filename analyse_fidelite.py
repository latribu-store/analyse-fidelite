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

    # =====================================================
    # 2️⃣ UPSERT TRANSACTIONS (FIX FINAL)
    # =====================================================
    existing_tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    tx_cols = [
        "TransactionID","ValidationDate","OrganisationID","CustomerID",
        "ProductID","Label","CA_TTC","CA_HT","Purch_Total_HT","Qty_Ticket"
    ]

    # Nettoyage et typage strict
    new_tx = tx[tx_cols].copy()
    new_tx["ValidationDate"] = _ensure_date(new_tx["ValidationDate"]).fillna(pd.Timestamp("1970-01-01"))
    for c in ["CA_TTC","CA_HT","Purch_Total_HT","Qty_Ticket"]:
        new_tx[c] = pd.to_numeric(new_tx[c], errors="coerce").fillna(0.0)
    for c in ["TransactionID","OrganisationID","CustomerID","ProductID","Label"]:
        new_tx[c] = new_tx[c].astype(str).fillna("")

    # ✅ Étape sûre : créer une vue temporaire DuckDB
    con.register("temp_tx", new_tx)

    if "transactions" not in existing_tables:
        con.execute("CREATE TABLE transactions AS SELECT * FROM temp_tx")
        st.success(f"✅ Table 'transactions' créée ({len(new_tx)} lignes).")
    else:
        # Vérifie les colonnes existantes
        existing_cols = [r[0] for r in con.execute("PRAGMA table_info('transactions')").fetchall()]
        common_cols = [c for c in tx_cols if c in existing_cols]
        insert_sql = f"""
            INSERT INTO transactions ({', '.join(common_cols)})
            SELECT {', '.join(common_cols)} FROM temp_tx
        """
        con.execute(insert_sql)
        st.success(f"✅ {len(new_tx)} nouvelles transactions insérées.")

    con.unregister("temp_tx")


    # =====================================================
    # 3️⃣ UPSERT COUPONS (FIX FINAL)
    # =====================================================
    existing_tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    cp_cols = [
        "CouponID","OrganisationID","EmissionDate","UseDate",
        "Amount_Initial","Amount_Remaining","Value_Used_Line"
    ]

    new_cp = cp[cp_cols].copy()
    new_cp["EmissionDate"] = _ensure_date(new_cp["EmissionDate"]).fillna(pd.Timestamp("1970-01-01"))
    new_cp["UseDate"] = _ensure_date(new_cp["UseDate"]).fillna(pd.Timestamp("1970-01-01"))
    for c in ["Amount_Initial","Amount_Remaining","Value_Used_Line"]:
        new_cp[c] = pd.to_numeric(new_cp[c], errors="coerce").fillna(0.0)
    for c in ["CouponID","OrganisationID"]:
        new_cp[c] = new_cp[c].astype(str).fillna("")

    con.register("temp_cp", new_cp)

    if "coupons" not in existing_tables:
        con.execute("CREATE TABLE coupons AS SELECT * FROM temp_cp")
        st.success(f"✅ Table 'coupons' créée ({len(new_cp)} lignes).")
    else:
        existing_cols = [r[0] for r in con.execute("PRAGMA table_info('coupons')").fetchall()]
        common_cols = [c for c in cp_cols if c in existing_cols]
        insert_sql = f"""
            INSERT INTO coupons ({', '.join(common_cols)})
            SELECT {', '.join(common_cols)} FROM temp_cp
        """
        con.execute(insert_sql)
        st.success(f"✅ {len(new_cp)} nouveaux coupons insérés.")

    con.unregister("temp_cp")


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
