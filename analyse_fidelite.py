import streamlit as st
import pandas as pd
import numpy as np
import duckdb
import os, traceback, sys
from datetime import datetime

# =====================================================================
# 🚑 Mode debug : capture les erreurs Python et les affiche dans Streamlit
# =====================================================================
def safe_run(fn):
    try:
        fn()
    except Exception:
        st.error("❌ Une erreur est survenue :")
        st.code("".join(traceback.format_exception(*sys.exc_info())), language="python")

# =====================================================================
# ⚙️ Configuration
# =====================================================================
st.set_page_config(page_title="Analyse Fidélité — Version debug", layout="wide")
st.title("🎯 Analyse Fidélité — Mode Debug (avec historique DuckDB)")

DB_PATH = "./data/transactions.parquet"
os.makedirs("data", exist_ok=True)

# =====================================================================
# 🔧 Fonctions utilitaires
# =====================================================================
def _ensure_date(s): return pd.to_datetime(s, errors="coerce")
def _month_str(s): return _ensure_date(s).dt.to_period("M").astype(str)

def safe_pick(df, *cands):
    for c in cands:
        if c.lower() in df.columns:
            return c.lower()
    return None

# =====================================================================
# 🚀 Application principale
# =====================================================================
def main():
    st.sidebar.header("📂 Importer les fichiers Keyneo")
    file_tx = st.sidebar.file_uploader("Transactions (CSV)", type=["csv"])
    file_cp = st.sidebar.file_uploader("Coupons (CSV)", type=["csv"])

    if not file_tx or not file_cp:
        st.info("➡️ Importez les fichiers Transactions et Coupons pour commencer.")
        return

    # -----------------------------------------------------------------
    # 1️⃣ Lecture et normalisation
    # -----------------------------------------------------------------
    def read_csv(uploaded):
        df = pd.read_csv(uploaded, sep=";", encoding="utf-8-sig", dtype=str, on_bad_lines="skip")
        df.columns = [c.lower().strip() for c in df.columns]
        return df

    tx = read_csv(file_tx)
    cp = read_csv(file_cp)

    st.write("🧩 Aperçu Transactions :", tx.head(3))
    st.write("🧾 Aperçu Coupons :", cp.head(3))

    # -----------------------------------------------------------------
    # 2️⃣ Mapping des colonnes Keyneo
    # -----------------------------------------------------------------
    map_tx = {
        "TransactionID": safe_pick(tx, "ticketnumber", "transactionid", "operationid"),
        "ValidationDate": safe_pick(tx, "validationdate", "operationdate"),
        "OrganisationID": safe_pick(tx, "organisationid", "organizationid"),
        "CustomerID": safe_pick(tx, "customerid", "clientid"),
        "ProductID": safe_pick(tx, "productid", "sku", "ean"),
        "Label": safe_pick(tx, "label", "designation"),
        "CA_TTC": safe_pick(tx, "totalamount", "totalttc", "totaltcc"),
        "CA_HT": safe_pick(tx, "linegrossamount", "montanthtligne", "cahtligne"),
        "Purch_Total_HT": safe_pick(tx, "linetotalpurchasingamount", "purchasingamount", "costprice"),
        "Qty_Ticket": safe_pick(tx, "quantity", "qty", "linequantity")
    }
    map_cp = {
        "CouponID": safe_pick(cp, "couponid", "id"),
        "OrganisationID": safe_pick(cp, "organisationid", "organizationid"),
        "EmissionDate": safe_pick(cp, "creationdate", "issuedate"),
        "UseDate": safe_pick(cp, "usedate", "validationdate"),
        "Amount_Initial": safe_pick(cp, "initialvalue", "amount", "value"),
        "Amount_Remaining": safe_pick(cp, "amountremaining", "restant", "reste")
    }

    st.write("✅ Mapping Transactions :", map_tx)
    st.write("✅ Mapping Coupons :", map_cp)

    # -----------------------------------------------------------------
    # 3️⃣ Sélection utile et conversions
    # -----------------------------------------------------------------
    tx = tx[[v for v in map_tx.values() if v in tx.columns]].copy()
    cp = cp[[v for v in map_cp.values() if v in cp.columns]].copy()

    tx.rename(columns={v: k for k, v in map_tx.items() if v}, inplace=True)
    cp.rename(columns={v: k for k, v in map_cp.items() if v}, inplace=True)

    for col in ["CA_TTC","CA_HT","Purch_Total_HT","Qty_Ticket"]:
        if col in tx.columns:
            tx[col] = pd.to_numeric(tx[col], errors="coerce").fillna(0)

    tx["ValidationDate"] = _ensure_date(tx["ValidationDate"])
    tx["month"] = _month_str(tx["ValidationDate"])
    tx["Estimated_Net_Margin_HT"] = tx.get("CA_HT",0) - tx.get("Purch_Total_HT",0)

    cp["UseDate"] = _ensure_date(cp.get("UseDate"))
    cp["EmissionDate"] = _ensure_date(cp.get("EmissionDate"))
    cp["Value_Used"] = (pd.to_numeric(cp.get("Amount_Initial",0), errors="coerce") -
                        pd.to_numeric(cp.get("Amount_Remaining",0), errors="coerce")).clip(lower=0)
    cp["month_use"] = _month_str(cp["UseDate"])
    cp["month_emit"] = _month_str(cp["EmissionDate"])

    # -----------------------------------------------------------------
    # 4️⃣ Stockage local Parquet (historique)
    # -----------------------------------------------------------------
    if os.path.exists(DB_PATH):
        old = pd.read_parquet(DB_PATH)
        combined = pd.concat([old, tx]).drop_duplicates(subset=["TransactionID"], keep="last")
    else:
        combined = tx.copy()
    combined.to_parquet(DB_PATH, index=False)
    st.success(f"💾 Transactions stockées ({len(combined)} lignes).")

    # -----------------------------------------------------------------
    # 5️⃣ Calcul KPI mensuels
    # -----------------------------------------------------------------
    fact = combined.copy()
    base = (
        fact.groupby(["month","OrganisationID"], dropna=False)
        .agg(
            CA_TTC=("CA_TTC","sum"),
            CA_HT=("CA_HT","sum"),
            Marge_net_HT_avant_coupon=("Estimated_Net_Margin_HT","sum"),
            Transactions=("TransactionID","nunique"),
            Clients=("CustomerID","nunique"),
            Qty_total=("Qty_Ticket","sum")
        )
        .reset_index()
    )

    coupons_used = (
        cp.dropna(subset=["month_use"])
        .groupby(["month_use","OrganisationID"], dropna=False)
        .agg(Coupons_utilise=("CouponID","nunique"), Montant_coupons_utilise=("Value_Used","sum"))
        .reset_index().rename(columns={"month_use":"month"})
    )

    coupons_emis = (
        cp.dropna(subset=["month_emit"])
        .groupby(["month_emit","OrganisationID"], dropna=False)
        .agg(Coupons_emis=("CouponID","nunique"), Montant_coupons_emis=("Amount_Initial","sum"))
        .reset_index().rename(columns={"month_emit":"month"})
    )

    kpi = (
        base.merge(coupons_used, on=["month","OrganisationID"], how="left")
            .merge(coupons_emis, on=["month","OrganisationID"], how="left")
            .fillna("")
    )

    st.success("✅ KPI mensuels générés.")
    st.dataframe(kpi)

# =====================================================================
# 🏁 Lancement sécurisé
# =====================================================================
safe_run(main)
