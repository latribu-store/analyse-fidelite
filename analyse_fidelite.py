import streamlit as st
import pandas as pd
import numpy as np
import os

# ============================================================
# CONFIG
# ============================================================
st.set_page_config(page_title="🎯 Analyse Fidélité - Keyneo AutoMap (Local)", layout="wide")
st.title("🎯 Analyse Fidélité - AutoMapping Keyneo ➜ KPI mensuels")

DATA_DIR = "data"
TX_PATH = os.path.join(DATA_DIR, "transactions.parquet")
CP_PATH = os.path.join(DATA_DIR, "coupons.parquet")

# ============================================================
# HELPERS
# ============================================================
def _ensure_date(s):
    return pd.to_datetime(s, errors="coerce")

def _month_str(s):
    return _ensure_date(s).dt.to_period("M").astype(str)

def read_csv(uploaded):
    df = pd.read_csv(uploaded, sep=";", encoding="utf-8-sig", on_bad_lines="skip", dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]
    return df

def ensure_data_dir():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

def load_parquet(path, columns):
    if os.path.exists(path):
        try:
            df = pd.read_parquet(path)
        except Exception:
            df = pd.DataFrame(columns=columns)
    else:
        df = pd.DataFrame(columns=columns)
    return df

def save_parquet(df, path):
    df.to_parquet(path, index=False)

def pick(df, *cands):
    for c in cands:
        c_clean = c.lower()
        if c_clean in df.columns:
            return c_clean
    return None

# ============================================================
# SCHEMA
# ============================================================
TX_COLS = [
    "TransactionID","ValidationDate","OrganisationID","CustomerID",
    "ProductID","Label","CA_TTC","CA_HT","Purch_Total_HT","Qty_Ticket"
]

CP_COLS = [
    "CouponID","OrganisationID","EmissionDate","UseDate",
    "Amount_Initial","Amount_Remaining","Value_Used_Line"
]

# ============================================================
# UI
# ============================================================
st.sidebar.header("📂 Importer les fichiers")
file_tx = st.sidebar.file_uploader("Transactions (CSV Keyneo)", type=["csv"])
file_cp = st.sidebar.file_uploader("Coupons (CSV Keyneo)", type=["csv"])

ensure_data_dir()

# ============================================================
# PIPELINE
# ============================================================
if file_tx and file_cp:
    # 1️⃣ Lecture fichiers CSV
    tx = read_csv(file_tx)
    cp = read_csv(file_cp)

    # 2️⃣ Chargement historique local
    hist_tx = load_parquet(TX_PATH, TX_COLS)
    hist_cp = load_parquet(CP_PATH, CP_COLS)

    # 3️⃣ Mapping automatique Keyneo
    def safe_pick(df, *cands):
        for c in cands:
            c_clean = c.lower()
            if c_clean in df.columns:
                return c_clean
        return None

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

    # 🪄 Debug visuel dans l'app
    st.write("🧩 Colonnes détectées (transactions):", map_tx)

    for k, v in map_tx.items():
        tx[k] = tx[v] if v and v in tx.columns else ""

    # Conversion types
    tx["ValidationDate"] = _ensure_date(tx["ValidationDate"])
    for col in ["CA_TTC","CA_HT","Purch_Total_HT","Qty_Ticket"]:
        tx[col] = pd.to_numeric(tx[col], errors="coerce").fillna(0.0)

    tx["Estimated_Net_Margin_HT"] = tx["CA_HT"] - tx["Purch_Total_HT"]
    tx["month"] = _month_str(tx["ValidationDate"])

    # 4️⃣ Mapping automatique Coupons
    map_cp = {
        "CouponID": safe_pick(cp, "couponid", "id"),
        "OrganisationID": safe_pick(cp, "organisationid", "organizationid"),
        "EmissionDate": safe_pick(cp, "creationdate", "issuedate"),
        "UseDate": safe_pick(cp, "usedate", "validationdate"),
        "Amount_Initial": safe_pick(cp, "initialvalue", "amount", "value"),
        "Amount_Remaining": safe_pick(cp, "amountremaining", "restant", "reste"),
    }

    st.write("🧾 Colonnes détectées (coupons):", map_cp)

    for k, v in map_cp.items():
        cp[k] = cp[v] if v and v in cp.columns else ""


    # 5️⃣ Append-only sans doublons
    tx["TransactionID"] = tx["TransactionID"].astype(str)
    cp["CouponID"] = cp["CouponID"].astype(str)
    hist_tx["TransactionID"] = hist_tx["TransactionID"].astype(str)
    hist_cp["CouponID"] = hist_cp["CouponID"].astype(str)

    new_tx = tx[~tx["TransactionID"].isin(hist_tx["TransactionID"])]
    new_cp = cp[~cp["CouponID"].isin(hist_cp["CouponID"])]

    hist_tx = pd.concat([hist_tx, new_tx], ignore_index=True)
    hist_cp = pd.concat([hist_cp, new_cp], ignore_index=True)

    save_parquet(hist_tx, TX_PATH)
    save_parquet(hist_cp, CP_PATH)

    st.success(f"✅ {len(new_tx)} nouvelles transactions et {len(new_cp)} nouveaux coupons ajoutés à l’historique local.")

    # ======================================================
    # 6️⃣ KPI Mensuel
    # ======================================================
    df_tx = hist_tx.copy()
    df_cp = hist_cp.copy()

    if df_tx.empty:
        st.warning("⚠️ Pas de données transactionnelles disponibles.")
        st.stop()

    base = df_tx.groupby(["month","OrganisationID"]).agg(
        CA_TTC=("CA_TTC","sum"),
        CA_HT=("CA_HT","sum"),
        Marge_net_HT_avant_coupon=("Estimated_Net_Margin_HT","sum"),
        Transactions=("TransactionID","nunique"),
        Clients=("CustomerID","nunique"),
        Qty_total=("Qty_Ticket","sum")
    ).reset_index()

    if not df_cp.empty:
        df_cp["month_use"] = _month_str(df_cp["UseDate"])
        df_cp["month_emit"] = _month_str(df_cp["EmissionDate"])

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

    # Harmonisation des types avant merge
    for df in [base, coupons_used, coupons_emis]:
        for col in ["month", "OrganisationID"]:
            if col not in df.columns:
                df[col] = ""
            df[col] = df[col].astype(str).fillna("")

    kpi = (base.merge(coupons_used, on=["month","OrganisationID"], how="left")
                .merge(coupons_emis, on=["month","OrganisationID"], how="left"))

    # Calculs complémentaires
    kpi["Marge_net_HT_apres_coupon"] = kpi["Marge_net_HT_avant_coupon"] - kpi["Montant_coupons_utilise"].fillna(0)
    kpi["Taux_marge_HT_avant_coupon"] = np.where(kpi["CA_HT"]>0, kpi["Marge_net_HT_avant_coupon"]/kpi["CA_HT"], np.nan)
    kpi["Taux_marge_HT_apres_coupon"] = np.where(kpi["CA_HT"]>0, kpi["Marge_net_HT_apres_coupon"]/kpi["CA_HT"], np.nan)
    kpi["Taux_utilisation_bons_montant"] = np.where(kpi["Montant_coupons_emis"]>0,
                                                    kpi["Montant_coupons_utilise"]/kpi["Montant_coupons_emis"], np.nan)
    kpi["Panier_moyen_HT"] = np.where(kpi["Transactions"]>0, kpi["CA_HT"]/kpi["Transactions"], np.nan)
    kpi["Prix_moyen_article_vendu_HT"] = np.where(kpi["Qty_total"]>0, kpi["CA_HT"]/kpi["Qty_total"], np.nan)
    kpi["Date"] = pd.to_datetime(kpi["month"], errors="coerce").dt.strftime("%d/%m/%Y")

    # Nettoyage final
    kpi = kpi.fillna("")

    st.subheader("📊 Aperçu KPI mensuel")
    st.dataframe(kpi.head(20))

else:
    st.info("➡️ Importez les fichiers Transactions et Coupons pour démarrer.")
