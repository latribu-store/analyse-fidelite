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
    map_tx = {
        "TransactionID": pick(tx, "ticketnumber", "transactionid", "operationid"),
        "ValidationDate": pick(tx, "validationdate", "operationdate"),
        "OrganisationID": pick(tx, "organisationid", "organizationid"),
        "CustomerID": pick(tx, "customerid", "clientid"),
        "ProductID": pick(tx, "productid", "sku", "ean"),
        "Label": pick(tx, "label", "designation"),
        "CA_TTC": pick(tx, "totalamount", "totalttc", "totaltcc"),
        "CA_HT": pick(tx, "linegrossamount", "montanthtligne", "cahtligne"),
        "Purch_Total_HT": pick(tx, "linetotalpurchasingamount", "purchasingamount", "costprice"),
        "Qty_Ticket": pick(tx, "quantity", "qty", "linequantity")
    }

    for k,v in map_tx.items():
        tx[k] = tx[v] if v in tx.columns else ""

    # Conversion types
    tx["ValidationDate"] = _ensure_date(tx["ValidationDate"])
    for col in ["CA_TTC","CA_HT","Purch_Total_HT","Qty_Ticket"]:
        tx[col] = pd.to_numeric(tx[col], errors="coerce").fillna(0.0)

    tx["Estimated_Net_Margin_HT"] = tx["CA_HT"] - tx["Purch_Total_HT"]
    tx["month"] = _month_str(tx["ValidationDate"])

    # 4️⃣ Mapping automatique Coupons (version Keyneo corrigée)
    map_cp = {
        "CouponID": pick(cp, "couponid", "id"),
        "OrganisationID": pick(cp, "organisationid", "organizationid"),
        "EmissionDate": pick(cp, "creationdate", "issuedate"),
        "UseDate": pick(cp, "usedate", "validationdate"),
        "Amount_Initial": pick(cp, "initialvalue", "value", "montantinitial"),
        "Amount_Used": pick(cp, "amount", "valeurutilisee", "usedamount"),
    }

    for k, v in map_cp.items():
        cp[k] = cp[v] if v and v in cp.columns else ""

    cp["EmissionDate"] = _ensure_date(cp["EmissionDate"])
    cp["UseDate"] = _ensure_date(cp["UseDate"])
    cp["Amount_Initial"] = pd.to_numeric(cp["Amount_Initial"], errors="coerce").fillna(0.0)
    cp["Amount_Used"] = pd.to_numeric(cp["Amount_Used"], errors="coerce").fillna(0.0)

    # Montant utilisé = directement la colonne 'amount' (Keyneo)
    cp["Value_Used_Line"] = cp["Amount_Used"].clip(lower=0.0)

    cp["month_use"] = _month_str(cp["UseDate"])
    cp["month_emit"] = _month_str(cp["EmissionDate"])


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
    # 6️⃣ KPI Mensuel (corrigé)
    # ======================================================
    df_tx = hist_tx.copy()
    df_cp = hist_cp.copy()

    if df_tx.empty:
        st.warning("⚠️ Pas de données transactionnelles disponibles.")
        st.stop()

    # Nettoyage
    df_tx["ValidationDate"] = _ensure_date(df_tx["ValidationDate"])
    df_tx["month"] = _month_str(df_tx["ValidationDate"])
    for col in ["CA_TTC","CA_HT","Purch_Total_HT","Qty_Ticket"]:
        df_tx[col] = pd.to_numeric(df_tx[col], errors="coerce").fillna(0.0)
    df_tx["Label"] = df_tx["Label"].fillna("").astype(str)
    df_tx["CustomerID"] = df_tx["CustomerID"].fillna("").astype(str)
    df_tx["OrganisationID"] = df_tx["OrganisationID"].fillna("").astype(str)
    df_tx["_is_coupon_line"] = df_tx["Label"].str.upper().eq("COUPON")

    # Agrégation ticket
    agg_ticket = df_tx.groupby("TransactionID", dropna=False).agg(
        CA_TTC_ticket=("CA_TTC", "max"),
        CA_HT_ticket=("CA_HT", "sum"),
        Cost_ticket=("Purch_Total_HT", "sum"),
        Qty_ticket=("Qty_Ticket", "sum"),
        Has_Coupon=("_is_coupon_line", "max"),
        ValidationDate=("ValidationDate", "max"),
        OrganisationID=("OrganisationID", "last"),
        CustomerID=("CustomerID", "last")
    ).reset_index()

    agg_ticket["month"] = _month_str(agg_ticket["ValidationDate"])
    agg_ticket["Marge_net_HT_ticket"] = agg_ticket["CA_HT_ticket"] - agg_ticket["Cost_ticket"]
    agg_ticket["CA_paid_with_coupons"] = np.where(agg_ticket["Has_Coupon"], agg_ticket["CA_TTC_ticket"], 0.0)

    # Base mensuelle
    base = agg_ticket.groupby(["month","OrganisationID"], dropna=False).agg(
        CA_TTC=("CA_TTC_ticket","sum"),
        CA_HT=("CA_HT_ticket","sum"),
        Marge_net_HT_avant_coupon=("Marge_net_HT_ticket","sum"),
        Transactions=("TransactionID","nunique"),
        Qty_total=("Qty_ticket","sum"),
        CA_paid_with_coupons=("CA_paid_with_coupons","sum"),
        Tickets_avec_coupon=("Has_Coupon","sum")
    ).reset_index()

    # Coupons utilisés / émis
    coupons_used = df_cp.dropna(subset=["UseDate"]).groupby(["month_use","OrganisationID"]).agg(
        Coupon_utilise=("CouponID","nunique"),
        Montant_coupons_utilise=("Value_Used_Line","sum")
    ).rename(columns={"month_use":"month"}).reset_index()

    coupons_emis = df_cp.dropna(subset=["EmissionDate"]).groupby(["month_emit","OrganisationID"]).agg(
        Coupon_emis=("CouponID","nunique"),
        Montant_coupons_emis=("Amount_Initial","sum")
    ).rename(columns={"month_emit":"month"}).reset_index()

    # --- Clients / Nouveaux / Récurrents / Rétention
    ticket_client = agg_ticket[agg_ticket["CustomerID"].str.len() > 0].copy()

    clients_mois = ticket_client.groupby(["month","OrganisationID"])["CustomerID"].nunique().reset_index(name="Clients")
    first_seen = ticket_client.groupby(["OrganisationID","CustomerID"])["ValidationDate"].min().reset_index(name="FirstDate")
    ticket_client = ticket_client.merge(first_seen, on=["OrganisationID","CustomerID"], how="left")
    ticket_client["IsNewThisMonth"] = (ticket_client["ValidationDate"].dt.to_period("M") == ticket_client["FirstDate"].dt.to_period("M"))

    nouveaux = ticket_client[ticket_client["IsNewThisMonth"]].groupby(["month","OrganisationID"])["CustomerID"].nunique().reset_index(name="Nouveau_client")
    tx_client = ticket_client.groupby(["month","OrganisationID"])["TransactionID"].nunique().reset_index(name="Transactions_Client")

    new_ret = (clients_mois.merge(nouveaux, on=["month","OrganisationID"], how="left")
                        .merge(tx_client, on=["month","OrganisationID"], how="left")
                        .fillna({"Nouveau_client":0,"Transactions_Client":0}))
    new_ret["Client_qui_reviennent"] = (new_ret["Clients"] - new_ret["Nouveau_client"]).clip(lower=0).astype(int)
    new_ret["Recurrence"] = np.where(new_ret["Clients"]>0, new_ret["Transactions_Client"]/new_ret["Clients"], "")

    cust_sets = ticket_client.groupby(["OrganisationID","month"])["CustomerID"].apply(lambda s: set(s.dropna().astype(str).unique())).reset_index(name="CustSet")
    cust_sets["_order"] = pd.PeriodIndex(cust_sets["month"], freq="M").to_timestamp()
    cust_sets = cust_sets.sort_values(["OrganisationID","_order"])
    cust_sets["Prev"] = cust_sets.groupby("OrganisationID")["CustSet"].shift(1)
    cust_sets["Retention_rate"] = cust_sets.apply(lambda r: len(r["Prev"].intersection(r["CustSet"])) / len(r["Prev"]) if isinstance(r["Prev"], set) and len(r["Prev"])>0 else "", axis=1)
    ret = cust_sets[["month","OrganisationID","Retention_rate"]]
    # 🔧 Harmonisation des colonnes avant merge KPI
    for df in [base, new_ret, ret, coupons_used, coupons_emis]:
        if "OrganisationID" not in df.columns:
            # essaie de récupérer une version minuscule si présente
            if "organisationid" in df.columns:
                df["OrganisationID"] = df["organisationid"]
            else:
                df["OrganisationID"] = ""
        df["OrganisationID"] = df["OrganisationID"].astype(str).fillna("")
        if "month" not in df.columns:
            df["month"] = ""
        df["month"] = df["month"].astype(str).fillna("")

    # --- Merge KPI
    kpi = (base.merge(new_ret, on=["month","OrganisationID"], how="left")
                .merge(ret, on=["month","OrganisationID"], how="left")
                .merge(coupons_used, on=["month","OrganisationID"], how="left")
                .merge(coupons_emis, on=["month","OrganisationID"], how="left"))

    # --- Calculs finaux
    kpi["Marge_net_HT_apres_coupon"] = kpi["Marge_net_HT_avant_coupon"] - kpi["Montant_coupons_utilise"].fillna(0)
    kpi["Taux_de_marge_HT_avant_coupon"] = np.where(kpi["CA_HT"]>0, kpi["Marge_net_HT_avant_coupon"]/kpi["CA_HT"], "")
    kpi["Taux_de_marge_HT_apres_coupons"] = np.where(kpi["CA_HT"]>0, kpi["Marge_net_HT_apres_coupon"]/kpi["CA_HT"], "")
    kpi["ROI_Proxy"] = np.where(kpi["Montant_coupons_utilise"].fillna(0)>0,
                                (kpi["CA_paid_with_coupons"].fillna(0) - kpi["Montant_coupons_utilise"].fillna(0)) / kpi["Montant_coupons_utilise"].fillna(0), "")
    kpi["Taux_utilisation_bons_montant"] = np.where(kpi["Montant_coupons_emis"].fillna(0)>0,
                                                    kpi["Montant_coupons_utilise"].fillna(0)/kpi["Montant_coupons_emis"].fillna(0), "")
    kpi["Taux_utilisation_bons_quantite"] = np.where(kpi["Coupon_emis"].fillna(0)>0,
                                                     kpi["Coupon_utilise"].fillna(0)/kpi["Coupon_emis"].fillna(0), "")
    kpi["Taux_CA_genere_par_bons_sur_CA_HT"] = np.where(kpi["CA_HT"]>0, kpi["CA_paid_with_coupons"]/kpi["CA_HT"], "")
    kpi["Voucher_share"] = np.where(kpi["Transactions"]>0, kpi["Tickets_avec_coupon"]/kpi["Transactions"], "")
    kpi["Panier_moyen_HT"] = np.where(kpi["Transactions"]>0, kpi["CA_HT"]/kpi["Transactions"], "")
    kpi["Prix_moyen_article_vendu_HT"] = np.where(kpi["Qty_total"]>0, kpi["CA_HT"]/kpi["Qty_total"], "")
    kpi["Date"] = pd.to_datetime(kpi["month"], errors="coerce").dt.strftime("%d/%m/%Y")

    # Nettoyage
    num_cols = ["CA_TTC","CA_HT","CA_paid_with_coupons","Marge_net_HT_avant_coupon","Marge_net_HT_apres_coupon",
                "Transactions","Transactions_Client","Clients","Coupon_utilise","Montant_coupons_utilise",
                "Coupon_emis","Montant_coupons_emis","Panier_moyen_HT","Prix_moyen_article_vendu_HT","Qty_total"]
    for c in num_cols:
        if c in kpi.columns:
            kpi[c] = pd.to_numeric(kpi[c], errors="coerce").fillna(0)
    kpi = kpi.fillna("")

    st.subheader("📊 Aperçu KPI mensuel")
    st.dataframe(kpi.head(50))

    # --- Export CSV
    csv = kpi.to_csv(index=False, sep=";").encode("utf-8-sig")
    st.download_button("💾 Télécharger le KPI mensuel (CSV)", csv, "KPI_mensuel.csv", "text/csv")

else:
    st.info("➡️ Importez les fichiers Transactions et Coupons pour démarrer.")
