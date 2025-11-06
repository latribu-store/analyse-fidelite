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

    # 4️⃣ Mapping automatique Coupons
    map_cp = {
        "CouponID": pick(cp, "couponid", "id"),
        "OrganisationID": pick(cp, "organisationid", "organizationid"),
        "EmissionDate": pick(cp, "creationdate", "issuedate"),
        "UseDate": pick(cp, "usedate", "validationdate"),
        "Amount_Initial": pick(cp, "initialvalue", "amount", "value"),
        "Amount_Remaining": pick(cp, "amountremaining", "restant", "reste"),
    }

    for k,v in map_cp.items():
        cp[k] = cp[v] if v in cp.columns else ""

    cp["EmissionDate"] = _ensure_date(cp["EmissionDate"])
    cp["UseDate"] = _ensure_date(cp["UseDate"])
    cp["Amount_Initial"] = pd.to_numeric(cp["Amount_Initial"], errors="coerce").fillna(0.0)
    cp["Amount_Remaining"] = pd.to_numeric(cp["Amount_Remaining"], errors="coerce").fillna(0.0)
    cp["Value_Used_Line"] = (cp["Amount_Initial"] - cp["Amount_Remaining"]).clip(lower=0.0)
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
    # 6️⃣ KPI Mensuel — version corrigée (ticket-based, no double count)
    # ======================================================
    df_tx = hist_tx.copy()
    df_cp = hist_cp.copy()

    if df_tx.empty:
        st.warning("⚠️ Pas de données transactionnelles disponibles.")
        st.stop()

    # --- 6.1 Construire le FACT TICKET (une ligne = un ticket)
    # Normalise champs nécessaires
    df_tx["ValidationDate"] = _ensure_date(df_tx["ValidationDate"])
    df_tx["month"] = _month_str(df_tx["ValidationDate"])
    for col in ["CA_TTC","CA_HT","Purch_Total_HT","Qty_Ticket"]:
        df_tx[col] = pd.to_numeric(df_tx[col], errors="coerce").fillna(0.0)
    df_tx["Label"] = df_tx["Label"].fillna("").astype(str)
    df_tx["CustomerID"] = df_tx["CustomerID"].fillna("").astype(str)
    df_tx["OrganisationID"] = df_tx["OrganisationID"].fillna("").astype(str)

    # Flag coupon au niveau ligne
    df_tx["_is_coupon_line"] = df_tx["Label"].str.upper().eq("COUPON")

    # Agrégations par ticket
    agg_ticket = df_tx.groupby("TransactionID", dropna=False).agg(
        CA_TTC_ticket=("CA_TTC", "max"),              # TTC répété par ligne → prendre max (ou 1ere)
        CA_HT_ticket=("CA_HT", "sum"),                # HT se somme par ligne
        Cost_ticket=("Purch_Total_HT", "sum"),
        Qty_ticket=("Qty_Ticket", "sum"),
        Has_Coupon=("._is_coupon_line".replace(".", ""), "max"),  # any coupon line
        ValidationDate=("ValidationDate", "max"),
        OrganisationID=("OrganisationID", "last"),
        CustomerID=("CustomerID", "last")
    ).reset_index()

    agg_ticket["month"] = _month_str(agg_ticket["ValidationDate"])
    agg_ticket["Marge_net_HT_ticket"] = agg_ticket["CA_HT_ticket"] - agg_ticket["Cost_ticket"]
    agg_ticket["CA_paid_with_coupons"] = np.where(agg_ticket["Has_Coupon"], agg_ticket["CA_TTC_ticket"], 0.0)

    # --- 6.2 Splits utiles
    ticket_client = agg_ticket[agg_ticket["CustomerID"].str.len() > 0].copy()
    ticket_non_client = agg_ticket[agg_ticket["CustomerID"].str.len() == 0].copy()
    ticket_coupon = agg_ticket[agg_ticket["Has_Coupon"]].copy()
    ticket_sans_coupon = agg_ticket[~agg_ticket["Has_Coupon"]].copy()

    # --- 6.3 Bases mensuelles par magasin (OrganisationID)
    base = agg_ticket.groupby(["month","OrganisationID"], dropna=False).agg(
        CA_TTC=("CA_TTC_ticket","sum"),
        CA_HT=("CA_HT_ticket","sum"),
        Marge_net_HT_avant_coupon=("Marge_net_HT_ticket","sum"),
        Transactions=("TransactionID","nunique"),
        Qty_total=("Qty_ticket","sum"),
        CA_paid_with_coupons=("CA_paid_with_coupons","sum"),
        Tickets_avec_coupon=("Has_Coupon","sum")
    ).reset_index()

    # Clients / Transactions assoc. à un client
    cli_base = ticket_client.groupby(["month","OrganisationID"], dropna=False).agg(
        Transactions_Client=("TransactionID","nunique"),
        Clients=("CustomerID","nunique")
    ).reset_index()

    # Association client
    assoc = base.merge(cli_base, on=["month","OrganisationID"], how="left").fillna(0)
    assoc["Taux_association_client"] = np.where(
        assoc["Transactions"]>0, assoc["Transactions_Client"]/assoc["Transactions"], ""
    )

    # Nouveaux vs récurrents (par magasin)
    first_seen = ticket_client.groupby(["OrganisationID","CustomerID"], dropna=False)["ValidationDate"].min().reset_index(name="FirstDate")
    ticket_client = ticket_client.merge(first_seen, on=["OrganisationID","CustomerID"], how="left")
    ticket_client["IsNewThisMonth"] = ticket_client["ValidationDate"].dt.to_period("M") == ticket_client["FirstDate"].dt.to_period("M")

    new_ret = ticket_client.groupby(["month","OrganisationID"], dropna=False).agg(
        Nouveau_client=("IsNewThisMonth", "sum"),
        Clients_mois=("CustomerID","nunique"),
        Transactions_Client=("TransactionID","nunique")
    ).reset_index()
    new_ret["Client_qui_reviennent"] = new_ret["Clients_mois"] - new_ret["Nouveau_client"]
    new_ret["Recurrence"] = np.where(new_ret["Clients_mois"]>0, new_ret["Transactions_Client"]/new_ret["Clients_mois"], "")

    # Rétention (intersection clients N-1 vs N, par magasin)
    cust_sets = (
        ticket_client.groupby(["OrganisationID","month"], dropna=False)["CustomerID"]
        .apply(lambda s: set(s.dropna().astype(str).unique()))
        .reset_index(name="CustSet")
    )
    cust_sets["_order"] = pd.PeriodIndex(cust_sets["month"], freq="M").to_timestamp()
    cust_sets = cust_sets.sort_values(["OrganisationID","_order"])
    cust_sets["Prev"] = cust_sets.groupby("OrganisationID")["CustSet"].shift(1)

    def _retention(row):
        prev, cur = row["Prev"], row["CustSet"]
        if not isinstance(prev, set) or len(prev)==0: 
            return ""
        return len(prev.intersection(cur))/len(prev) if len(prev)>0 else ""

    ret = cust_sets[["month","OrganisationID"]].copy()
    ret["Retention_rate"] = cust_sets.apply(_retention, axis=1)

    # Coupons (émis / utilisés)
    if not df_cp.empty:
        df_cp["EmissionDate"] = _ensure_date(df_cp["EmissionDate"])
        df_cp["UseDate"] = _ensure_date(df_cp["UseDate"])
        df_cp["month_emit"] = _month_str(df_cp["EmissionDate"])
        df_cp["month_use"] = _month_str(df_cp["UseDate"])
        df_cp["Amount_Initial"] = pd.to_numeric(df_cp["Amount_Initial"], errors="coerce").fillna(0.0)
        df_cp["Amount_Remaining"] = pd.to_numeric(df_cp["Amount_Remaining"], errors="coerce").fillna(0.0)
        df_cp["Value_Used_Line"] = (df_cp["Amount_Initial"] - df_cp["Amount_Remaining"]).clip(lower=0.0)
        df_cp["OrganisationID"] = df_cp["OrganisationID"].fillna("").astype(str)

        coupons_used = df_cp.dropna(subset=["UseDate"]).groupby(["month_use","OrganisationID"], dropna=False).agg(
            Coupon_utilise=("CouponID","nunique"),
            Montant_coupons_utilise=("Value_Used_Line","sum")
        ).rename(columns={"month_use":"month"}).reset_index()

        coupons_emis = df_cp.dropna(subset=["EmissionDate"]).groupby(["month_emit","OrganisationID"], dropna=False).agg(
            Coupon_emis=("CouponID","nunique"),
            Montant_coupons_emis=("Amount_Initial","sum")
        ).rename(columns={"month_emit":"month"}).reset_index()
    else:
        coupons_used = pd.DataFrame(columns=["month","OrganisationID","Coupon_utilise","Montant_coupons_utilise"])
        coupons_emis = pd.DataFrame(columns=["month","OrganisationID","Coupon_emis","Montant_coupons_emis"])

    # Harmonise types pour merge
    for df in [base, assoc, new_ret, ret, coupons_used, coupons_emis]:
        for col in ["month","OrganisationID"]:
            if col not in df.columns: df[col] = ""
            df[col] = df[col].astype(str).fillna("")

    # --- 6.4 KPI final (toutes colonnes)
    kpi = (base
        .merge(assoc[["month","OrganisationID","Transactions_Client","Clients","Taux_association_client"]], on=["month","OrganisationID"], how="left")
        .merge(new_ret[["month","OrganisationID","Nouveau_client","Client_qui_reviennent","Recurrence"]], on=["month","OrganisationID"], how="left")
        .merge(ret, on=["month","OrganisationID"], how="left")
        .merge(coupons_used, on=["month","OrganisationID"], how="left")
        .merge(coupons_emis, on=["month","OrganisationID"], how="left")
        )

    # Paniers moyens
    kpi["Panier_moyen_HT"] = np.where(kpi["Transactions"]>0, kpi["CA_HT"]/kpi["Transactions"], "")
    # Panier client vs non client
    cli_panier = ticket_client.groupby(["month","OrganisationID"], dropna=False)["CA_HT_ticket"].mean().reset_index(name="Panier_moyen_client")
    noncli_panier = ticket_non_client.groupby(["month","OrganisationID"], dropna=False)["CA_HT_ticket"].mean().reset_index(name="Panier_moyen_non_client")
    kpi = (kpi.merge(cli_panier, on=["month","OrganisationID"], how="left")
            .merge(noncli_panier, on=["month","OrganisationID"], how="left"))

    # Panier avec / sans coupon
    panier_coupon = ticket_coupon.groupby(["month","OrganisationID"], dropna=False)["CA_HT_ticket"].mean().reset_index(name="Panier_moyen_avec_coupon")
    panier_sans_coupon = ticket_sans_coupon.groupby(["month","OrganisationID"], dropna=False)["CA_HT_ticket"].mean().reset_index(name="Panier_moyen_sans_coupon")
    kpi = (kpi.merge(panier_coupon, on=["month","OrganisationID"], how="left")
            .merge(panier_sans_coupon, on=["month","OrganisationID"], how="left"))

    # Taux de marge
    kpi["Marge_net_HT_apres_coupon"] = (kpi["Marge_net_HT_avant_coupon"] - kpi["Montant_coupons_utilise"].fillna(0))
    kpi["Taux_de_marge_HT_avant_coupon"] = np.where(kpi["CA_HT"]>0, kpi["Marge_net_HT_avant_coupon"]/kpi["CA_HT"], "")
    kpi["Taux_de_marge_HT_apres_coupons"] = np.where(kpi["CA_HT"]>0, kpi["Marge_net_HT_apres_coupon"]/kpi["CA_HT"], "")

    # ROI Proxy
    kpi["ROI_Proxy"] = np.where(kpi["Montant_coupons_utilise"].fillna(0)>0,
                                (kpi["CA_paid_with_coupons"].fillna(0) - kpi["Montant_coupons_utilise"].fillna(0)) / kpi["Montant_coupons_utilise"].fillna(0),
                                "")

    # Taux utilisation bons
    kpi["Taux_utilisation_bons_montant"] = np.where(kpi["Montant_coupons_emis"].fillna(0)>0,
                                                    kpi["Montant_coupons_utilise"].fillna(0)/kpi["Montant_coupons_emis"].fillna(0), "")
    kpi["Taux_utilisation_bons_quantite"] = np.where(kpi["Coupon_emis"].fillna(0)>0,
                                                    kpi["Coupon_utilise"].fillna(0)/kpi["Coupon_emis"].fillna(0), "")

    # Part du CA liée aux bons & voucher share
    kpi["Taux_CA_genere_par_bons_sur_CA_HT"] = np.where(kpi["CA_HT"]>0, kpi["CA_paid_with_coupons"]/kpi["CA_HT"], "")
    kpi["Voucher_share"] = np.where(kpi["Transactions"]>0, kpi["Tickets_avec_coupon"]/kpi["Transactions"], "")

    # Prix moyen article & quantité moyenne par transaction
    kpi["Prix_moyen_article_vendu_HT"] = np.where(kpi["Qty_total"]>0, kpi["CA_HT"]/kpi["Qty_total"], "")
    kpi["Quantite_moy_article_par_transaction"] = np.where(kpi["Transactions"]>0, kpi["Qty_total"]/kpi["Transactions"], "")

    # Date lisible
    kpi["Date"] = pd.to_datetime(kpi["month"], errors="coerce").dt.strftime("%d/%m/%Y")

    # Ordonne & nettoie
    colonnes_ordre = [
        "month","Date","OrganisationID",
        "CA_TTC","CA_HT","CA_paid_with_coupons",
        "Marge_net_HT_avant_coupon","Marge_net_HT_apres_coupon",
        "Taux_de_marge_HT_avant_coupon","Taux_de_marge_HT_apres_coupons",
        "Transactions","Transactions_Client","Taux_association_client",
        "Clients","Nouveau_client","Client_qui_reviennent","Recurrence","Retention_rate",
        "Coupon_utilise","Montant_coupons_utilise","Coupon_emis","Montant_coupons_emis",
        "Taux_utilisation_bons_montant","Taux_utilisation_bons_quantite",
        "Taux_CA_genere_par_bons_sur_CA_HT","Voucher_share",
        "Panier_moyen_HT","Panier_moyen_client","Panier_moyen_non_client",
        "Panier_moyen_sans_coupon","Panier_moyen_avec_coupon",
        "Prix_moyen_article_vendu_HT","Quantite_moy_article_par_transaction","Qty_total"
    ]
    for c in colonnes_ordre:
        if c not in kpi.columns:
            kpi[c] = ""

    kpi = kpi[colonnes_ordre].fillna("")

    st.subheader("📊 KPI mensuels (version corrigée)")
    st.dataframe(kpi.head(50))


else:
    st.info("➡️ Importez les fichiers Transactions et Coupons pour démarrer.")
