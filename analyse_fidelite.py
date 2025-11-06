import streamlit as st
import pandas as pd
import numpy as np
import duckdb, os, json, re, smtplib, requests
from datetime import datetime
from email.message import EmailMessage

import gspread
from google.oauth2 import service_account

# =========================
# PAGE & TITRE
# =========================
st.set_page_config(page_title="Analyse Fidélité — DuckDB ➜ KPI Sheets", layout="wide")
st.title("🎯 Analyse Fidélité — Historique DuckDB ➜ KPI mensuels (Google Sheets)")

# =========================
# SECRETS (à configurer)
# =========================
# Dans Streamlit Cloud, mets-les dans Settings > Secrets.
# En local, crée .streamlit/secrets.toml et ajoute :
# [sheets]
# spreadsheet_id="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
# [email]
# smtp_server="smtp.ionos.fr"
# smtp_port=587
# smtp_user="xxx@xxx"
# smtp_password="xxxxx"
# receiver="destinataire@xxx"
# [gcp]
# service_json = """{ ... ton JSON de compte de service ... }"""
# [app]
# looker_url="https://lookerstudio.google.com/..."   # optionnel

SPREADSHEET_ID = st.secrets.get("sheets", {}).get("spreadsheet_id", "")
SHEET_KPI = "KPI_Mensuels"
LOOKER_URL = st.secrets.get("app", {}).get("looker_url", "")

SMTP_SERVER = st.secrets.get("email", {}).get("smtp_server", "")
SMTP_PORT   = int(st.secrets.get("email", {}).get("smtp_port", 587))
SMTP_USER   = st.secrets.get("email", {}).get("smtp_user", "")
SMTP_PASS   = st.secrets.get("email", {}).get("smtp_password", "")
DEFAULT_RECEIVER = st.secrets.get("email", {}).get("receiver", "")

SERVICE_JSON = st.secrets.get("gcp", {}).get("service_json", "")

# Fichier DuckDB persistant
DUCKDB_PATH = "historique.duckdb"

# =========================
# HELPERS
# =========================
def _ensure_date(s): return pd.to_datetime(s, errors="coerce")
def _month_str(s): return _ensure_date(s).dt.to_period("M").astype(str)
def _norm_cols(df): return df.rename(columns={c: re.sub(r"[^a-z0-9]", "", c.lower()) for c in df.columns})
def _pick(df, *cands):
    for c in cands:
        k = re.sub(r"[^a-z0-9]", "", c.lower())
        if k in df.columns: return k
    return None

@st.cache_resource
# --- Authentification Google : via Drive file ID ou secrets direct
def get_gcp_creds():
    if "gcp" in st.secrets and "json_drive_file_id" in st.secrets["gcp"]:
        file_id = st.secrets["gcp"]["json_drive_file_id"]
        url = f"https://drive.google.com/uc?id={file_id}"
        resp = requests.get(url)
        resp.raise_for_status()
        gcp_service_account_info = json.loads(resp.content)
    elif "gcp" in st.secrets and "service_json" in st.secrets["gcp"]:
        gcp_service_account_info = json.loads(st.secrets["gcp"]["service_json"])
    else:
        st.error("❌ Aucun identifiant Google (gcp.json_drive_file_id ou gcp.service_json).")
        st.stop()

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_info(gcp_service_account_info, scopes=scopes)
    return creds

@st.cache_resource
def gs_client():
    return gspread.authorize(get_gcp_creds())

def ws_open_or_create(spreadsheet_id, tab_name):
    sh = gs_client().open_by_key(spreadsheet_id)
    try:
        return sh.worksheet(tab_name)
    except Exception:
        return sh.add_worksheet(title=tab_name, rows=2, cols=120)

def ws_overwrite_small(ws, df):
    safe = df.copy()
    # stringify dates
    for c in safe.columns:
        if np.issubdtype(safe[c].dtype, np.datetime64):
            safe[c] = pd.to_datetime(safe[c], errors="coerce").dt.strftime("%Y-%m-%d")
    safe = safe.where(pd.notnull(safe), "")
    ws.clear()
    ws.update("A1", [list(safe.columns)] + safe.values.tolist(), value_input_option="USER_ENTERED")

def _read_csv_tolerant(uploaded):
    df = pd.read_csv(uploaded, sep=";", encoding="utf-8-sig", on_bad_lines="skip", engine="python", dtype=str)
    for col in df.columns:
        df[col] = df[col].astype(str).str.replace("'", "", regex=False).str.strip()
    # conversion douce : si >70% de la colonne est numérique, on convertit (en remplaçant , par .)
    for col in df.columns:
        try_num = pd.to_numeric(df[col].str.replace(",", ".", regex=False), errors="coerce")
        if try_num.notna().mean() > 0.7:
            df[col] = try_num
    return df

def _build_fact_from_transactions(tx):
    """Transforme le CSV 'transactions' en table tickets agrégée (fact)."""
    c_txid  = _pick(tx, "ticketnumber", "transactionid", "operationid")
    c_total = _pick(tx, "totalamount", "totaltcc", "totalttc")               # TTC ticket
    c_label = _pick(tx, "label", "libelle", "designation")
    c_valid = _pick(tx, "validationdate", "operationdate", "date")
    c_org   = _pick(tx, "organisationid", "organizationid")
    c_cust  = _pick(tx, "customerid", "clientid")
    c_gross = _pick(tx, "linegrossamount", "montanthtligne", "cahtligne", "montantht")  # HT ligne
    c_cost  = _pick(tx, "linetotalpurchasingamount", "purchasingamount", "achatht")
    c_qty   = _pick(tx, "quantity", "qty", "linequantity", "quantite")

    if not all([c_txid, c_total, c_valid, c_org, c_gross, c_cost]):
        st.error("❌ Colonnes minimales manquantes dans Transactions (ID, TTC, Date, Org, HT ligne, Coût).")
        st.stop()

    # conversions sûres
    for c in [c_total, c_gross, c_cost, c_qty]:
        if c and c in tx.columns:
            tx[c] = pd.to_numeric(tx[c], errors="coerce")

    ca_ttc = tx.groupby(c_txid, dropna=False)[c_total].max().rename("CA_TTC").reset_index()
    ca_ht  = tx.groupby(c_txid, dropna=False)[c_gross].sum().rename("CA_HT").reset_index()
    cost   = tx.groupby(c_txid, dropna=False)[c_cost].sum().rename("Purch_Total_HT").reset_index()
    if c_qty and c_qty in tx.columns and tx[c_qty].notna().any():
        qty_ticket = tx.groupby(c_txid)[c_qty].sum().rename("Qty_Ticket").reset_index()
    else:
        qty_ticket = tx.groupby(c_txid)[c_txid].size().rename("Qty_Ticket").reset_index()

    has_coupon = (
        tx.assign(_lbl=tx[c_label].fillna("").astype(str).str.upper() if c_label else "")
          .groupby(c_txid)["_lbl"].apply(lambda s: s.str.contains("COUPON", regex=False).any())
          .reset_index(name="Has_Coupon")
    )

    ctx_cols = [c_txid, c_valid, c_org] + ([c_cust] if c_cust else [])
    ctx = tx[ctx_cols].drop_duplicates(subset=[c_txid]).rename(columns={
        c_txid:"TransactionID", c_valid:"ValidationDate", c_org:"OrganisationID", c_cust:"CustomerID" if c_cust else None
    })

    fact = (ca_ttc.merge(ca_ht, on=c_txid)
                 .merge(cost, on=c_txid)
                 .merge(qty_ticket, on=c_txid)
                 .merge(has_coupon, on=c_txid)
                 .merge(ctx, left_on=c_txid, right_on="TransactionID", how="left"))

    fact["ValidationDate"] = _ensure_date(fact["ValidationDate"])
    fact["month"] = _month_str(fact["ValidationDate"])
    fact["CustomerID"] = fact["CustomerID"].astype(str).str.strip().replace(["", "nan", "none", "NaN", "None"], np.nan)
    fact["is_client"] = fact["CustomerID"].notna()
    fact["CA_paid_with_coupons_HT"] = np.where(fact["Has_Coupon"], fact["CA_HT"], 0.0)
    fact["Estimated_Net_Margin_HT"] = fact["CA_HT"] - fact["Purch_Total_HT"]
    return fact

def _build_coupon_table(cp):
    """Nettoie/structure le CSV 'coupons'."""
    c_couponid = _pick(cp, "couponid", "id")
    c_init     = _pick(cp, "initialvalue", "valeurinitiale", "montantinit")
    c_rem      = _pick(cp, "amount", "remaining", "reste")
    c_usedate  = _pick(cp, "usedate", "dateutilisation")
    c_emiss    = _pick(cp, "creationdate", "datecreation")
    c_orgc     = _pick(cp, "organisationid", "organizationid")
    if not all([c_couponid, c_orgc]):
        st.warning("⚠️ Colonnes coupons incomplètes — KPIs liés aux bons peuvent être vides.")
    cp["UseDate"] = _ensure_date(cp[c_usedate]) if c_usedate else pd.NaT
    cp["EmissionDate"] = _ensure_date(cp[c_emiss]) if c_emiss else pd.NaT
    cp["Amount_Initial"]   = pd.to_numeric(cp[c_init].astype(str).str.replace(",", ".", regex=False), errors="coerce").fillna(0) if c_init else 0.0
    cp["Amount_Remaining"] = pd.to_numeric(cp[c_rem].astype(str).str.replace(",", ".", regex=False), errors="coerce").fillna(0) if c_rem else 0.0
    cp["Value_Used_Line"]  = (cp["Amount_Initial"] - cp["Amount_Remaining"]).clip(lower=0)
    cp["month_use"]  = _month_str(cp["UseDate"])
    cp["month_emit"] = _month_str(cp["EmissionDate"])
    cp = cp.rename(columns={c_couponid:"CouponID", c_orgc:"OrganisationID"})
    return cp[["CouponID","OrganisationID","UseDate","EmissionDate","Amount_Initial","Amount_Remaining","Value_Used_Line","month_use","month_emit"]]

def init_duckdb():
    con = duckdb.connect(DUCKDB_PATH)
    # tables si absentes
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

def upsert_transactions(con, fact_df):
    # Insert uniquement les nouvelles TransactionID
    con.execute("CREATE TEMP TABLE tmp_new AS SELECT * FROM fact_df")
    con.execute("""
        INSERT INTO transactions
        SELECT * FROM tmp_new n
        WHERE NOT EXISTS (
            SELECT 1 FROM transactions t WHERE t.TransactionID = n.TransactionID
        )
    """)
    con.execute("DROP TABLE tmp_new")

def append_coupons(con, cp_df):
    # On peut faire un append "naïf" + dédup si besoin
    con.execute("CREATE TEMP TABLE tmp_cp AS SELECT * FROM cp_df")
    con.execute("""
        INSERT INTO coupons
        SELECT * FROM tmp_cp
    """)
    con.execute("DROP TABLE tmp_cp")

def compute_kpi(con):
    # Récupération en DataFrame pandas
    fact = con.execute("SELECT * FROM transactions").fetch_df()
    if fact.empty:
        return pd.DataFrame()

    grp = ["month","OrganisationID"]

    # Transactions de base
    base = (fact.groupby(grp).agg(
        CA_TTC=("CA_TTC","sum"),
        CA_HT=("CA_HT","sum"),
        Transactions=("TransactionID","nunique"),
        Qty_total=("Qty_Ticket","sum")
    ).reset_index())

    # Transactions associées à un client
    tx_client = (fact[fact["is_client"]==True]
                 .groupby(grp)["TransactionID"]
                 .nunique().reset_index(name="Transaction associé à un client (nombre)"))

    # Clients, nouveaux, récurrence
    tx_clients = fact[fact["is_client"]==True].copy()
    kpi_parts = []
    if not tx_clients.empty:
        first_seen = tx_clients.groupby("CustomerID")["ValidationDate"].min().reset_index(name="FirstDate")
        tx_clients = tx_clients.merge(first_seen, on="CustomerID", how="left")
        tx_clients["IsNew"] = tx_clients["ValidationDate"].dt.to_period("M") == tx_clients["FirstDate"].dt.to_period("M")

        clients = tx_clients.groupby(grp)["CustomerID"].nunique().reset_index(name="Client")
        newc    = tx_clients[tx_clients["IsNew"]].groupby(grp)["CustomerID"].nunique().reset_index(name="Nouveau client")
        txc     = tx_clients.groupby(grp)["TransactionID"].nunique().reset_index(name="TX_client")

        churn = clients.merge(newc, on=grp, how="left").merge(txc, on=grp, how="left").fillna(0)
        churn["Client qui reviennent"] = churn["Client"] - churn["Nouveau client"]
        churn["Recurrence (combien de fois un client revient par mois en moyenne)"] = np.where(
            churn["Client"]>0, churn["TX_client"]/churn["Client"], np.nan
        )
        churn = churn.drop(columns=["TX_client"])
        kpi_parts.append(churn)

        # Rétention (M-1 -> M, par org)
        cust_sets = tx_clients.groupby(grp)["CustomerID"].apply(lambda s: set(s.unique())).reset_index(name="CustSet")
        cust_sets["_order"] = pd.PeriodIndex(cust_sets["month"], freq="M").to_timestamp()
        cust_sets = cust_sets.sort_values(["OrganisationID","_order"])
        cust_sets["Prev"] = cust_sets.groupby("OrganisationID")["CustSet"].shift(1)
        cust_sets["Retention_rate"] = cust_sets.apply(
            lambda r: (len(r["Prev"].intersection(r["CustSet"])) / len(r["Prev"])) if isinstance(r["Prev"], set) and len(r["Prev"])>0 else np.nan,
            axis=1
        )
        retention = cust_sets[grp+["Retention_rate"]]
        kpi_parts.append(retention)
    else:
        kpi_parts.append(pd.DataFrame(columns=grp+["Client","Nouveau client","Client qui reviennent","Recurrence (combien de fois un client revient par mois en moyenne)"]))
        kpi_parts.append(pd.DataFrame(columns=grp+["Retention_rate"]))

    # CA payé avec coupons HT
    ca_coupon = fact.groupby(grp)["CA_paid_with_coupons_HT"].sum().reset_index(name="CA paid with coupons")

    # Paniers moyens (global, client, non client, avec/sans coupon)
    panier_global = base.assign(**{
        "Panier moyen HT": np.where(base["Transactions"]>0, base["CA_HT"]/base["Transactions"], np.nan)
    })[grp+["Panier moyen HT"]]

    by_client = fact.groupby(grp+["is_client"]).agg(CA_HT=("CA_HT","sum"), TX=("TransactionID","nunique")).reset_index()
    pm_client = by_client[by_client["is_client"]==True].copy()
    pm_client["Panier moyen client"] = np.where(pm_client["TX"]>0, pm_client["CA_HT"]/pm_client["TX"], np.nan)
    pm_client = pm_client[grp+["Panier moyen client"]]
    pm_non = by_client[by_client["is_client"]==False].copy()
    pm_non["Panier moyen non client"] = np.where(pm_non["TX"]>0, pm_non["CA_HT"]/pm_non["TX"], np.nan)
    pm_non = pm_non[grp+["Panier moyen non client"]]

    by_coupon = fact.groupby(grp+["Has_Coupon"]).agg(CA_HT=("CA_HT","sum"), TX=("TransactionID","nunique")).reset_index()
    pm_avec = by_coupon[by_coupon["Has_Coupon"]==True].copy()
    pm_avec["Panier moyen avec coupon"] = np.where(pm_avec["TX"]>0, pm_avec["CA_HT"]/pm_avec["TX"], np.nan)
    pm_avec = pm_avec[grp+["Panier moyen avec coupon"]]
    pm_sans = by_coupon[by_coupon["Has_Coupon"]==False].copy()
    pm_sans["Panier moyen sans coupon"] = np.where(pm_sans["TX"]>0, pm_sans["CA_HT"]/pm_sans["TX"], np.nan)
    pm_sans = pm_sans[grp+["Panier moyen sans coupon"]]

    # Prix moyen article & quantité moyenne par transaction
    prix_moy = fact.groupby(grp).agg(Qty_total=("Qty_Ticket","sum"), CA_HT=("CA_HT","sum")).reset_index()
    prix_moy["Prix moyen article vendu HT"] = np.where(prix_moy["Qty_total"]>0, prix_moy["CA_HT"]/prix_moy["Qty_total"], np.nan)
    prix_moy = prix_moy[grp+["Prix moyen article vendu HT"]]
    qte_moy_tx = fact.groupby(grp)["Qty_Ticket"].mean().reset_index(name="Quantité moyen article par transaction")

    # Marges
    marge_avant = fact.groupby(grp).apply(lambda d: (d["CA_HT"] - d["Purch_Total_HT"]).sum()).reset_index(name="Marge net HT avant coupon")

    # Coupons (depuis DuckDB)
    coupons = con.execute("SELECT * FROM coupons").fetch_df()
    if coupons.empty:
        coupons_used = pd.DataFrame(columns=grp+["Coupon utilisé","Montant coupons utilisé"])
        coupons_emis = pd.DataFrame(columns=grp+["Coupon émis","Montant coupons émis"])
    else:
        coupons_used = (coupons.dropna(subset=["UseDate"])
                        .groupby(["month_use","OrganisationID"])
                        .agg(**{"Coupon utilisé":("CouponID","nunique"),
                                "Montant coupons utilisé":("Value_Used_Line","sum")})
                        .reset_index().rename(columns={"month_use":"month"}))
        coupons_emis = (coupons.dropna(subset=["EmissionDate"])
                        .groupby(["month_emit","OrganisationID"])
                        .agg(**{"Coupon émis":("CouponID","nunique"),
                                "Montant coupons émis":("Amount_Initial","sum")})
                        .reset_index().rename(columns={"month_emit":"month"}))

    # Merge progressif KPI
    kpi = (base
           .merge(tx_client, on=grp, how="left")
           .merge(kpi_parts[0], on=grp, how="left")      # churn + new + recurrence
           .merge(kpi_parts[1], on=grp, how="left")      # retention
           .merge(ca_coupon, on=grp, how="left")
           .merge(coupons_used, on=grp, how="left")
           .merge(coupons_emis, on=grp, how="left")
           .merge(marge_avant, on=grp, how="left")
           .merge(panier_global, on=grp, how="left")
           .merge(pm_client, on=grp, how="left")
           .merge(pm_non, on=grp, how="left")
           .merge(pm_sans, on=grp, how="left")
           .merge(pm_avec, on=grp, how="left")
           .merge(prix_moy, on=grp, how="left")
           .merge(qte_moy_tx, on=grp, how="left")
           )

    # Post-calculs & ratios
    kpi = kpi.rename(columns={"Transactions":"Transaction (nombre)"})
    kpi["Taux association client"] = np.where(kpi["Transaction (nombre)"]>0,
                                              kpi["Transaction associé à un client (nombre)"]/kpi["Transaction (nombre)"], np.nan)
    kpi["Marge net HT après coupon"] = kpi["Marge net HT avant coupon"] - kpi["Montant coupons utilisé"].fillna(0)
    kpi["Taux de marge HT avant coupon"]  = np.where(kpi["CA_HT"]>0, kpi["Marge net HT avant coupon"]/kpi["CA_HT"], np.nan)
    kpi["Taux de marge HT après coupons"] = np.where(kpi["CA_HT"]>0, kpi["Marge net HT après coupon"]/kpi["CA_HT"], np.nan)
    kpi["ROI_Proxy"] = np.where(kpi["Montant coupons utilisé"]>0,
                                (kpi["CA paid with coupons"] - kpi["Montant coupons utilisé"])/kpi["Montant coupons utilisé"],
                                np.nan)
    kpi["Taux d'utilisation des bons en montant"]  = np.where(kpi["Montant coupons émis"]>0,
                                                              kpi["Montant coupons utilisé"]/kpi["Montant coupons émis"], np.nan)
    kpi["Taux d'utilisation des bons en quantité"] = np.where(kpi["Coupon émis"]>0,
                                                              kpi["Coupon utilisé"]/kpi["Coupon émis"], np.nan)
    kpi["Taux de CA généré par les bons sur CA HT"] = np.where(kpi["CA_HT"]>0,
                                                               kpi["CA paid with coupons"]/kpi["CA_HT"], np.nan)
    kpi["Voucher_share"] = kpi["Taux de CA généré par les bons sur CA HT"]
    kpi["date (format date)"] = pd.to_datetime(kpi["month"], format="%Y-%m", errors="coerce").dt.strftime("%d/%m/%Y")

    # Colonnes finales (ordre figé)
    final_cols = [
        "month","OrganisationID",
        "CA_TTC","CA_HT","CA paid with coupons",
        "Coupon émis","Coupon utilisé","Montant coupons émis","Montant coupons utilisé",
        "Transaction (nombre)","Transaction associé à un client (nombre)",
        "Client","Nouveau client","Client qui reviennent",
        "Recurrence (combien de fois un client revient par mois en moyenne)",
        "Retention_rate","Taux association client",
        "Marge net HT avant coupon","Marge net HT après coupon",
        "Taux de marge HT avant coupon","Taux de marge HT après coupons","ROI_Proxy",
        "Panier moyen HT","Panier moyen client","Panier moyen non client",
        "Panier moyen sans coupon","Panier moyen avec coupon",
        "Taux d'utilisation des bons en montant","Taux d'utilisation des bons en quantité",
        "Taux de CA généré par les bons sur CA HT",
        "Prix moyen article vendu HT","Quantité moyen article par transaction",
        "Voucher_share","date (format date)"
    ]
    for col in final_cols:
        if col not in kpi.columns:
            kpi[col] = np.nan
    kpi = kpi[final_cols].sort_values(["OrganisationID","month"]).reset_index(drop=True)
    return kpi

def send_mail(to_list, subject, body):
    if not (SMTP_SERVER and SMTP_USER and SMTP_PASS and to_list):
        st.warning("⚠️ Paramètres SMTP incomplets — mail non envoyé.")
        return
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(to_list)
    msg.set_content(body)
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)

# =========================
# UI — UPLOAD
# =========================
st.sidebar.header("📂 Importer les fichiers")
file_tx = st.sidebar.file_uploader("Transactions (CSV Keyneo)", type=["csv"])
file_cp = st.sidebar.file_uploader("Coupons (CSV Keyneo)", type=["csv"])
emails_supp = st.sidebar.text_input("📧 Autres destinataires (séparés par des virgules)")

if file_tx and file_cp:
    tx_csv = _norm_cols(_read_csv_tolerant(file_tx))
    cp_csv = _norm_cols(_read_csv_tolerant(file_cp))

    # Construire tables propres
    fact = _build_fact_from_transactions(tx_csv)
    cp_tbl = _build_coupon_table(cp_csv)

    # Init DB et upsert
    con = init_duckdb()
    con.register("fact_df", fact)
    con.register("cp_df", cp_tbl)

    # Transactions: insert only new IDs
    before_tx = con.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    upsert_transactions(con, fact)
    after_tx  = con.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    new_tx_count = after_tx - before_tx

    # Coupons: append
    before_cp = con.execute("SELECT COUNT(*) FROM coupons").fetchone()[0]
    append_coupons(con, cp_tbl)
    after_cp  = con.execute("SELECT COUNT(*) FROM coupons").fetchone()[0]
    new_cp_count = after_cp - before_cp

    st.success(f"✅ Historique mis à jour : +{new_tx_count} transactions, +{new_cp_count} coupons.")

    # KPI
    kpi = compute_kpi(con)
    if kpi.empty:
        st.warning("Aucune ligne KPI calculée (vérifie tes uploads).")
        st.stop()

    with st.expander("👀 Aperçu KPI (15 premières lignes)"):
        st.dataframe(kpi.head(15))

    # Export Google Sheets
    if not SPREADSHEET_ID:
        st.error("❌ sheets.spreadsheet_id manquant dans secrets.")
        st.stop()
    ws_kpi = ws_open_or_create(SPREADSHEET_ID, SHEET_KPI)
    ws_overwrite_small(ws_kpi, kpi)
    st.success(f"📤 KPI_Mensuels mis à jour dans Google Sheets ({len(kpi)} lignes).")

    # Envoi mail
    if st.button("📧 Envoyer le lien par e-mail"):
        recipients = [DEFAULT_RECEIVER] + [e.strip() for e in emails_supp.split(",") if e.strip()]
        subject = f"📊 Rapport fidélité — KPI mis à jour ({datetime.today().strftime('%d/%m/%Y')})"
        link = LOOKER_URL or f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
        body = f"""Bonjour,

Le rapport fidélité a été mis à jour (KPI mensuels).
Accès : {link}

Bien à vous,
Automate KPI (DuckDB ➜ Sheets)
"""
        try:
            send_mail(recipients, subject, body)
            st.success("✉️ Mail envoyé.")
        except Exception as e:
            st.error(f"❌ Erreur d’envoi mail : {e}")

else:
    st.info("➡️ Importez vos CSV Transactions et Coupons pour démarrer.")
