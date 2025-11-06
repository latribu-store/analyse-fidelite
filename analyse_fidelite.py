import streamlit as st
import pandas as pd
import numpy as np
import duckdb, json, re, smtplib, requests
from datetime import datetime
from email.message import EmailMessage
import gspread
from google.oauth2 import service_account

# =========================================================
# PAGE / TITRE
# =========================================================
st.set_page_config(page_title="Analyse Fidélité — La Tribu", layout="wide")
st.title("🎯 Analyse Fidélité — Historique DuckDB ➜ KPI mensuels (Google Sheets)")

# =========================================================
# SECRETS (format que tu m’as donné)
# =========================================================
SPREADSHEET_ID = st.secrets["sheets"]["spreadsheet_id"]
SHEET_KPI = "KPI_Mensuels"

SMTP_SERVER = st.secrets["email"]["smtp_server"]
SMTP_PORT   = int(st.secrets["email"]["smtp_port"])
SMTP_USER   = st.secrets["email"]["smtp_user"]
SMTP_PASS   = st.secrets["email"]["smtp_password"]
DEFAULT_RECEIVER = st.secrets["email"]["receiver"]

LOOKER_URL = st.secrets["app"].get("looker_url", "")
GCP_JSON_DRIVE_FILE_ID = st.secrets["gcp"]["json_drive_file_id"]

DUCKDB_PATH = "historique.duckdb"

# =========================================================
# HELPERS
# =========================================================
def _ensure_date(s): return pd.to_datetime(s, errors="coerce")
def _month_str(s): return _ensure_date(s).dt.to_period("M").astype(str)

def _norm_cols(df):
    # on garde flexible, mais on travaille sur colonnes originales
    return df

def _pick_exact(df, *cands):
    # pick exact par nom (sans normalisation), selon tes CSV réels
    for c in cands:
        if c in df.columns: return c
    return None

# ---------------- Google Auth via Drive file_id ----------------
def get_gcp_creds():
    url = f"https://drive.google.com/uc?id={GCP_JSON_DRIVE_FILE_ID}"
    resp = requests.get(url); resp.raise_for_status()
    gcp_info = json.loads(resp.content)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    return service_account.Credentials.from_service_account_info(gcp_info, scopes=scopes)

@st.cache_resource
def gs_client():
    return gspread.authorize(get_gcp_creds())

def ws_open_or_create(spreadsheet_id, tab_name):
    sh = gs_client().open_by_key(spreadsheet_id)
    try:
        return sh.worksheet(tab_name)
    except Exception:
        return sh.add_worksheet(title=tab_name, rows=2, cols=160)

def ws_overwrite_small(ws, df):
    safe = df.copy()
    for c in safe.columns:
        if np.issubdtype(safe[c].dtype, np.datetime64):
            safe[c] = pd.to_datetime(safe[c], errors="coerce").dt.strftime("%Y-%m-%d")
    safe = safe.where(pd.notnull(safe), "")
    ws.clear()
    ws.update("A1", [list(safe.columns)] + safe.values.tolist(), value_input_option="USER_ENTERED")

# ---------------- CSV tolerant ----------------
def _read_csv_tolerant(uploaded):
    df = pd.read_csv(uploaded, sep=";", encoding="utf-8-sig", on_bad_lines="skip", engine="python", dtype=str)
    # nettoyage doux des nombres
    for col in df.columns:
        # on ne casse pas les colonnes non-numériques (dates, ids)
        maybe_num = pd.to_numeric(df[col].str.replace(",", ".", regex=False), errors="coerce")
        if maybe_num.notna().mean() > 0.7:
            df[col] = maybe_num
        else:
            df[col] = df[col].astype(str).str.strip()
    return df

# =========================================================
# DUCKDB INIT
# =========================================================
def init_duckdb():
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

# =========================================================
# UPSERT ROBUSTE (blindé contre Binder/KeyError/Conversion)
# =========================================================
def upsert_transactions(con, fact_df):
    """Insert-only robuste : tolérant aux colonnes manquantes/renommées/types."""
    if fact_df is None or len(fact_df) == 0:
        return

    df = fact_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # 0) s'assurer d'avoir un identifiant (TransactionID) — mapping depuis tes CSV
    if "TransactionID" not in df.columns:
        # ton CSV a 'TicketNumber'
        if "TicketNumber" in df.columns:
            df = df.rename(columns={"TicketNumber": "TransactionID"})
        else:
            st.error(f"❌ 'TransactionID' ou 'TicketNumber' introuvable. Colonnes: {list(df.columns)}")
            st.stop()

    expected_cols = {
        "TransactionID": str,
        "ValidationDate": "datetime64[ns]",
        "month": str,
        "OrganisationID": str,
        "CustomerID": str,
        "is_client": bool,
        "CA_TTC": float,
        "CA_HT": float,
        "Purch_Total_HT": float,
        "Qty_Ticket": float,
        "Has_Coupon": bool,
        "CA_paid_with_coupons_HT": float,
        "Estimated_Net_Margin_HT": float,
    }
    for col, typ in expected_cols.items():
        if col not in df.columns:
            df[col] = np.nan

    for col, typ in expected_cols.items():
        if typ == str:
            df[col] = df[col].astype(str)
        elif typ == bool:
            df[col] = df[col].astype("boolean").fillna(False).astype(bool)
        elif typ == "datetime64[ns]":
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif typ == float:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    # ordre exact des colonnes
    cols_db = [r[0] for r in con.execute("PRAGMA table_info('transactions')").fetchall()]
    if "TransactionID" not in cols_db:
        st.error("❌ Table 'transactions' mal initialisée (TransactionID manquant).")
        st.stop()
    df = df[[c for c in cols_db if c in df.columns]]

    # insert only new
    existing_ids_df = con.execute("SELECT TransactionID FROM transactions").fetchdf()
    existing_ids = set(existing_ids_df["TransactionID"]) if not existing_ids_df.empty else set()
    df["TransactionID"] = df["TransactionID"].astype(str)
    new_fact = df.loc[~df["TransactionID"].isin(existing_ids)]
    if new_fact.empty:
        return
    con.append("transactions", new_fact)

def append_coupons(con, cp_df):
    if cp_df is None or len(cp_df) == 0:
        return
    con.append("coupons", cp_df)

# =========================================================
# BUILD TABLES (à partir de tes CSV REELS)
# =========================================================
def _build_fact_from_transactions(tx):
    # Colonnes (d’après tes CSV réels)
    c_txid  = _pick_exact(tx, "TicketNumber")
    c_total = _pick_exact(tx, "TotalAmount")
    c_label = _pick_exact(tx, "Label")
    c_valid = _pick_exact(tx, "ValidationDate")
    c_org   = _pick_exact(tx, "OrganisationId")
    c_cust  = _pick_exact(tx, "CustomerId")
    c_gross = _pick_exact(tx, "LineGrossAmount")             # HT ligne (donné par Keyneo)
    c_cost  = _pick_exact(tx, "lineTotalPurchasingAmount")   # coût HT ligne
    c_qty   = _pick_exact(tx, "Quantity")

    # Sécurités
    need = [c_txid, c_total, c_valid, c_org, c_gross, c_cost]
    if any(c is None for c in need):
        st.error(f"❌ Colonnes clés manquantes dans Transactions. Colonnes reçues: {list(tx.columns)}")
        st.stop()

    # cast numeriques
    for c in [c_total, c_gross, c_cost, c_qty]:
        if c and c in tx.columns:
            tx[c] = pd.to_numeric(tx[c], errors="coerce")

    # agrégations au ticket
    ca_ttc = tx.groupby(c_txid, dropna=False)[c_total].max().rename("CA_TTC").reset_index()
    ca_ht  = tx.groupby(c_txid, dropna=False)[c_gross].sum().rename("CA_HT").reset_index()
    cost   = tx.groupby(c_txid, dropna=False)[c_cost].sum().rename("Purch_Total_HT").reset_index()

    # Quantité par ticket
    if c_qty and tx[c_qty].notna().any():
        qty_ticket = tx.groupby(c_txid)[c_qty].sum().rename("Qty_Ticket").reset_index()
    else:
        qty_ticket = tx.groupby(c_txid)[c_txid].size().rename("Qty_Ticket").reset_index()

    # détection coupon via Label
    has_coupon = (
        tx.assign(_lbl=tx[c_label].fillna("").astype(str).str.upper() if c_label else "")
          .groupby(c_txid)["_lbl"].apply(lambda s: s.str.contains("COUPON", regex=False).any())
          .reset_index(name="Has_Coupon")
    )

    # contexte unique par ticket
    ctx = tx[[c_txid, c_valid, c_org, c_cust]].drop_duplicates(subset=[c_txid]).rename(columns={
        c_txid:"TransactionID", c_valid:"ValidationDate", c_org:"OrganisationID", c_cust:"CustomerID"
    })

    fact = (ca_ttc.merge(ca_ht, on=c_txid)
                 .merge(cost, on=c_txid)
                 .merge(qty_ticket, on=c_txid)
                 .merge(has_coupon, on=c_txid)
                 .merge(ctx, left_on=c_txid, right_on="TransactionID", how="left"))

    fact["ValidationDate"] = _ensure_date(fact["ValidationDate"])
    fact["month"]          = _month_str(fact["ValidationDate"])
    fact["CustomerID"]     = fact["CustomerID"].astype(str).str.strip().replace(["", "nan", "none", "NaN", "None"], np.nan)
    fact["is_client"]      = fact["CustomerID"].notna()
    fact["Estimated_Net_Margin_HT"] = fact["CA_HT"] - fact["Purch_Total_HT"]
    fact["CA_paid_with_coupons_HT"] = np.where(fact["Has_Coupon"], fact["CA_HT"], 0)
    return fact

def _build_coupon_table(cp):
    # Colonnes (d’après ton CSV réel)
    c_couponid = _pick_exact(cp, "CouponId")
    c_init     = _pick_exact(cp, "InitialValue")
    c_rem      = _pick_exact(cp, "Amount")            # montant restant/après utilisation
    c_usedate  = _pick_exact(cp, "UseDate")
    c_emiss    = _pick_exact(cp, "CreationDate")
    c_orgc     = _pick_exact(cp, "OrganisationId")

    # si colonnes minimales manquantes, renvoyer DF vide
    if c_couponid is None or c_orgc is None:
        return pd.DataFrame(columns=["CouponID","OrganisationID","UseDate","EmissionDate","month_use","month_emit","Amount_Initial","Amount_Remaining","Value_Used_Line"])

    cp["UseDate"]      = _ensure_date(cp[c_usedate]) if c_usedate else pd.NaT
    cp["EmissionDate"] = _ensure_date(cp[c_emiss]) if c_emiss else pd.NaT
    cp["Amount_Initial"]   = pd.to_numeric(cp[c_init], errors="coerce").fillna(0) if c_init else 0.0
    cp["Amount_Remaining"] = pd.to_numeric(cp[c_rem], errors="coerce").fillna(0) if c_rem else 0.0
    cp["Value_Used_Line"]  = (cp["Amount_Initial"] - cp["Amount_Remaining"]).clip(lower=0)
    cp["month_use"]  = _month_str(cp["UseDate"])
    cp["month_emit"] = _month_str(cp["EmissionDate"])

    return cp.rename(columns={c_couponid:"CouponID", c_orgc:"OrganisationID"})[
        ["CouponID","OrganisationID","UseDate","EmissionDate","month_use","month_emit","Amount_Initial","Amount_Remaining","Value_Used_Line"]
    ]

# =========================================================
# KPI COMPLET (toutes tes colonnes, ordre figé)
# =========================================================
def compute_kpi(con):
    fact = con.execute("SELECT * FROM transactions").fetch_df()
    if fact.empty:
        return pd.DataFrame()

    grp = ["month","OrganisationID"]

    base = (fact.groupby(grp).agg(
        CA_TTC=("CA_TTC","sum"),
        CA_HT=("CA_HT","sum"),
        Transactions=("TransactionID","nunique"),
        Qty_total=("Qty_Ticket","sum")
    ).reset_index())

    # tx client
    tx_client = (fact[fact["is_client"]==True]
                 .groupby(grp)["TransactionID"]
                 .nunique().reset_index(name="Transaction associé à un client (nombre)"))

    # clients / nouveaux / reviennent / récurrence
    tx_clients = fact[fact["is_client"]==True].copy()
    if not tx_clients.empty:
        first_seen = tx_clients.groupby("CustomerID")["ValidationDate"].min().reset_index(name="FirstDate")
        tx_clients = tx_clients.merge(first_seen, on="CustomerID", how="left")
        tx_clients["IsNewThisMonth"] = tx_clients["ValidationDate"].dt.to_period("M") == tx_clients["FirstDate"].dt.to_period("M")

        clients = tx_clients.groupby(grp)["CustomerID"].nunique().reset_index(name="Client")
        newc    = tx_clients[tx_clients["IsNewThisMonth"]].groupby(grp)["CustomerID"].nunique().reset_index(name="Nouveau client")
        txc     = tx_clients.groupby(grp)["TransactionID"].nunique().reset_index(name="TX_client")

        churn = clients.merge(newc, on=grp, how="left").merge(txc, on=grp, how="left").fillna(0)
        churn["Client qui reviennent"] = churn["Client"] - churn["Nouveau client"]
        churn["Recurrence (combien de fois un client revient par mois en moyenne)"] = np.where(
            churn["Client"]>0, churn["TX_client"]/churn["Client"], np.nan
        )
        churn = churn.drop(columns=["TX_client"])

        # rétention M-1 -> M
        cust_sets = tx_clients.groupby(grp)["CustomerID"].apply(lambda s: set(s.unique())).reset_index(name="CustSet")
        cust_sets["_order"] = pd.PeriodIndex(cust_sets["month"], freq="M").to_timestamp()
        cust_sets = cust_sets.sort_values(["OrganisationID","_order"])
        cust_sets["Prev"] = cust_sets.groupby("OrganisationID")["CustSet"].shift(1)
        cust_sets["Retention_rate"] = cust_sets.apply(
            lambda r: (len(r["Prev"].intersection(r["CustSet"])) / len(r["Prev"])) if isinstance(r["Prev"], set) and len(r["Prev"])>0 else np.nan,
            axis=1
        )
        retention = cust_sets[grp+["Retention_rate"]]
    else:
        churn = pd.DataFrame(columns=grp+["Client","Nouveau client","Client qui reviennent","Recurrence (combien de fois un client revient par mois en moyenne)"])
        retention = pd.DataFrame(columns=grp+["Retention_rate"])

    # CA payé via coupons (HT)
    ca_coupons = fact.groupby(grp)["CA_paid_with_coupons_HT"].sum().reset_index(name="CA paid with coupons")

    # paniers
    panier_global = base.assign(**{
        "Panier moyen HT": np.where(base["Transactions"]>0, base["CA_HT"]/base["Transactions"], np.nan)
    })[grp+["Panier moyen HT"]]

    by_client = fact.groupby(grp+["is_client"]).agg(CA_HT=("CA_HT","sum"), TX=("TransactionID","nunique")).reset_index()
    pm_cli  = by_client[by_client["is_client"]==True].assign(
        **{"Panier moyen client": lambda d: np.where(d["TX"]>0, d["CA_HT"]/d["TX"], np.nan)}
    )[grp+["Panier moyen client"]]
    pm_non  = by_client[by_client["is_client"]==False].assign(
        **{"Panier moyen non client": lambda d: np.where(d["TX"]>0, d["CA_HT"]/d["TX"], np.nan)}
    )[grp+["Panier moyen non client"]]

    by_coupon = fact.groupby(grp+["Has_Coupon"]).agg(CA_HT=("CA_HT","sum"), TX=("TransactionID","nunique")).reset_index()
    pm_avec = by_coupon[by_coupon["Has_Coupon"]==True].assign(
        **{"Panier moyen avec coupon": lambda d: np.where(d["TX"]>0, d["CA_HT"]/d["TX"], np.nan)}
    )[grp+["Panier moyen avec coupon"]]
    pm_sans = by_coupon[by_coupon["Has_Coupon"]==False].assign(
        **{"Panier moyen sans coupon": lambda d: np.where(d["TX"]>0, d["CA_HT"]/d["TX"], np.nan)}
    )[grp+["Panier moyen sans coupon"]]

    # prix moyen article & quantité moyenne par transaction
    prix_moy = fact.groupby(grp).agg(Qty_total=("Qty_Ticket","sum"), CA_HT=("CA_HT","sum")).reset_index()
    prix_moy["Prix moyen article vendu HT"] = np.where(prix_moy["Qty_total"]>0, prix_moy["CA_HT"]/prix_moy["Qty_total"], np.nan)
    prix_moy = prix_moy[grp+["Prix moyen article vendu HT"]]
    qte_moy_tx = fact.groupby(grp)["Qty_Ticket"].mean().reset_index(name="Quantité moyen article par transaction")

    # marge avant
    marge_avant = fact.groupby(grp).apply(lambda d: (d["CA_HT"] - d["Purch_Total_HT"]).sum()).reset_index(name="Marge net HT avant coupon")

    # coupons (depuis table coupons)
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

    # merge final
    kpi = (base
           .merge(tx_client, on=grp, how="left")
           .merge(churn, on=grp, how="left")
           .merge(retention, on=grp, how="left")
           .merge(ca_coupons, on=grp, how="left")
           .merge(coupons_used, on=grp, how="left")
           .merge(coupons_emis, on=grp, how="left")
           .merge(marge_avant, on=grp, how="left")
           .merge(panier_global, on=grp, how="left")
           .merge(pm_cli, on=grp, how="left")
           .merge(pm_non, on=grp, how="left")
           .merge(pm_sans, on=grp, how="left")
           .merge(pm_avec, on=grp, how="left")
           .merge(prix_moy, on=grp, how="left")
           .merge(qte_moy_tx, on=grp, how="left"))

    # ratios
    kpi = kpi.rename(columns={"Transactions":"Transaction (nombre)"})
    kpi["Taux association client"] = np.where(kpi["Transaction (nombre)"]>0,
                                              kpi["Transaction associé à un client (nombre)"]/kpi["Transaction (nombre)"], np.nan)
    kpi["Marge net HT après coupon"] = kpi["Marge net HT avant coupon"] - kpi["Montant coupons utilisé"].fillna(0)
    kpi["Taux de marge HT avant coupon"]  = np.where(kpi["CA_HT"]>0, kpi["Marge net HT avant coupon"]/kpi["CA_HT"], np.nan)
    kpi["Taux de marge HT après coupons"] = np.where(kpi["CA_HT"]>0, kpi["Marge net HT après coupon"]/kpi["CA_HT"], np.nan)
    kpi["ROI_Proxy"] = np.where(kpi["Montant coupons utilisé"]>0,
                                (kpi["CA paid with coupons"] - kpi["Montant coupons utilisé"]) / kpi["Montant coupons utilisé"], np.nan)
    kpi["Taux d'utilisation des bons en montant"]  = np.where(kpi["Montant coupons émis"]>0,
                                                              kpi["Montant coupons utilisé"]/kpi["Montant coupons émis"], np.nan)
    kpi["Taux d'utilisation des bons en quantité"] = np.where(kpi["Coupon émis"]>0,
                                                              kpi["Coupon utilisé"]/kpi["Coupon émis"], np.nan)
    kpi["Taux de CA généré par les bons sur CA HT"] = np.where(kpi["CA_HT"]>0,
                                                               kpi["CA paid with coupons"]/kpi["CA_HT"], np.nan)
    kpi["Voucher_share"] = kpi["Taux de CA généré par les bons sur CA HT"]
    kpi["date (format date)"] = pd.to_datetime(kpi["month"], format="%Y-%m", errors="coerce").dt.strftime("%d/%m/%Y")

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

# =========================================================
# MAIL
# =========================================================
def send_mail(to_list, subject, body):
    if not to_list: return
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(to_list)
    msg.set_content(body)
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls(); server.login(SMTP_USER, SMTP_PASS); server.send_message(msg)

# =========================================================
# UI
# =========================================================
st.sidebar.header("📂 Importer les fichiers")
file_tx = st.sidebar.file_uploader("Transactions (CSV Keyneo)", type=["csv"])
file_cp = st.sidebar.file_uploader("Coupons (CSV Keyneo)", type=["csv"])
emails_supp = st.sidebar.text_input("📧 Autres destinataires (séparés par des virgules)")

if file_tx and file_cp:
    # 1) Lecture
    tx_csv = _norm_cols(_read_csv_tolerant(file_tx))
    cp_csv = _norm_cols(_read_csv_tolerant(file_cp))

    # 2) Build tables à partir de TES colonnes réelles
    fact    = _build_fact_from_transactions(tx_csv)
    coupons = _build_coupon_table(cp_csv)

    # 3) DuckDB upsert
    con = init_duckdb()
    before_tx = con.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    upsert_transactions(con, fact)
    after_tx = con.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    new_tx = after_tx - before_tx

    before_cp = con.execute("SELECT COUNT(*) FROM coupons").fetchone()[0]
    append_coupons(con, coupons)
    after_cp = con.execute("SELECT COUNT(*) FROM coupons").fetchone()[0]
    new_cp = after_cp - before_cp

    st.success(f"✅ Base mise à jour : +{new_tx} transactions, +{new_cp} coupons")
    st.info(f"📦 Historique total : {after_tx} transactions / {after_cp} coupons")

    # 4) KPI
    kpi = compute_kpi(con)
    if kpi.empty:
        st.warning("Aucun KPI calculé — vérifie les fichiers importés.")
    else:
        with st.expander("👀 Aperçu KPI (15 lignes)"):
            st.dataframe(kpi.head(15))

        # 5) Export Google Sheets
        ws_kpi = ws_open_or_create(SPREADSHEET_ID, SHEET_KPI)
        ws_overwrite_small(ws_kpi, kpi)
        st.success(f"📤 KPI_Mensuels mis à jour dans Google Sheets ({len(kpi)} lignes).")

        # 6) Email (bouton)
        if st.button("📧 Envoyer le lien par e-mail"):
            link = LOOKER_URL or f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
            recipients = [DEFAULT_RECEIVER] + [e.strip() for e in emails_supp.split(",") if e.strip()]
            subject = f"📊 Rapport fidélité — KPI mis à jour ({datetime.today().strftime('%d/%m/%Y')})"
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
