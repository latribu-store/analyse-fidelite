import streamlit as st
import pandas as pd
import numpy as np
import json, requests, re, smtplib
from datetime import datetime
from email.message import EmailMessage
import gspread
from google.oauth2 import service_account

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
st.set_page_config(page_title="Analyse Fidélité - KPI (complet)", layout="wide")
st.title("🎯 Analyse Fidélité — KPI mensuels (liste complète)")

SPREADSHEET_ID = "1xYQ0mjr37Fnmal8yhi0kBE3_96EN6H6qsYrAtQkHpGo"
SHEET_KPI      = "KPI_Mensuels"
LOOKER_URL     = "https://lookerstudio.google.com/reporting/a0037205-4433-4f9e-b19b-1601fbf24006"

SMTP_SERVER = st.secrets["email"]["smtp_server"]
SMTP_PORT   = st.secrets["email"]["smtp_port"]
SMTP_USER   = st.secrets["email"]["smtp_user"]
SMTP_PASS   = st.secrets["email"]["smtp_password"]
DEFAULT_RECEIVER = st.secrets["email"]["receiver"]

# === Auth Google (service account stocké sur Drive) ===
file_id = "12O9eFGFmwTu1n6kF4AIDIm0KXKMIgOvg"
url = f"https://drive.google.com/uc?id={file_id}"
resp = requests.get(url); resp.raise_for_status()
gcp_service_account_info = json.loads(resp.content)
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = service_account.Credentials.from_service_account_info(gcp_service_account_info, scopes=scopes)
client = gspread.authorize(creds)

# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------
def _ensure_date(s): return pd.to_datetime(s, errors="coerce")
def _month_str(s): return _ensure_date(s).dt.to_period("M").astype(str)
def _norm_cols(df): return df.rename(columns={c: re.sub(r"[^a-z0-9]", "", c.lower()) for c in df.columns})
def _pick(df, *cands):
    for c in cands:
        k = re.sub(r"[^a-z0-9]", "", c.lower())
        if k in df.columns: return k
    return None
def _open_or_create(sheet_id, tab_name):
    sh = client.open_by_key(sheet_id)
    try: return sh.worksheet(tab_name)
    except Exception: return sh.add_worksheet(title=tab_name, rows=2, cols=160)
def _sanitize_df_for_sheets(df):
    d = df.copy()
    for c in d.columns:
        if np.issubdtype(d[c].dtype, np.datetime64):
            d[c] = pd.to_datetime(d[c], errors="coerce").dt.strftime("%Y-%m-%d")
    return d.where(pd.notnull(d), "")
def _update_ws(ws, df):
    safe = _sanitize_df_for_sheets(df)
    ws.clear()
    ws.update("A1", [list(safe.columns)] + safe.values.tolist(), value_input_option="USER_ENTERED")

def _read_csv_tolerant(uploaded):
    df = pd.read_csv(uploaded, sep=";", encoding="utf-8-sig", on_bad_lines="skip", engine="python", dtype=str)
    # hygiène
    for c in df.columns:
        df[c] = df[c].astype(str).str.replace("'", "", regex=False).str.strip()
    # conversion douce : si >70% de la colonne est numérique, on convertit (en remplaçant , par .)
    for c in df.columns:
        num = pd.to_numeric(df[c].str.replace(",", ".", regex=False), errors="coerce")
        if num.notna().mean() > 0.7:
            df[c] = num
    return df

# ------------------------------------------------------------
# UI
# ------------------------------------------------------------
st.sidebar.header("📂 Importer les fichiers")
file_tx = st.sidebar.file_uploader("Transactions (CSV Keyneo)", type=["csv"])
file_cp = st.sidebar.file_uploader("Coupons (CSV Keyneo)", type=["csv"])
emails_supp = st.sidebar.text_input("📧 Autres destinataires (séparés par des virgules)")

if file_tx and file_cp:
    # 1) Lecture & normalisation
    tx = _norm_cols(_read_csv_tolerant(file_tx))
    cp = _norm_cols(_read_csv_tolerant(file_cp))

    # Colonnes clés (avec fallback)
    c_txid  = _pick(tx, "ticketnumber", "transactionid", "operationid")
    c_total = _pick(tx, "totalamount", "totaltcc", "totalttc")                # TTC ticket
    c_label = _pick(tx, "label", "libelle", "designation")
    c_valid = _pick(tx, "validationdate", "operationdate", "date")
    c_org   = _pick(tx, "organisationid", "organizationid")
    c_cust  = _pick(tx, "customerid", "clientid")
    c_gross = _pick(tx, "linegrossamount", "montanthtligne", "cahtligne", "montantht")  # HT ligne
    c_cost  = _pick(tx, "linetotalpurchasingamount", "purchasingamount", "achatht")
    c_qty   = _pick(tx, "quantity", "qty", "linequantity", "quantite")

    # conversions
    for c in [c_total, c_gross, c_cost, c_qty]:
        if c and c in tx.columns:
            tx[c] = pd.to_numeric(tx[c], errors="coerce")

    # 2) Fact table par ticket
    ca_ttc = tx.groupby(c_txid, dropna=False)[c_total].max().rename("CA_TTC").reset_index()
    ca_ht  = tx.groupby(c_txid, dropna=False)[c_gross].sum().rename("CA_HT").reset_index()
    cost   = tx.groupby(c_txid, dropna=False)[c_cost].sum().rename("Purch_Total_HT").reset_index()

    if c_qty and c_qty in tx.columns and tx[c_qty].notna().any():
        qty_ticket = tx.groupby(c_txid)[c_qty].sum().rename("Qty_Ticket").reset_index()
    else:
        # fallback : nombre de lignes par ticket = proxy quantité
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

    # Nettoyage fort des IDs clients (vrai NA si vide/'nan'/'none')
    fact["CustomerID"] = fact["CustomerID"].astype(str).str.strip().replace(
        ["", "nan", "none", "NaN", "None"], np.nan
    )
    fact["is_client"] = fact["CustomerID"].notna()

    # Indicateurs ticket
    fact["CA_paid_with_coupons_HT"] = np.where(fact["Has_Coupon"], fact["CA_HT"], 0.0)
    fact["Marge_HT"] = fact["CA_HT"] - fact["Purch_Total_HT"]

    # 3) Coupons (émis / utilisés)
    c_couponid = _pick(cp, "couponid", "id")
    c_init     = _pick(cp, "initialvalue", "valeurinitiale", "montantinit")
    c_rem      = _pick(cp, "amount", "remaining", "reste")
    c_usedate  = _pick(cp, "usedate", "dateutilisation")
    c_emiss    = _pick(cp, "creationdate", "datecreation")
    c_orgc     = _pick(cp, "organisationid", "organizationid")

    if c_couponid and c_orgc:
        cp["UseDate"] = _ensure_date(cp[c_usedate]) if c_usedate else pd.NaT
        cp["EmissionDate"] = _ensure_date(cp[c_emiss]) if c_emiss else pd.NaT
        cp["Amount_Initial"]   = pd.to_numeric(cp[c_init].astype(str).str.replace(",", ".", regex=False), errors="coerce").fillna(0) if c_init else 0.0
        cp["Amount_Remaining"] = pd.to_numeric(cp[c_rem].astype(str).str.replace(",", ".", regex=False), errors="coerce").fillna(0) if c_rem else 0.0
        cp["Value_Used_Line"]  = (cp["Amount_Initial"] - cp["Amount_Remaining"]).clip(lower=0)
        cp["month_use"]  = _month_str(cp["UseDate"])
        cp["month_emit"] = _month_str(cp["EmissionDate"])
        cp = cp.rename(columns={c_couponid:"CouponID", c_orgc:"OrganisationID"})

        coupons_used = cp.dropna(subset=["UseDate"]).groupby(["month_use","OrganisationID"]).agg(
            **{"Coupon utilisé":("CouponID","nunique"),
               "Montant coupons utilisé":("Value_Used_Line","sum")}
        ).reset_index().rename(columns={"month_use":"month"})

        coupons_emis = cp.dropna(subset=["EmissionDate"]).groupby(["month_emit","OrganisationID"]).agg(
            **{"Coupon émis":("CouponID","nunique"),
               "Montant coupons émis":("Amount_Initial","sum")}
        ).reset_index().rename(columns={"month_emit":"month"})
    else:
        coupons_used = pd.DataFrame(columns=["month","OrganisationID","Coupon utilisé","Montant coupons utilisé"])
        coupons_emis = pd.DataFrame(columns=["month","OrganisationID","Coupon émis","Montant coupons émis"])

    # 4) KPI mensuels — toutes les colonnes demandées
    grp = ["month","OrganisationID"]

    base = fact.groupby(grp).agg(
        **{
            "CA TTC": ("CA_TTC","sum"),
            "CA HT": ("CA_HT","sum"),
            "Transactions (count)": ("TransactionID","nunique"),
            "Qty_total": ("Qty_Ticket","sum")
        }
    ).reset_index()

    # Transactions associées à un client (nunique de tickets côté clients)
    tx_client = fact[fact["is_client"]==True].groupby(grp)["TransactionID"].nunique().reset_index(
        name="Transaction associé à un client (nombre)"
    )

    # Clients / nouveaux / reviennent / récurrence
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
    else:
        churn = pd.DataFrame(columns=grp+["Client","Nouveau client","Client qui reviennent","Recurrence (combien de fois un client revient par mois en moyenne)"])

    # Rétention M-1 → M (intersection des sets)
    if not tx_clients.empty:
        cs = tx_clients.groupby(grp)["CustomerID"].apply(lambda s: set(s.unique())).reset_index(name="CustSet")
        cs["_order"] = pd.PeriodIndex(cs["month"], freq="M").to_timestamp()
        cs = cs.sort_values(["OrganisationID","_order"])
        cs["Prev"] = cs.groupby("OrganisationID")["CustSet"].shift(1)
        cs["Retention_rate"] = cs.apply(
            lambda r: (len(r["Prev"].intersection(r["CustSet"])) / len(r["Prev"])) if isinstance(r["Prev"], set) and len(r["Prev"])>0 else np.nan,
            axis=1
        )
        retention = cs[grp+["Retention_rate"]]
    else:
        retention = pd.DataFrame(columns=grp+["Retention_rate"])

    # CA payé avec coupons (HT)
    ca_coupons = fact.groupby(grp)["CA_paid_with_coupons_HT"].sum().reset_index(name="CA paid with coupons")

    # Paniers moyens
    # Global
    panier_global = base.assign(**{
        "Panier moyen HT": np.where(base["Transactions (count)"]>0, base["CA HT"]/base["Transactions (count)"], np.nan)
    })[grp+["Panier moyen HT"]]

    # Client vs non client
    by_client = fact.groupby(grp+["is_client"]).agg(CA_HT=("CA_HT","sum"), TX=("TransactionID","nunique")).reset_index()
    pm_cli  = by_client[by_client["is_client"]==True].assign(**{
        "Panier moyen client": lambda d: np.where(d["TX"]>0, d["CA_HT"]/d["TX"], np.nan)
    })[grp+["Panier moyen client"]]
    pm_non  = by_client[by_client["is_client"]==False].assign(**{
        "Panier moyen non client": lambda d: np.where(d["TX"]>0, d["CA_HT"]/d["TX"], np.nan)
    })[grp+["Panier moyen non client"]]

    # Avec / sans coupon
    by_coupon = fact.groupby(grp+["Has_Coupon"]).agg(CA_HT=("CA_HT","sum"), TX=("TransactionID","nunique")).reset_index()
    pm_avec = by_coupon[by_coupon["Has_Coupon"]==True].assign(**{
        "Panier moyen avec coupon": lambda d: np.where(d["TX"]>0, d["CA_HT"]/d["TX"], np.nan)
    })[grp+["Panier moyen avec coupon"]]
    pm_sans = by_coupon[by_coupon["Has_Coupon"]==False].assign(**{
        "Panier moyen sans coupon": lambda d: np.where(d["TX"]>0, d["CA_HT"]/d["TX"], np.nan)
    })[grp+["Panier moyen sans coupon"]]

    # Prix moyen article HT & Quantité moyenne article / transaction
    prix_moy = fact.groupby(grp).agg(Qty_total=("Qty_Ticket","sum"), CA_HT=("CA_HT","sum")).reset_index()
    prix_moy["Prix moyen article vendu HT"] = np.where(prix_moy["Qty_total"]>0, prix_moy["CA_HT"]/prix_moy["Qty_total"], np.nan)
    prix_moy = prix_moy[grp+["Prix moyen article vendu HT"]]

    qte_moy_tx = fact.groupby(grp)["Qty_Ticket"].mean().reset_index(name="Quantité moyen article par transaction")

    # Marge avant / après coupon
    marge_avant = fact.groupby(grp).apply(lambda d: (d["CA_HT"] - d["Purch_Total_HT"]).sum()).reset_index(name="Marge net HT avant coupon")

    # Assemblage KPI
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
           .merge(qte_moy_tx, on=grp, how="left")
           )

    # Post-calculs & ratios
    kpi = kpi.rename(columns={"Transactions (count)":"Transaction (nombre)"})
    kpi["Taux association client"] = np.where(kpi["Transaction (nombre)"]>0,
                                              kpi["Transaction associé à un client (nombre)"]/kpi["Transaction (nombre)"], np.nan)
    kpi["Marge net HT après coupon"] = kpi["Marge net HT avant coupon"] - kpi["Montant coupons utilisé"].fillna(0)
    kpi["Taux de marge HT avant coupon"]  = np.where(kpi["CA HT"]>0, kpi["Marge net HT avant coupon"]/kpi["CA HT"], np.nan)
    kpi["Taux de marge HT après coupons"] = np.where(kpi["CA HT"]>0, kpi["Marge net HT après coupon"]/kpi["CA HT"], np.nan)
    kpi["ROI_Proxy"] = np.where(kpi["Montant coupons utilisé"]>0,
                                (kpi["CA paid with coupons"] - kpi["Montant coupons utilisé"]) / kpi["Montant coupons utilisé"],
                                np.nan)
    kpi["Taux d'utilisation des bons en montant"]  = np.where(kpi["Montant coupons émis"]>0,
                                                              kpi["Montant coupons utilisé"]/kpi["Montant coupons émis"], np.nan)
    kpi["Taux d'utilisation des bons en quantité"] = np.where(kpi["Coupon émis"]>0,
                                                              kpi["Coupon utilisé"]/kpi["Coupon émis"], np.nan)
    kpi["Taux de CA généré par les bons sur CA HT"] = np.where(kpi["CA HT"]>0,
                                                               kpi["CA paid with coupons"]/kpi["CA HT"], np.nan)
    kpi["Voucher_share"] = kpi["Taux de CA généré par les bons sur CA HT"]
    kpi["date (format date)"] = pd.to_datetime(kpi["month"], format="%Y-%m", errors="coerce").dt.strftime("%d/%m/%Y")

    # Colonnes finales EXACTES (ordre figé)
    final_cols = [
        "month", "OrganisationID",
        "CA TTC", "CA HT", "CA paid with coupons",
        "Coupon émis", "Coupon utilisé", "Montant coupons émis", "Montant coupons utilisé",
        "Transaction (nombre)", "Transaction associé à un client (nombre)",
        "Client", "Nouveau client", "Client qui reviennent",
        "Recurrence (combien de fois un client revient par mois en moyenne)",
        "Retention_rate", "Taux association client",
        "Marge net HT avant coupon", "Marge net HT après coupon",
        "Taux de marge HT avant coupon", "Taux de marge HT après coupons", "ROI_Proxy",
        "Panier moyen HT", "Panier moyen client", "Panier moyen non client",
        "Panier moyen sans coupon", "Panier moyen avec coupon",
        "Taux d'utilisation des bons en montant", "Taux d'utilisation des bons en quantité",
        "Taux de CA généré par les bons sur CA HT",
        "Prix moyen article vendu HT", "Quantité moyen article par transaction",
        "Voucher_share", "date (format date)"
    ]
    # S’assure que tout existe
    for col in final_cols:
        if col not in kpi.columns:
            kpi[col] = np.nan

    kpi = kpi[final_cols].sort_values(["OrganisationID","month"]).reset_index(drop=True)

    # 5) Export Google Sheets
    ws_kpi = _open_or_create(SPREADSHEET_ID, SHEET_KPI)
    _update_ws(ws_kpi, kpi)

    st.success(f"✅ KPI_Mensuels mis à jour ({len(kpi)} lignes) avec TOUTES les colonnes demandées.")
    with st.expander("👀 Aperçu KPI (10 lignes)"):
        st.dataframe(kpi.head(10))

    # 6) Envoi e-mail (lien Looker)
    if st.button("📤 Envoyer le lien Looker par e-mail"):
        recipients = [DEFAULT_RECEIVER] + [e.strip() for e in emails_supp.split(",") if e.strip()]
        msg = EmailMessage()
        msg["Subject"] = f"📊 Rapport fidélité La Tribu - {datetime.today().strftime('%d/%m/%Y')}"
        msg["From"] = SMTP_USER
        msg["To"] = ", ".join(recipients)
        msg.set_content(f"""Bonjour,

Le tableau de bord fidélité La Tribu a été mis à jour.

👉 Accès : {LOOKER_URL}

Bien à vous,
L’équipe La Tribu""")
        try:
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USER, SMTP_PASS)
                server.send_message(msg)
            st.success("📧 Mail Looker envoyé.")
        except Exception as e:
            st.error(f"❌ Erreur d’envoi mail : {e}")
else:
    st.info("➡️ Importez les CSV Transactions et Coupons pour démarrer.")
