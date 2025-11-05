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
st.set_page_config(page_title="Analyse Fidélité - La Tribu (KPI exact)", layout="wide")
st.title("🎯 Analyse Fidélité - La Tribu — KPI mensuels exacts (colonnes figées)")

SPREADSHEET_ID = "1xYQ0mjr37Fnmal8yhi0kBE3_96EN6H6qsYrAtQkHpGo"
SHEET_KPI      = "KPI_Mensuels"
LOOKER_URL     = "https://lookerstudio.google.com/reporting/a0037205-4433-4f9e-b19b-1601fbf24006"

SMTP_SERVER = st.secrets["email"]["smtp_server"]
SMTP_PORT   = st.secrets["email"]["smtp_port"]
SMTP_USER   = st.secrets["email"]["smtp_user"]
SMTP_PASS   = st.secrets["email"]["smtp_password"]
DEFAULT_RECEIVER = st.secrets["email"]["receiver"]

# === Auth Google ===
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
    except Exception: return sh.add_worksheet(title=tab_name, rows=2, cols=120)

def _sanitize_df_for_sheets(df):
    safe = df.copy()
    for col in safe.columns:
        if np.issubdtype(safe[col].dtype, np.datetime64):
            safe[col] = pd.to_datetime(safe[col], errors="coerce").dt.strftime("%Y-%m-%d")
    safe = safe.where(pd.notnull(safe), "")
    return safe

def _update_ws(ws, df):
    safe = _sanitize_df_for_sheets(df)
    ws.clear()
    ws.update("A1", [list(safe.columns)] + safe.values.tolist(), value_input_option="USER_ENTERED")

def _read_csv_tolerant(uploaded):
    df = pd.read_csv(uploaded, sep=";", encoding="utf-8-sig", on_bad_lines="skip", engine="python", dtype=str)
    # hygiène de base
    for col in df.columns:
        df[col] = (
            df[col].astype(str)
            .str.replace("'", "", regex=False)
            .str.strip()
        )
    # tentative numérisation (on NE remplace PAS ici la virgule par un point pour ne pas casser des codes/IDs)
    for col in df.columns:
        # on crée aussi une version num si possible (utile pour montants)
        try_num = pd.to_numeric(df[col].str.replace(",", ".", regex=False), errors="coerce")
        # si >70% convertible, on remplace par la version num
        if try_num.notna().mean() > 0.7:
            df[col] = try_num
    return df

# ------------------------------------------------------------
# UI
# ------------------------------------------------------------
st.sidebar.header("📂 Importer les fichiers")
file_tx = st.sidebar.file_uploader("Transactions (CSV Keyneo)", type=["csv"])
file_cp = st.sidebar.file_uploader("Coupons (CSV Keyneo)", type=["csv"])
emails_supp = st.sidebar.text_input("📧 Autres destinataires (séparés par des virgules)")

if file_tx and file_cp:
    # =====================================================
    # 1) LECTURE & NORMALISATION
    # =====================================================
    tx = _norm_cols(_read_csv_tolerant(file_tx))
    cp = _norm_cols(_read_csv_tolerant(file_cp))

    # --- Colonnes clés transactions ---
    c_txid  = _pick(tx, "ticketnumber", "transactionid", "operationid")
    c_total = _pick(tx, "totalamount", "totaltcc", "totalttc")                     # TTC ticket
    c_label = _pick(tx, "label", "libelle", "designation")
    c_valid = _pick(tx, "validationdate", "operationdate", "date")
    c_org   = _pick(tx, "organisationid", "organizationid")
    c_cust  = _pick(tx, "customerid", "clientid")
    c_gross = _pick(tx, "linegrossamount", "montanthtligne", "cahtligne", "montantht")   # HT ligne
    c_cost  = _pick(tx, "linetotalpurchasingamount", "purchasingamount", "achatht")
    c_qty   = _pick(tx, "quantity", "qty", "linequantity", "quantite")

    # exigences minimales
    if not all([c_txid, c_total, c_valid, c_org]):
        st.error("❌ Colonnes minimales manquantes (TransactionID/TTC/Date/Organisation).")
        st.stop()

    # conversions sécurisées
    for c in [c_total, c_gross, c_cost, c_qty]:
        if c and c in tx.columns:
            tx[c] = pd.to_numeric(tx[c], errors="coerce")

    # =====================================================
    # 2) FACT TABLE PAR TICKET
    # =====================================================
    # montants par ticket
    ca_ttc = tx.groupby(c_txid, dropna=False)[c_total].max().rename("CA_TTC").reset_index()
    ca_ht_ticket = tx.groupby(c_txid, dropna=False)[c_gross].sum().rename("CA_HT").reset_index()
    cost_ticket  = tx.groupby(c_txid, dropna=False)[c_cost].sum().rename("Purch_Total_HT").reset_index()

    # quantité par ticket
    if c_qty and c_qty in tx.columns and tx[c_qty].notna().any():
        qty_ticket = tx.groupby(c_txid)[c_qty].sum().rename("Qty_Ticket").reset_index()
    else:
        qty_ticket = tx.groupby(c_txid)[c_txid].size().rename("Qty_Ticket").reset_index()

    # coupon flag
    has_coupon = (
        tx.assign(_lbl=tx[c_label].fillna("").astype(str).str.upper() if c_label else "")
          .groupby(c_txid)["_lbl"].apply(lambda s: s.str.contains("COUPON", regex=False).any())
          .reset_index(name="Has_Coupon")
    )

    # contexte
    ctx_cols = [c_txid, c_valid, c_org] + ([c_cust] if c_cust else [])
    ctx = tx[ctx_cols].drop_duplicates(subset=[c_txid]).rename(columns={
        c_txid:"TransactionID", c_valid:"ValidationDate", c_org:"OrganisationID", c_cust:"CustomerID" if c_cust else None
    })

    fact = (ca_ttc.merge(ca_ht_ticket, on=c_txid)
                 .merge(cost_ticket, on=c_txid)
                 .merge(qty_ticket, on=c_txid)
                 .merge(has_coupon, on=c_txid)
                 .merge(ctx, left_on=c_txid, right_on="TransactionID", how="left"))

    fact["ValidationDate"] = _ensure_date(fact["ValidationDate"])
    fact["month"] = _month_str(fact["ValidationDate"])

    # CA payé avec coupon (on passe en HT pour cohérence des ratios sur CA_HT)
    # ici: si le ticket contient un coupon → part entière du CA_HT affectée aux coupons (proxy conservateur)
    fact["CA_paid_with_coupons_HT"] = np.where(fact["Has_Coupon"], fact["CA_HT"], 0.0)

    # Splits client / non client
    fact["is_client"] = fact["CustomerID"].notna()

    # =====================================================
    # 3) COUPONS (émis / utilisés)
    # =====================================================
    c_couponid = _pick(cp, "couponid", "id")
    c_init     = _pick(cp, "initialvalue", "valeurinitiale", "montantinit")
    c_rem      = _pick(cp, "amount", "remaining", "reste")
    c_usedate  = _pick(cp, "usedate", "dateutilisation")
    c_emiss    = _pick(cp, "creationdate", "datecreation")
    c_orgc     = _pick(cp, "organisationid", "organizationid")

    if c_couponid and c_orgc:
        cp["UseDate"]      = _ensure_date(cp[c_usedate]) if c_usedate else pd.NaT
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

    # =====================================================
    # 4) AGRÉGATS KPI — EXACTEMENT LES COLONNES DEMANDÉES
    # =====================================================
       
     # Calcul des agrégats de base sans placeholder invalides
    base = fact.groupby(grp).agg(
        **{
            "CA TTC": ("CA_TTC","sum"),
            "CA HT": ("CA_HT","sum"),
            "Quantité total": ("Qty_Ticket","sum"),
            "Transaction (nombre)": ("TransactionID","nunique")
        }
    ).reset_index()


    # comme agg lambda sur DataFrame n'est pas simple, on recalcule proprement :
    base = fact.groupby(grp).agg(
        **{
            "CA TTC": ("CA_TTC","sum"),
            "CA HT": ("CA_HT","sum"),
            "Quantité total": ("Qty_Ticket","sum"),
            "Transaction (nombre)": ("TransactionID","nunique")
        }
    ).reset_index()

    tx_client = fact[fact["is_client"]==True].groupby(grp)["TransactionID"].nunique().reset_index(name="Transaction associé à un client (nombre)")

    # Clients / Nouveaux / Reviennent / Récurrence
    tx_clients = fact[fact["is_client"]==True].copy()
    if not tx_clients.empty:
        first_seen = tx_clients.groupby("CustomerID")["ValidationDate"].min().reset_index(name="FirstDate")
        tx_clients = tx_clients.merge(first_seen, on="CustomerID", how="left")
        tx_clients["IsNewThisMonth"] = tx_clients["ValidationDate"].dt.to_period("M") == tx_clients["FirstDate"].dt.to_period("M")

        clients = tx_clients.groupby(grp)["CustomerID"].nunique().reset_index(name="Client")
        newc    = tx_clients[tx_clients["IsNewThisMonth"]].groupby(grp)["CustomerID"].nunique().reset_index(name="Nouveau client")
        txc     = tx_clients.groupby(grp)["TransactionID"].nunique().reset_index(name="Transactions_Client_tmp")

        churn = clients.merge(newc, on=grp, how="left").merge(txc, on=grp, how="left").fillna(0)
        churn["Client qui reviennent"] = churn["Client"] - churn["Nouveau client"]
        churn["Recurrence (combien de fois un client revient par mois en moyenne)"] = np.where(
            churn["Client"]>0, churn["Transactions_Client_tmp"]/churn["Client"], np.nan
        )
        churn = churn.drop(columns=["Transactions_Client_tmp"])
    else:
        churn = pd.DataFrame(columns=grp+["Client","Nouveau client","Client qui reviennent","Recurrence (combien de fois un client revient par mois en moyenne)"])

    # Rétention (Moins-1 -> M)
    if not tx_clients.empty:
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
        retention = pd.DataFrame(columns=grp+["Retention_rate"])

    # CA payé avec coupons (HT)
    ca_coupons_ht = fact.groupby(grp)["CA_paid_with_coupons_HT"].sum().reset_index(name="CA paid with coupons")

    # Paniers
    # - Panier moyen HT (global)
    panier_moy_ht = base.assign(**{
        "Panier moyen HT": np.where(base["Transaction (nombre)"]>0, base["CA HT"]/base["Transaction (nombre)"], np.nan)
    })[grp+["Panier moyen HT"]]

    # - Panier moyen client / non client
    by_is_client = fact.groupby(grp+["is_client"]).agg(CA_HT=("CA_HT","sum"), TX=("TransactionID","nunique")).reset_index()
    pm_client = by_is_client[by_is_client["is_client"]==True].copy()
    pm_client["Panier moyen client"] = np.where(pm_client["TX"]>0, pm_client["CA_HT"]/pm_client["TX"], np.nan)
    pm_client = pm_client[grp+["Panier moyen client"]]
    pm_non = by_is_client[by_is_client["is_client"]==False].copy()
    pm_non["Panier moyen non client"] = np.where(pm_non["TX"]>0, pm_non["CA_HT"]/pm_non["TX"], np.nan)
    pm_non = pm_non[grp+["Panier moyen non client"]]

    # - Panier moyen sans / avec coupon
    by_coupon = fact.groupby(grp+["Has_Coupon"]).agg(CA_HT=("CA_HT","sum"), TX=("TransactionID","nunique")).reset_index()
    pm_avec = by_coupon[by_coupon["Has_Coupon"]==True].copy()
    pm_avec["Panier moyen avec coupon"] = np.where(pm_avec["TX"]>0, pm_avec["CA_HT"]/pm_avec["TX"], np.nan)
    pm_avec = pm_avec[grp+["Panier moyen avec coupon"]]
    pm_sans = by_coupon[by_coupon["Has_Coupon"]==False].copy()
    pm_sans["Panier moyen sans coupon"] = np.where(pm_sans["TX"]>0, pm_sans["CA_HT"]/pm_sans["TX"], np.nan)
    pm_sans = pm_sans[grp+["Panier moyen sans coupon"]]

    # Prix moyen article vendu HT & Quantité moyenne article par transaction
    prix_moy_art = fact.groupby(grp).agg(Qty_total=("Qty_Ticket","sum"), CA_HT=("CA_HT","sum")).reset_index()
    prix_moy_art["Prix moyen article vendu HT"] = np.where(prix_moy_art["Qty_total"]>0, prix_moy_art["CA_HT"]/prix_moy_art["Qty_total"], np.nan)
    prix_moy_art = prix_moy_art[grp+["Prix moyen article vendu HT"]]

    qte_moy_tx = fact.groupby(grp)["Qty_Ticket"].mean().reset_index(name="Quantité moyen article par transaction")

    # Marge
    marge_avant = fact.groupby(grp).apply(lambda d: (d["CA_HT"] - d["Purch_Total_HT"]).sum()).reset_index(name="Marge net HT avant coupon")
    # Après coupon = avant - montant coupons utilisé (montant utilisé provient de la table coupons)
    # on fusionnera plus bas une fois les montants de coupons groupés.

    # Assemble progressif
    kpi = base.drop(columns=["Quantité total"]).merge(tx_client, on=grp, how="left") \
             .merge(churn, on=grp, how="left") \
             .merge(retention, on=grp, how="left") \
             .merge(ca_coupons_ht, on=grp, how="left") \
             .merge(panier_moy_ht, on=grp, how="left") \
             .merge(pm_client, on=grp, how="left") \
             .merge(pm_non, on=grp, how="left") \
             .merge(pm_sans, on=grp, how="left") \
             .merge(pm_avec, on=grp, how="left") \
             .merge(prix_moy_art, on=grp, how="left") \
             .merge(qte_moy_tx, on=grp, how="left") \
             .merge(marge_avant, on=grp, how="left") \
             .merge(coupons_used, on=grp, how="left") \
             .merge(coupons_emis, on=grp, how="left")

    # Montants coupons NaN -> 0
    for c in ["Coupon émis","Montant coupons émis","Coupon utilisé","Montant coupons utilisé"]:
        if c in kpi.columns:
            kpi[c] = kpi[c].fillna(0)

    # Marge après coupon
    kpi["Marge net HT après coupon"] = kpi["Marge net HT avant coupon"] - kpi["Montant coupons utilisé"].fillna(0)

    # Taux marge avant/après
    kpi["Taux de marge HT avant coupon"]  = np.where(kpi["CA HT"]>0, kpi["Marge net HT avant coupon"]/kpi["CA HT"], np.nan)
    kpi["Taux de marge HT après coupons"] = np.where(kpi["CA HT"]>0, kpi["Marge net HT après coupon"]/kpi["CA HT"], np.nan)

    # ROI proxy
    kpi["ROI_Proxy"] = np.where(kpi["Montant coupons utilisé"]>0,
                                (kpi["CA paid with coupons"] - kpi["Montant coupons utilisé"]) / kpi["Montant coupons utilisé"],
                                np.nan)

    # Taux d’utilisation des bons
    kpi["Taux d'utilisation des bons en montant"]  = np.where(kpi["Montant coupons émis"]>0,
                                                              kpi["Montant coupons utilisé"]/kpi["Montant coupons émis"], np.nan)
    kpi["Taux d'utilisation des bons en quantité"] = np.where(kpi["Coupon émis"]>0,
                                                              kpi["Coupon utilisé"]/kpi["Coupon émis"], np.nan)

    # Taux de CA généré par les bons (et Voucher_share = même définition)
    kpi["Taux de CA généré par les bons sur CA HT"] = np.where(kpi["CA HT"]>0,
                                                               kpi["CA paid with coupons"]/kpi["CA HT"], np.nan)
    kpi["Voucher_share"] = kpi["Taux de CA généré par les bons sur CA HT"]

    # Taux d’association client
    kpi["Taux association client"] = np.where(kpi["Transaction (nombre)"]>0,
                                              kpi["Transaction associé à un client (nombre)"]/kpi["Transaction (nombre)"], np.nan)

    # Date (1er jour du mois, format date en string jj/mm/aaaa)
    kpi["date (format date)"] = pd.to_datetime(kpi["month"], format="%Y-%m", errors="coerce").dt.strftime("%d/%m/%Y")

    # Colonnes FINALES EXACTES + ordre demandé
    final_cols = [
        "month",
        "OrganisationID",
        "CA TTC",
        "CA HT",
        "CA paid with coupons",
        "Coupon émis",
        "Coupon utilisé",
        "Montant coupons émis",
        "Montant coupons utilisé",
        "Transaction (nombre)",
        "Transaction associé à un client (nombre)",
        "Client",
        "Nouveau client",
        "Client qui reviennent",
        "Recurrence (combien de fois un client revient par mois en moyenne)",
        "Retention_rate",
        "Taux association client",
        "Marge net HT avant coupon",
        "Marge net HT après coupon",
        "Taux de marge HT avant coupon",
        "Taux de marge HT après coupons",
        "ROI_Proxy",
        "Panier moyen HT",
        "Panier moyen client",
        "Panier moyen non client",
        "Panier moyen sans coupon",
        "Panier moyen avec coupon",
        "Taux d'utilisation des bons en montant",
        "Taux d'utilisation des bons en quantité",
        "Taux de CA généré par les bons sur CA HT",
        "Prix moyen article vendu HT",
        "Quantité moyen article par transaction",
        "Voucher_share",
        "date (format date)"
    ]

    # mapping sources vers noms finaux
    rename_map = {
        "CA paid with coupons":"CA paid with coupons",  # déjà bon
        "Retention_rate":"Retention_rate",
        "Prix moyen article vendu HT":"Prix moyen article vendu HT",
        "Quantité moyen article par transaction":"Quantité moyen article par transaction"
    }

    # Harmonisation noms déjà présents
    kpi = kpi.rename(columns=rename_map)

    # S’assurer que toutes les colonnes existent (même si vides)
    for col in final_cols:
        if col not in kpi.columns:
            kpi[col] = np.nan

    # Ordre final
    kpi = kpi[final_cols].sort_values(["OrganisationID","month"]).reset_index(drop=True)

    # =====================================================
    # 5) EXPORT — KPI -> GOOGLE SHEETS (seul tableau poussé)
    # =====================================================
    ws_kpi = _open_or_create(SPREADSHEET_ID, SHEET_KPI)
    _update_ws(ws_kpi, kpi)

    st.success(f"✅ KPI_Mensuels mis à jour ({len(kpi)} lignes) avec EXACTEMENT les colonnes demandées.")
    with st.expander("👀 Aperçu KPI (10 premières lignes)"):
        st.dataframe(kpi.head(10))

    # =====================================================
    # 6) ENVOI EMAIL LOOKER
    # =====================================================
    if st.button("📤 Envoyer le lien Looker par e-mail"):
        recipients = [DEFAULT_RECEIVER] + [e.strip() for e in emails_supp.split(",") if e.strip()]
        msg = EmailMessage()
        msg["Subject"] = f"📊 Rapport fidélité La Tribu - {datetime.today().strftime('%d/%m/%Y')}"
        msg["From"] = SMTP_USER
        msg["To"] = ", ".join(recipients)
        msg.set_content(f"Bonjour,\n\nLe tableau de bord fidélité La Tribu a été mis à jour.\n\n👉 Accédez-y ici : {LOOKER_URL}\n\nBien à vous,\nL’équipe La Tribu")
        try:
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USER, SMTP_PASS)
                server.send_message(msg)
            st.success("📧 Mail Looker envoyé avec succès.")
        except Exception as e:
            st.error(f"❌ Erreur d’envoi mail : {e}")

else:
    st.info("➡️ Importez les fichiers Transactions et Coupons pour démarrer.")
