import streamlit as st
import pandas as pd
import numpy as np
import os
import io
import json
import smtplib
from email.message import EmailMessage
import requests
import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
import gc

# ============================================================
# CONFIG
# ============================================================
st.set_page_config(page_title="🎯 Analyse Fidélité - KPI Automatisé", layout="wide")
st.title("🎯 Analyse Fidélité - AutoMapping Keyneo ➜ KPI mensuels (Drive + Mail)")

DATA_DIR = "data"
TX_PATH = os.path.join(DATA_DIR, "transactions.parquet")
CP_PATH = os.path.join(DATA_DIR, "coupons.parquet")

SPREADSHEET_ID = st.secrets["sheets"]["spreadsheet_id"]
LOOKER_URL = st.secrets["app"]["looker_url"]
SMTP_SERVER = st.secrets["email"]["smtp_server"]
SMTP_PORT = st.secrets["email"]["smtp_port"]
SMTP_USER = st.secrets["email"]["smtp_user"]
SMTP_PASS = st.secrets["email"]["smtp_password"]
DEFAULT_RECEIVER = st.secrets["email"]["receiver"]
DRIVE_FILE_ID = st.secrets["gcp"]["json_drive_file_id"]

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
# OPTIMISATION MÉMOIRE
# ============================================================
def optimize_floats(df):
    """Convertit toutes les colonnes float64/int64 en float32/int32 pour économiser la RAM."""
    for dtype, target in [("float64", "float32"), ("int64", "int32")]:
        cols = df.select_dtypes(include=[dtype]).columns
        df[cols] = df[cols].apply(lambda x: x.astype(target))
    return df

def memory_report(df, name="DataFrame"):
    """Affiche l’utilisation mémoire d’un DataFrame dans Streamlit (en MB)."""
    mem_mb = df.memory_usage(deep=True).sum() / 1024**2
    st.sidebar.write(f"💾 {name} : {mem_mb:.2f} MB")

# ============================================================
# GOOGLE DRIVE AUTH
# ============================================================
url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID}"
resp = requests.get(url)
resp.raise_for_status()
gcp_info = json.loads(resp.content)

creds = service_account.Credentials.from_service_account_info(
    gcp_info,
    scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets"
    ]
)

gspread_client = gspread.authorize(creds)
drive_service = build("drive", "v3", credentials=creds)

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
# 📥 RÉCUPÉRATION DES FICHIERS EXISTANTS SUR GOOGLE DRIVE
# ============================================================
def download_from_drive(file_name, local_path, folder_id=None):
    """Télécharge un fichier depuis le dossier Drive (ou Drive partagé)."""
    if folder_id is None:
        folder_id = st.secrets["gcp"].get("folder_id", "")
    try:
        query = f"name='{file_name}' and trashed=false"
        if folder_id:
            query += f" and '{folder_id}' in parents"
        results = (
            drive_service.files()
            .list(q=query, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True)
            .execute()
            .get("files", [])
        )
        if not results:
            st.info(f"ℹ️ Fichier '{file_name}' non trouvé sur le Drive (initialisation).")
            return False
        file_id = results[0]["id"]
        request = drive_service.files().get_media(fileId=file_id, supportsAllDrives=True)
        fh = io.FileIO(local_path, "wb")
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.close()
        st.success(f"📥 Fichier '{file_name}' téléchargé depuis le Drive.")
        return True
    except Exception as e:
        st.warning(f"⚠️ Erreur téléchargement '{file_name}' : {e}")
        return False

# ============================================================
# UI
# ============================================================
st.sidebar.header("📂 Importer les fichiers CSV")
file_tx = st.sidebar.file_uploader("Transactions (CSV Keyneo)", type=["csv"])
file_cp = st.sidebar.file_uploader("Coupons (CSV Keyneo)", type=["csv"])
st.sidebar.button("🧹 Nettoyer la mémoire", on_click=lambda: (st.cache_data.clear(), st.cache_resource.clear(), gc.collect()))
ensure_data_dir()

# Téléchargement des fichiers Drive actuels
_ = download_from_drive("transactions.parquet", TX_PATH)
_ = download_from_drive("coupons.parquet", CP_PATH)

# ============================================================
# PIPELINE
# ============================================================
if file_tx and file_cp:
    tx = read_csv(file_tx)
    cp = read_csv(file_cp)
    hist_tx = load_parquet(TX_PATH, TX_COLS)

    # === Mapping transactions et coupons ===
    map_tx = {
        "TransactionID": pick(tx, "ticketnumber","transactionid","operationid"),
        "ValidationDate": pick(tx, "validationdate","operationdate"),
        "OrganisationID": pick(tx, "organisationid","organizationid"),
        "CustomerID": pick(tx, "customerid","clientid"),
        "ProductID": pick(tx, "productid","sku","ean"),
        "Label": pick(tx, "label","designation"),
        "CA_TTC": pick(tx, "totalamount","totalttc","totaltcc"),
        "CA_HT": pick(tx, "linegrossamount","montanthtligne","cahtligne"),
        "Purch_Total_HT": pick(tx, "linetotalpurchasingamount","purchasingamount","costprice"),
        "Qty_Ticket": pick(tx, "quantity","qty","linequantity")
    }
    for k,v in map_tx.items():
        tx[k] = tx[v] if v in tx.columns else ""
    tx["ValidationDate"] = _ensure_date(tx["ValidationDate"])
    for col in ["CA_TTC","CA_HT","Purch_Total_HT","Qty_Ticket"]:
        tx[col] = pd.to_numeric(tx[col], errors="coerce").fillna(0.0)
    tx["Estimated_Net_Margin_HT"] = tx["CA_HT"] - tx["Purch_Total_HT"]
    tx["month"] = _month_str(tx["ValidationDate"])

    map_cp = {
        "CouponID": pick(cp, "couponid", "id"),
        "OrganisationID": pick(cp, "organisationid", "organizationid"),
        "EmissionDate": pick(cp, "creationdate", "issuedate"),
        "UseDate": pick(cp, "usedate", "validationdate"),
        "Amount_Initial": pick(cp, "initialvalue", "value", "montantinitial"),
        "Amount_Remaining": pick(cp, "amount", "reste", "remaining"),
    }
    for k,v in map_cp.items():
        cp[k] = cp[v] if v and v in cp.columns else ""
    for col in ["Amount_Initial", "Amount_Remaining"]:
        cp[col] = cp[col].astype(str).str.replace(",", ".", regex=False)
        cp[col] = pd.to_numeric(cp[col], errors="coerce").fillna(0.0)
    cp["EmissionDate"] = _ensure_date(cp["EmissionDate"])
    cp["UseDate"] = _ensure_date(cp["UseDate"])
    cp["Value_Used_Line"] = (cp["Amount_Initial"] - cp["Amount_Remaining"]).clip(lower=0.0)
    cp["month_use"] = _month_str(cp["UseDate"])
    cp["month_emit"] = _month_str(cp["EmissionDate"])

    # === Optimisation mémoire ===
    tx = optimize_floats(tx)
    cp = optimize_floats(cp)
    hist_tx = optimize_floats(hist_tx)
    memory_report(tx, "Transactions (optimisées)")
    memory_report(cp, "Coupons (optimisés)")

    # === Append et sauvegarde ===
    tx["TransactionID"] = tx["TransactionID"].astype(str)
    hist_tx["TransactionID"] = hist_tx["TransactionID"].astype(str)
    new_tx = tx[~tx["TransactionID"].isin(hist_tx["TransactionID"])]
    hist_tx = pd.concat([hist_tx, new_tx], ignore_index=True)
    save_parquet(hist_tx, TX_PATH)
    save_parquet(cp, CP_PATH)
    st.success(f"✅ {len(new_tx)} nouvelles transactions ajoutées. Coupons mis à jour.")

    # ======================================================
    # 6️⃣ KPI Mensuel — COMPLET (toutes colonnes demandées)
    # ======================================================
    df_tx = hist_tx.copy()

    # ✅ AJOUT MINIMAL pour conserver ta logique: on recharge hist_cp
    hist_cp = load_parquet(CP_PATH, CP_COLS)
    df_cp = hist_cp.copy()

    if df_tx.empty:
        st.warning("⚠️ Pas de données transactionnelles disponibles.")
        st.stop()

    # --- Nettoyage transactions
    df_tx["ValidationDate"] = _ensure_date(df_tx["ValidationDate"])
    df_tx["month"] = _month_str(df_tx["ValidationDate"])
    for col in ["CA_TTC","CA_HT","Purch_Total_HT","Qty_Ticket"]:
        df_tx[col] = pd.to_numeric(df_tx[col], errors="coerce").fillna(0.0)
    df_tx["Label"] = df_tx["Label"].fillna("").astype(str)
    df_tx["CustomerID"] = df_tx["CustomerID"].fillna("").astype(str)
    df_tx["OrganisationID"] = df_tx["OrganisationID"].fillna("").astype(str)
    df_tx["_is_coupon_line"] = df_tx["Label"].str.upper().eq("COUPON")

    # --- Fact ticket (1 ligne = 1 ticket)
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

    # --- Splits utiles
    ticket_client = agg_ticket[agg_ticket["CustomerID"].str.len() > 0].copy()
    ticket_non_client = agg_ticket[agg_ticket["CustomerID"].str.len() == 0].copy()
    ticket_coupon = agg_ticket[agg_ticket["Has_Coupon"]].copy()
    ticket_sans_coupon = agg_ticket[~agg_ticket["Has_Coupon"]].copy()

    # --- Base mensuelle (par magasin)
    base = agg_ticket.groupby(["month","OrganisationID"], dropna=False).agg(
        CA_TTC=("CA_TTC_ticket","sum"),
        CA_HT=("CA_HT_ticket","sum"),
        Marge_net_HT_avant_coupon=("Marge_net_HT_ticket","sum"),
        Transactions=("TransactionID","nunique"),
        Qty_total=("Qty_ticket","sum"),
        CA_paid_with_coupons=("CA_paid_with_coupons","sum"),
        Tickets_avec_coupon=("Has_Coupon","sum")
    ).reset_index()

    # --- Clients / Transactions côté clients
    cli_base = ticket_client.groupby(["month","OrganisationID"], dropna=False).agg(
        Transactions_Client=("TransactionID","nunique"),
        Clients=("CustomerID","nunique")
    ).reset_index()
    assoc = base.merge(cli_base, on=["month","OrganisationID"], how="left").fillna({"Transactions_Client":0, "Clients":0})
    assoc["Taux_association_client"] = np.where(
        assoc["Transactions"]>0, assoc["Transactions_Client"]/assoc["Transactions"], np.nan
    )

    # --- Nouveaux / Récurrents
    first_seen = ticket_client.groupby(["OrganisationID","CustomerID"], dropna=False)["ValidationDate"].min().reset_index(name="FirstDate")
    ticket_client = ticket_client.merge(first_seen, on=["OrganisationID","CustomerID"], how="left")
    ticket_client["IsNewThisMonth"] = ticket_client["ValidationDate"].dt.to_period("M") == ticket_client["FirstDate"].dt.to_period("M")
    new_ret = ticket_client.groupby(["month","OrganisationID"], dropna=False).agg(
        Nouveau_client=("IsNewThisMonth", "sum"),
        Clients_mois=("CustomerID","nunique"),
        Transactions_Client=("TransactionID","nunique")
    ).reset_index()
    new_ret["Client_qui_reviennent"] = (new_ret["Clients_mois"] - new_ret["Nouveau_client"]).clip(lower=0).astype(int)
    new_ret["Recurrence"] = np.where(new_ret["Clients_mois"]>0, new_ret["Transactions_Client"]/new_ret["Clients_mois"], np.nan)
    new_ret = new_ret.rename(columns={"Clients_mois":"Clients"})

    # --- Rétention (clients N-1 vus en N)
    cust_sets = (
        ticket_client.groupby(["OrganisationID","month"], dropna=False)["CustomerID"]
        .apply(lambda s: set(s.dropna().astype(str).unique()))
        .reset_index(name="CustSet")
    )
    cust_sets["_order"] = pd.PeriodIndex(cust_sets["month"], freq="M").to_timestamp()
    cust_sets = cust_sets.sort_values(["OrganisationID","_order"])
    cust_sets["Prev"] = cust_sets.groupby("OrganisationID")["CustSet"].shift(1)
    ret = cust_sets[["month","OrganisationID"]].copy()
    ret["Retention_rate"] = cust_sets.apply(
        lambda r: (len(r["Prev"].intersection(r["CustSet"])) / len(r["Prev"]))
        if isinstance(r["Prev"], set) and len(r["Prev"])>0 else np.nan,
        axis=1
    )

    # --- Coupons (émis / utilisés)
    coupons_used = df_cp.dropna(subset=["UseDate"]).groupby(["month_use","OrganisationID"]).agg(
        Coupon_utilise=("CouponID","nunique"),
        Montant_coupons_utilise=("Value_Used_Line","sum")
    ).rename(columns={"month_use":"month"}).reset_index()
    coupons_emis = df_cp.dropna(subset=["EmissionDate"]).groupby(["month_emit","OrganisationID"]).agg(
        Coupon_emis=("CouponID","nunique"),
        Montant_coupons_emis=("Amount_Initial","sum")
    ).rename(columns={"month_emit":"month"}).reset_index()

    # --- Paniers moyens
    panier_client = ticket_client.groupby(["month","OrganisationID"], dropna=False)["CA_HT_ticket"].mean().reset_index(name="Panier_moyen_client")
    panier_non_client = ticket_non_client.groupby(["month","OrganisationID"], dropna=False)["CA_HT_ticket"].mean().reset_index(name="Panier_moyen_non_client")
    panier_avec = ticket_coupon.groupby(["month","OrganisationID"], dropna=False)["CA_HT_ticket"].mean().reset_index(name="Panier_moyen_avec_coupon")
    panier_sans = ticket_sans_coupon.groupby(["month","OrganisationID"], dropna=False)["CA_HT_ticket"].mean().reset_index(name="Panier_moyen_sans_coupon")

    # --- Harmonisation clés avant merges (corrigé)
    for df_ in [base, assoc, new_ret, ret, panier_client, panier_non_client, panier_avec, panier_sans]:
        if "OrganisationID" not in df_.columns and "organisationid" in df_.columns:
            df_["OrganisationID"] = df_["organisationid"]
        df_["OrganisationID"] = df_["OrganisationID"].astype(str).fillna("")
        if "month" not in df_.columns:
            df_["month"] = df_.get("month", "")
        df_["month"] = df_["month"].astype(str).fillna("")

    # ⚠️ Ne pas toucher à coupons_used / coupons_emis car leurs colonnes 'month_use' et 'month_emit'
    # doivent être renommées manuellement :
    coupons_used = coupons_used.rename(columns={"month_use": "month"})
    coupons_emis = coupons_emis.rename(columns={"month_emit": "month"})

    # --- KPI fusionné
    kpi = (base
        .merge(assoc[["month","OrganisationID","Transactions_Client","Clients","Taux_association_client"]], on=["month","OrganisationID"], how="left")
        .merge(new_ret[["month","OrganisationID","Nouveau_client","Client_qui_reviennent","Recurrence"]], on=["month","OrganisationID"], how="left")
        .merge(ret, on=["month","OrganisationID"], how="left")
        .merge(coupons_used, on=["month","OrganisationID"], how="left")
        .merge(coupons_emis, on=["month","OrganisationID"], how="left")
        .merge(panier_client, on=["month","OrganisationID"], how="left")
        .merge(panier_non_client, on=["month","OrganisationID"], how="left")
        .merge(panier_avec, on=["month","OrganisationID"], how="left")
        .merge(panier_sans, on=["month","OrganisationID"], how="left")
    )

    # --- Dérivés finaux
    kpi["Marge_net_HT_apres_coupon"] = kpi["Marge_net_HT_avant_coupon"] - kpi["Montant_coupons_utilise"].fillna(0)
    kpi["Taux_de_marge_HT_avant_coupon"] = np.where(kpi["CA_HT"]>0, kpi["Marge_net_HT_avant_coupon"]/kpi["CA_HT"], np.nan)
    kpi["Taux_de_marge_HT_apres_coupons"] = np.where(kpi["CA_HT"]>0, kpi["Marge_net_HT_apres_coupon"]/kpi["CA_HT"], np.nan)
    kpi["ROI_Proxy"] = np.where(
        kpi["Montant_coupons_utilise"].fillna(0)>0,
        (kpi["CA_paid_with_coupons"].fillna(0) - kpi["Montant_coupons_utilise"].fillna(0)) / kpi["Montant_coupons_utilise"].fillna(0),
        np.nan
    )
    kpi["Panier_moyen_HT"] = np.where(kpi["Transactions"]>0, kpi["CA_HT"]/kpi["Transactions"], np.nan)
    kpi["Prix_moyen_article_vendu_HT"] = np.where(kpi["Qty_total"]>0, kpi["CA_HT"]/kpi["Qty_total"], np.nan)
    kpi["Quantite_moy_article_par_transaction"] = np.where(kpi["Transactions"]>0, kpi["Qty_total"]/kpi["Transactions"], np.nan)
    kpi["Taux_utilisation_bons_montant"] = np.where(kpi["Montant_coupons_emis"].fillna(0)>0, kpi["Montant_coupons_utilise"].fillna(0)/kpi["Montant_coupons_emis"].fillna(0), np.nan)
    kpi["Taux_utilisation_bons_quantite"] = np.where(kpi["Coupon_emis"].fillna(0)>0, kpi["Coupon_utilise"].fillna(0)/kpi["Coupon_emis"].fillna(0), np.nan)
    kpi["Taux_CA_genere_par_bons_sur_CA_HT"] = np.where(kpi["CA_HT"]>0, kpi["CA_paid_with_coupons"]/kpi["CA_HT"], np.nan)
    kpi["Voucher_share"] = np.where(kpi["Transactions"]>0, kpi["Tickets_avec_coupon"]/kpi["Transactions"], np.nan)
    kpi["Date"] = pd.to_datetime(kpi["month"], errors="coerce").dt.strftime("%d/%m/%Y")

    # --- Renommage final (titres FR) & ordre exact
    rename_fr = {
        "month":"month",
        "Date":"Date",
        "OrganisationID":"OrganisationID",
        "CA_TTC":"CA TTC",
        "CA_HT":"CA HT",
        "CA_paid_with_coupons":"CA paid with coupons",
        "Marge_net_HT_avant_coupon":"Marge net HT avant coupon",
        "Marge_net_HT_apres_coupon":"Marge net HT après coupon",
        "Taux_de_marge_HT_avant_coupon":"Taux de marge HT avant coupon",
        "Taux_de_marge_HT_apres_coupons":"Taux de marge HT après coupons",
        "Transactions":"Transaction",
        "Transactions_Client":"Transaction associé à un client (nombre)",
        "Clients":"Client",
        "Nouveau_client":"Nouveau client",
        "Client_qui_reviennent":"Client qui reviennent",
        "Recurrence":"Recurrence",
        "Retention_rate":"Retention_rate",
        "Taux_association_client":"Taux association client",
        "Coupon_utilise":"Coupon utilisé",
        "Montant_coupons_utilise":"Montant coupons utilisé",
        "Coupon_emis":"Coupon émis",
        "Montant_coupons_emis":"Montant coupons émis",
        "Taux_utilisation_bons_montant":"Taux d'utilisation des bons en montant",
        "Taux_utilisation_bons_quantite":"Taux d'utilisation des bons en quantité",
        "Taux_CA_genere_par_bons_sur_CA_HT":"Taux de CA généré par les bons sur CA HT",
        "Voucher_share":"Voucher_share",
        "Panier_moyen_HT":"Panier moyen HT",
        "Panier_moyen_client":"Panier moyen client",
        "Panier_moyen_non_client":"Panier moyen non client",
        "Panier_moyen_sans_coupon":"Panier moyen sans coupon",
        "Panier_moyen_avec_coupon":"Panier moyen avec coupon",
        "Prix_moyen_article_vendu_HT":"Prix moyen article vendu HT",
        "Quantite_moy_article_par_transaction":"Quantité moyen article par transaction",
        "Qty_total":"Quantité total article (somme)"
    }
    kpi = kpi.rename(columns=rename_fr)

    order_cols = [
        "month","Date","OrganisationID",
        "CA TTC","CA HT","CA paid with coupons",
        "Marge net HT avant coupon","Marge net HT après coupon",
        "Taux de marge HT avant coupon","Taux de marge HT après coupons",
        "Transaction","Transaction associé à un client (nombre)","Taux association client",
        "Client","Nouveau client","Client qui reviennent","Recurrence","Retention_rate",
        "Coupon utilisé","Montant coupons utilisé","Coupon émis","Montant coupons émis",
        "Taux d'utilisation des bons en montant","Taux d'utilisation des bons en quantité",
        "Taux de CA généré par les bons sur CA HT","Voucher_share",
        "Panier moyen HT","Panier moyen client","Panier moyen non client",
        "Panier moyen sans coupon","Panier moyen avec coupon",
        "Prix moyen article vendu HT","Quantité moyen article par transaction","Quantité total article (somme)"
    ]
    for c in order_cols:
        if c not in kpi.columns:
            kpi[c] = np.nan
    kpi = kpi[order_cols]

    # --- Nettoyage sorties (NaN → "")
    kpi = kpi.replace([np.inf, -np.inf], np.nan)
    kpi = kpi.fillna("")

    st.subheader("📊 KPI mensuels (complet)")
    st.dataframe(kpi.head(50))
    # === Optimisation du KPI avant export ===
    kpi = optimize_floats(kpi)
    memory_report(kpi, "KPI mensuels (optimisé)")
   
    # --- Export CSV local pour download
    csv = kpi.to_csv(index=False, sep=";").encode("utf-8-sig")
    st.download_button("💾 Télécharger le KPI mensuel (CSV)", csv, "KPI_mensuel.csv", "text/csv")

    # ============================================================
    # EXPORT GOOGLE DRIVE & GOOGLE SHEET
    # ============================================================
    st.subheader("☁️ Export Google Drive & Google Sheets")

    def upload_to_drive(file_path, file_name, mime_type="application/octet-stream", folder_id=None):
        ...
        # (bloc d’upload complet identique à ta dernière version)
        ...

    def update_sheet(spreadsheet_id, sheet_name, df):
        """Réécrit totalement la feuille KPI dans Google Sheets avec formatage FR homogène."""
        try:
            sh = gspread_client.open_by_key(spreadsheet_id)
            try:
                ws = sh.worksheet(sheet_name)
                sh.del_worksheet(ws)
            except gspread.WorksheetNotFound:
                pass
            ws = sh.add_worksheet(title=sheet_name, rows=str(len(df) + 10), cols=str(len(df.columns) + 5))

            def format_val(x):
                if pd.isna(x) or x == "":
                    return ""
                try:
                    x = float(x)
                    return str(round(x, 4)).replace(".", ",")
                except Exception:
                    return str(x).replace("'", "")

            df_upload = df.copy()
            for col in df_upload.columns:
                df_upload[col] = df_upload[col].apply(format_val)

            ws.update("A1", [list(df_upload.columns)] + df_upload.values.tolist(), value_input_option="USER_ENTERED")
            st.success(f"✅ Feuille '{sheet_name}' mise à jour avec {len(df)} lignes (format FR homogène).")
        except Exception as e:
            st.error(f"❌ Erreur mise à jour Google Sheets : {e}")

    try:
        _ = upload_to_drive(TX_PATH, "transactions.parquet", "application/octet-stream")
        _ = upload_to_drive(CP_PATH, "coupons.parquet", "application/octet-stream")
        st.success("✅ Transactions et coupons exportés sur Google Drive.")
    except Exception as e:
        st.error(f"❌ Erreur export Drive : {e}")

    update_sheet(SPREADSHEET_ID, "KPI_Mensuels", kpi)

else:
    st.info("➡️ Importez les fichiers Transactions et Coupons pour démarrer.")
