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
from googleapiclient.http import MediaIoBaseUpload
from datetime import datetime

# ============================================================
# CONFIG GLOBALE
# ============================================================
st.set_page_config(page_title="🎯 Analyse Fidélité & Stocks - KPI Automatisé", layout="wide")
st.title("🎯 Analyse Fidélité & Stocks - AutoMapping Keyneo ➜ KPI mensuels & Valorisation")

DATA_DIR = "data"
TX_PATH = os.path.join(DATA_DIR, "transactions.parquet")
CP_PATH = os.path.join(DATA_DIR, "coupons.parquet")

# 👉 Ce SPREADSHEET_ID doit pointer vers ton Google Sheet "KPI - La Tribu"
SPREADSHEET_ID = st.secrets["sheets"]["spreadsheet_id"]

LOOKER_URL = st.secrets["app"]["looker_url"]          # Pour le dashboard Looker / Stock
SMTP_SERVER = st.secrets["email"]["smtp_server"]
SMTP_PORT = st.secrets["email"]["smtp_port"]
SMTP_USER = st.secrets["email"]["smtp_user"]
SMTP_PASS = st.secrets["email"]["smtp_password"]
DEFAULT_RECEIVER = st.secrets["email"]["receiver"]

# Service account JSON stocké sur Drive (comme dans ton analyse_fidelite.py)
DRIVE_FILE_ID = st.secrets["gcp"]["json_drive_file_id"]

# Historique stock local
HISTO_FILE = "historique_valorisation.csv"

# ============================================================
# HELPERS COMMUNS
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
# GOOGLE DRIVE + GSPREAD AUTH (commun Fidélité + Stock)
# ============================================================
url = f"https://drive.google.com/uc?id={DRIVE_FILE_ID}"
resp = requests.get(url)
resp.raise_for_status()
gcp_info = json.loads(resp.content)

creds = service_account.Credentials.from_service_account_info(
    gcp_info,
    scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ],
)
gspread_client = gspread.authorize(creds)
drive_service = build("drive", "v3", credentials=creds)

# ============================================================
# SCHEMA TRANSACTIONS / COUPONS (Fidélité)
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
# HELPERS GSPREAD STOCK (upsert dans une feuille)
# ============================================================
def _ensure_date_series(s: pd.Series) -> pd.Series:
    ds = pd.to_datetime(s, errors="coerce")
    return ds.dt.strftime("%Y-%m-%d")

def _gsheet_read_as_df(sheet_id: str, tab_name: str):
    try:
        ws = gspread_client.open_by_key(sheet_id).worksheet(tab_name)
    except Exception:
        sh = gspread_client.open_by_key(sheet_id)
        ws = sh.add_worksheet(title=tab_name, rows=2, cols=20)
    rows = ws.get_all_values()
    if not rows:
        return pd.DataFrame(), ws
    header = rows[0]
    data = rows[1:]
    if not data:
        return pd.DataFrame(columns=header), ws
    df = pd.DataFrame(data, columns=header)
    return df, ws

def _gsheet_upsert_dataframe(sheet_id: str, tab_name: str, df_new: pd.DataFrame) -> pd.DataFrame:
    """
    Upsert full historique valorisation dans un onglet donné.
    On concatène ancien + nouveau, on dédoublonne, puis on réécrit toute la feuille.
    """
    df_old, ws = _gsheet_read_as_df(sheet_id, tab_name)

    if not df_old.empty:
        # aligner colonnes
        for c in df_new.columns:
            if c not in df_old.columns:
                df_old[c] = pd.NA
        for c in df_old.columns:
            if c not in df_new.columns:
                df_new[c] = pd.NA
        df_old = df_old[df_new.columns]

    if "date" in df_new.columns:
        df_new["date"] = _ensure_date_series(df_new["date"])
    if not df_old.empty and "date" in df_old.columns:
        df_old["date"] = _ensure_date_series(df_old["date"])

    df_all = pd.concat([df_old, df_new], ignore_index=True) if not df_old.empty else df_new.copy()

    key_cols = [c for c in ["date", "organisationId", "brand"] if c in df_all.columns]
    if key_cols:
        key = df_all[key_cols].astype(str).agg("|".join, axis=1)
        df_all = df_all.loc[~key.duplicated(keep="last")].copy()

    if "date" in df_all.columns:
        dmax = pd.to_datetime(df_all["date"], errors="coerce").max()
        df_all["est_derniere_date"] = (pd.to_datetime(df_all["date"], errors="coerce") == dmax)

    sort_cols = [c for c in ["date", "organisationId", "brand"] if c in df_all.columns]
    if sort_cols:
        df_all = df_all.sort_values(sort_cols)

    ws.clear()
    values = [list(df_all.columns)] + df_all.astype(object).where(pd.notnull(df_all), "").values.tolist()
    ws.update("A1", values)
    return df_all

# ============================================================
# UI SIDEBAR
# ============================================================
st.sidebar.header("📂 Données Fidélité")
file_tx = st.sidebar.file_uploader("Transactions (CSV Keyneo)", type=["csv"], key="tx")
file_cp = st.sidebar.file_uploader("Coupons (CSV Keyneo)", type=["csv"], key="cp")

st.sidebar.header("🏪 Données Stock")
stock_files = st.sidebar.file_uploader("Fichiers de stock (un par magasin)", type=["csv"], accept_multiple_files=True, key="stocks")
product_file = st.sidebar.file_uploader("Base produit (Excel)", type=["xls", "xlsx"], key="products")
emails_supp_stock = st.sidebar.text_input("📧 Autres destinataires rapport stock (séparés par des virgules)", key="emails_stock")

ensure_data_dir()

# ============================================================
# TABS
# ============================================================
tab_fid, tab_stock = st.tabs(["🧑‍🤝‍🧑 Fidélité", "📦 Stocks"])

# ============================================================
# ONGLET FIDÉLITÉ (reprend analyse_fidelite.py)
# ============================================================
with tab_fid:
    if file_tx and file_cp:
        # 1️⃣ Lecture CSV
        tx = read_csv(file_tx)
        cp = read_csv(file_cp)

        # 2️⃣ Chargement historique transactions uniquement
        hist_tx = load_parquet(TX_PATH, TX_COLS)

        # 3️⃣ Mapping transactions
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
            "Qty_Ticket": pick(tx, "quantity","qty","linequantity"),
        }
        for k, v in map_tx.items():
            tx[k] = tx[v] if v in tx.columns else ""

        tx["ValidationDate"] = _ensure_date(tx["ValidationDate"])
        for col in ["CA_TTC","CA_HT","Purch_Total_HT","Qty_Ticket"]:
            tx[col] = pd.to_numeric(tx[col], errors="coerce").fillna(0.0)

        tx = tx[list(map_tx.keys())].copy()
        tx = tx.dropna(subset=["ValidationDate"])

        # 4️⃣ Mapping coupons
        map_cp = {
            "CouponID": pick(cp, "couponid","id"),
            "OrganisationID": pick(cp, "organisationid","organizationid"),
            "EmissionDate": pick(cp, "emissiondate","createdate"),
            "UseDate": pick(cp, "usedate","validationdate"),
            "Amount_Initial": pick(cp, "amountinitial","amount"),
            "Amount_Remaining": pick(cp, "amountremaining"),
            "Value_Used_Line": pick(cp, "valueusedline","valueused","montantutilise"),
        }
        for k, v in map_cp.items():
            cp[k] = cp[v] if v in cp.columns else ""

        for col in ["Amount_Initial","Amount_Remaining","Value_Used_Line"]:
            cp[col] = pd.to_numeric(cp[col], errors="coerce").fillna(0.0)
        cp["EmissionDate"] = _ensure_date(cp["EmissionDate"])
        cp["UseDate"] = _ensure_date(cp["UseDate"])
        cp = cp[list(map_cp.keys())].copy()

        # 5️⃣ Sauvegarde transactions / coupons (historique)
        full_tx = pd.concat([hist_tx, tx], ignore_index=True)
        full_tx = full_tx.drop_duplicates(subset=["TransactionID"], keep="last")
        save_parquet(full_tx, TX_PATH)
        save_parquet(cp, CP_PATH)

        st.success(f"✅ Transactions mises à jour ({len(full_tx)} lignes au total).")

        # 6️⃣ Calcul KPI mensuels
        df = full_tx.copy()
        df["month"] = _month_str(df["ValidationDate"])
        df["CA_HT_ticket"] = df.groupby("TransactionID")["CA_HT"].transform("sum")
        df["CA_TTC_ticket"] = df.groupby("TransactionID")["CA_TTC"].transform("sum")

        ticket = df.drop_duplicates(subset=["TransactionID"])
        ticket_client = ticket[~ticket["CustomerID"].isna() & (ticket["CustomerID"].astype(str) != "")]
        ticket_non_client = ticket[ticket["CustomerID"].isna() | (ticket["CustomerID"].astype(str) == "")]

        # Base CA, marge, etc.
        base = (
            ticket
            .groupby(["month","OrganisationID"], dropna=False)
            .agg(
                CA_TTC=("CA_TTC_ticket","sum"),
                CA_HT=("CA_HT_ticket","sum"),
                Purch_Total_HT=("Purch_Total_HT","sum"),
                Transactions=("TransactionID","nunique"),
            )
            .reset_index()
        )
        base["Marge_brute"] = base["CA_HT"] - base["Purch_Total_HT"]
        base["Taux_marge"] = np.where(base["CA_HT"] != 0, base["Marge_brute"] / base["CA_HT"], np.nan)

        # Nouveau / récurrent / rétention (je garde ta logique actuelle)
        ticket_client["month"] = _month_str(ticket_client["ValidationDate"])
        assoc = (
            ticket_client
            .groupby(["month","OrganisationID"], dropna=False)
            .agg(
                Clients_mois=("CustomerID","nunique"),
                Transactions_Client=("TransactionID","nunique"),
            )
            .reset_index()
        )

        # Clients vus pour la première fois
        min_month = (
            ticket_client
            .groupby("CustomerID")["month"]
            .min()
            .rename("first_month")
            .reset_index()
        )
        ticket_client = ticket_client.merge(min_month, on="CustomerID", how="left")
        new_ret = (
            ticket_client
            .groupby(["month","OrganisationID"], dropna=False)
            .agg(
                Clients_mois=("CustomerID","nunique"),
                Nouveau_client=("first_month", lambda s: (s == s.name[0]).sum()),
                Transactions_Client=("TransactionID","nunique"),
            )
            .reset_index()
        )
        new_ret["Client_qui_reviennent"] = new_ret["Clients_mois"] - new_ret["Nouveau_client"]
        new_ret["Recurrence"] = np.where(
            new_ret["Clients_mois"] > 0,
            new_ret["Transactions_Client"] / new_ret["Clients_mois"],
            np.nan,
        )
        new_ret = new_ret.rename(columns={"Clients_mois":"Clients"})

        # Rétention
        ticket_client["month"] = _month_str(ticket_client["ValidationDate"])
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
            if isinstance(r["Prev"], set) and len(r["Prev"]) > 0 else np.nan,
            axis=1,
        )

        # Coupons (émis / utilisés)
        cp["month_emit"] = _month_str(cp["EmissionDate"])
        cp["month_use"] = _month_str(cp["UseDate"])
        df_cp = cp.copy()
        coupons_used = df_cp.dropna(subset=["UseDate"]).groupby(["month_use","OrganisationID"]).agg(
            Coupon_utilise=("CouponID","nunique"),
            Montant_coupons_utilise=("Value_Used_Line","sum"),
        ).rename(columns={"month_use":"month"}).reset_index()
        coupons_emis = df_cp.dropna(subset=["EmissionDate"]).groupby(["month_emit","OrganisationID"]).agg(
            Coupon_emis=("CouponID","nunique"),
            Montant_coupons_emis=("Amount_Initial","sum"),
        ).rename(columns={"month_emit":"month"}).reset_index()

        # Paniers moyens
        panier_client = ticket_client.groupby(["month","OrganisationID"])["CA_HT_ticket"].mean().reset_index(name="Panier_moyen_client")
        panier_non_client = ticket_non_client.groupby(["month","OrganisationID"])["CA_HT_ticket"].mean().reset_index(name="Panier_moyen_non_client")

        ticket_coupon = ticket[ticket["TransactionID"].isin(df_cp.dropna(subset=["UseDate"])["CouponID"].unique())]
        ticket_sans_coupon = ticket[~ticket["TransactionID"].isin(ticket_coupon["TransactionID"].unique())]

        panier_avec = ticket_coupon.groupby(["month","OrganisationID"])["CA_HT_ticket"].mean().reset_index(name="Panier_moyen_avec_coupon")
        panier_sans = ticket_sans_coupon.groupby(["month","OrganisationID"])["CA_HT_ticket"].mean().reset_index(name="Panier_moyen_sans_coupon")

        # Harmonisation clés
        for df_ in [base, assoc, new_ret, ret, coupons_used, coupons_emis, panier_client, panier_non_client, panier_avec, panier_sans]:
            if "OrganisationID" in df_.columns:
                df_["OrganisationID"] = df_["OrganisationID"].astype(str)
            if "month" in df_.columns:
                df_["month"] = df_["month"].astype(str)

        kpi = base.merge(assoc, on=["month","OrganisationID"], how="left")
        kpi = kpi.merge(new_ret, on=["month","OrganisationID"], how="left", suffixes=("","_new"))
        kpi = kpi.merge(ret, on=["month","OrganisationID"], how="left")
        kpi = kpi.merge(coupons_used, on=["month","OrganisationID"], how="left")
        kpi = kpi.merge(coupons_emis, on=["month","OrganisationID"], how="left")
        kpi = kpi.merge(panier_client, on=["month","OrganisationID"], how="left")
        kpi = kpi.merge(panier_non_client, on=["month","OrganisationID"], how="left")
        kpi = kpi.merge(panier_avec, on=["month","OrganisationID"], how="left")
        kpi = kpi.merge(panier_sans, on=["month","OrganisationID"], how="left")

        # On garde la version "Clients" issue de new_ret
        if "Clients_new" in kpi.columns:
            kpi["Clients"] = kpi["Clients"].fillna(kpi["Clients_new"])
            kpi = kpi.drop(columns=["Clients_new"])

        # Quelques ratios coupons
        kpi["Taux_utilisation_bons_montant"] = np.where(kpi["Montant_coupons_emis"] > 0,
                                                        kpi["Montant_coupons_utilise"] / kpi["Montant_coupons_emis"], np.nan)
        kpi["Taux_utilisation_bons_quantite"] = np.where(kpi.get("Coupon_emis", 0) > 0,
                                                         kpi["Coupon_utilise"] / kpi["Coupon_emis"], np.nan)
        kpi["Taux_CA_genere_par_bons_sur_CA_HT"] = np.where(kpi["CA_HT"] > 0,
                                                            kpi["Montant_coupons_utilise"] / kpi["CA_HT"], np.nan)

        # Renommage colonnes lisibles
        rename_map = {
            "CA_TTC":"CA_TTC",
            "CA_HT":"CA_HT",
            "Purch_Total_HT":"Total_achats_HT",
            "Marge_brute":"Marge_brute",
            "Taux_marge":"Taux_marge",
            "Transactions":"Transactions",
            "Clients":"Clients",
            "Nouveau_client":"Nouveau_client",
            "Client_qui_reviennent":"Client_qui_reviennent",
            "Recurrence":"Recurrence",
            "Retention_rate":"Retention_rate",
            "Coupon_utilise":"Coupon_utilise",
            "Montant_coupons_utilise":"Montant_coupons_utilise",
            "Coupon_emis":"Coupon_emis",
            "Montant_coupons_emis":"Montant_coupons_emis",
            "Taux_utilisation_bons_montant":"Taux_utilisation_bons_montant",
            "Taux_utilisation_bons_quantite":"Taux_utilisation_bons_quantite",
            "Taux_CA_genere_par_bons_sur_CA_HT":"Taux_CA_genere_par_bons_sur_CA_HT",
            "Panier_moyen_client":"Panier_moyen_client",
            "Panier_moyen_non_client":"Panier_moyen_non_client",
            "Panier_moyen_avec_coupon":"Panier_moyen_avec_coupon",
            "Panier_moyen_sans_coupon":"Panier_moyen_sans_coupon",
        }
        kpi = kpi.rename(columns=rename_map)

        # Export Drive (transactions + coupons)
        st.subheader("☁️ Export Google Drive & Google Sheets (Fidélité)")

        def upload_to_drive(file_path, file_name, mime_type="application/octet-stream", folder_id=None):
            """Upload un fichier dans le dossier Google Drive (gestion Drive partagés incluse)."""
            try:
                folder_id = st.secrets["gcp"]["drive_folder_id"]
            except Exception:
                folder_id = None

            file_metadata = {"name": file_name}
            if folder_id:
                if folder_id.startswith("0A"):  # Drive partagé
                    file_metadata["driveId"] = folder_id
                    file_metadata["parents"] = []
                else:
                    file_metadata["parents"] = [folder_id]

            media = MediaIoBaseUpload(open(file_path, "rb"), mimetype=mime_type, resumable=True)
            query = f"name='{file_name}' and trashed=false"
            if folder_id and not folder_id.startswith("0A"):
                query += f" and '{folder_id}' in parents"

            existing = (
                drive_service.files()
                .list(q=query, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True)
                .execute()
                .get("files", [])
            )

            try:
                if existing:
                    file_id = existing[0]["id"]
                    drive_service.files().update(
                        fileId=file_id,
                        media_body=media,
                        supportsAllDrives=True
                    ).execute()
                else:
                    drive_service.files().create(
                        body=file_metadata,
                        media_body=media,
                        supportsAllDrives=True
                    ).execute()
                st.success(f"✅ Fichier '{file_name}' exporté sur Google Drive.")
            except Exception as e:
                st.error(f"❌ Erreur export Drive : {e}")

        if st.button("📤 Exporter Transactions & Coupons sur Drive"):
            try:
                upload_to_drive(TX_PATH, "transactions.parquet", "application/octet-stream")
                upload_to_drive(CP_PATH, "coupons.parquet", "application/octet-stream")
                st.success("✅ Transactions et coupons exportés sur Google Drive.")
            except Exception as e:
                st.error(f"❌ Erreur export Drive : {e}")

        # Mise à jour Google Sheets (même fichier 'KPI - La Tribu', onglet KPI_Mensuels)
        def update_sheet(spreadsheet_id, sheet_name, df):
            try:
                sh = gspread_client.open_by_key(spreadsheet_id)
                try:
                    ws = sh.worksheet(sheet_name)
                except gspread.WorksheetNotFound:
                    ws = sh.add_worksheet(title=sheet_name, rows="100", cols="20")
                ws.clear()
                ws.update("A1", [list(df.columns)] + df.astype(str).values.tolist())
                st.success(f"✅ Feuille '{sheet_name}' mise à jour avec {len(df)} lignes.")
            except Exception as e:
                st.error(f"❌ Erreur mise à jour Google Sheets : {e}")

        if st.button("📈 Mettre à jour Google Sheets Fidélité (KPI_Mensuels)"):
            update_sheet(SPREADSHEET_ID, "KPI_Mensuels", kpi)

        st.subheader("📊 Aperçu KPI Fidélité")
        st.dataframe(kpi, use_container_width=True)

    else:
        st.info("➡️ Importez les fichiers Transactions et Coupons pour démarrer la partie Fidélité.")

# ============================================================
# ONGLET STOCKS (reprend valostock_gsheet.py vers même Sheet)
# ============================================================
with tab_stock:
    st.subheader("📦 Valorisation de stock & export vers Google Sheets")

    if stock_files and product_file:
        stock_list = [pd.read_csv(f, sep=';') for f in stock_files]
        stocks_df = pd.concat(stock_list, ignore_index=True)
        products_df = pd.read_excel(product_file)

        # Harmonisation colonnes produits
        products_df = products_df.rename(columns={"SKU": "sku", "PurchasingPrice": "purchasing_price", "Brand": "brand"})
        products_df = products_df[["sku", "purchasing_price", "brand"]]

        stocks_df["sku"] = stocks_df["sku"].astype(str)
        products_df["sku"] = products_df["sku"].astype(str)

        merged_df = pd.merge(stocks_df, products_df, on="sku", how="left")
        merged_df["quantity"] = pd.to_numeric(merged_df["quantity"], errors="coerce")
        merged_df["purchasing_price"] = pd.to_numeric(merged_df["purchasing_price"], errors="coerce")
        merged_df.dropna(subset=["quantity", "purchasing_price"], inplace=True)
        merged_df["valorisation"] = merged_df["quantity"] * merged_df["purchasing_price"]

        date_import = datetime.today().strftime('%d-%m-%Y')

        report_df = merged_df.groupby(["organisationId", "brand"], as_index=False)["valorisation"].sum()
        report_df.insert(0, "date", datetime.today().strftime('%Y-%m-%d'))
        report_df = report_df[report_df["valorisation"] > 0].drop_duplicates()
        report_df["valorisation"] = report_df["valorisation"].round(2)

        # Chargement / mise à jour de l'historique local
        if os.path.exists(HISTO_FILE):
            historique_df = pd.read_csv(HISTO_FILE)
        else:
            historique_df = pd.DataFrame(columns=["date", "organisationId", "brand", "valorisation"])

        report_df["clef"] = report_df["date"] + "|" + report_df["organisationId"] + "|" + report_df["brand"]
        if not historique_df.empty:
            historique_df["clef"] = historique_df["date"] + "|" + historique_df["organisationId"] + "|" + historique_df["brand"]
            historique_df = historique_df[~historique_df["clef"].isin(report_df["clef"])]
        historique_df = pd.concat([historique_df, report_df], ignore_index=True)
        if "clef" in historique_df.columns:
            historique_df.drop(columns=["clef"], inplace=True)

        historique_df["date"] = pd.to_datetime(historique_df["date"])
        latest_date = historique_df["date"].max()
        historique_df["est_derniere_date"] = historique_df["date"] == latest_date
        historique_df["date"] = historique_df["date"].dt.strftime("%Y-%m-%d")

        # Nettoyage valorisation
        historique_df["valorisation"] = (
            historique_df["valorisation"]
            .astype(str)
            .str.replace("'", "", regex=False)
        )
        historique_df["valorisation"] = pd.to_numeric(historique_df["valorisation"], errors="coerce").round(2)

        historique_df.to_csv(HISTO_FILE, index=False)
        st.success(f"✅ Données ajoutées à l'historique stock ({len(report_df)} lignes).")

        # Bouton : update GSheet & mail
        if st.button("📤 Mettre à jour Google Sheets Stock (KPI_Stock) + envoyer le lien Looker"):
            try:
                df_all = _gsheet_upsert_dataframe(SPREADSHEET_ID, "KPI_Stock", historique_df)

                default_extra_recipients = [
                    "alexandre.audinot@latribu.fr",
                    "jm.lelann@latribu.fr",
                    "philippe.risso@firea.com",
                ]
                all_recipients = [DEFAULT_RECEIVER] + default_extra_recipients + [
                    e.strip() for e in emails_supp_stock.split(",") if e.strip() != ""
                ]

                msg = EmailMessage()
                msg["Subject"] = f"📊 Rapport de valorisation des stocks au {date_import}"
                msg["From"] = SMTP_USER
                msg["To"] = ", ".join(all_recipients)
                msg.set_content(
                    f"Bonjour,\n\nVoici le lien vers le tableau de bord dynamique de valorisation des stocks fournisseurs :\n👉 {LOOKER_URL}\n"
                )

                with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                    server.starttls()
                    server.login(SMTP_USER, SMTP_PASS)
                    server.send_message(msg)

                st.success("📈 Google Sheets (KPI_Stock) mis à jour et lien Looker envoyé par e-mail !")

            except Exception as e:
                st.error("❌ Erreur pendant la mise à jour ou l'envoi de l'e-mail.")
                st.exception(e)

        st.subheader("🗂️ Historique valorisation stock (toutes dates)")
        st.dataframe(historique_df, use_container_width=True)

    else:
        st.info("➡️ Importez au moins un fichier de stock et la base produit pour démarrer la partie Stock.")
