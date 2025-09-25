from flask import Flask, request, jsonify
import pandas as pd
import io
import re
from pathlib import Path

app = Flask(__name__)

STANDARD_COLS = [
    'date', 'fuel_type', 'amount', 'price', 'number_plate',
    'card_number', 'station_name', 'invoice_number',
    'price_without_tax', 'price_with_tax', 'source'
]

# ==============================
# 📌 Cleaning Helper
# ==============================
def clean_number_plate(plate):
    if pd.isna(plate):
        return plate
    s = str(plate)
    s = s.replace("Plate No.", "")
    s = re.sub(r'\bสบ\.?\b', '', s)
    return s.strip().upper()

# ==============================
# 📌 Bangchak processor (full logic)
# ==============================
def process_bangchak(file_bytes):
    df = pd.read_excel(io.BytesIO(file_bytes))
    df = df.iloc[17:].reset_index(drop=True)
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)
    df = df.dropna(subset=['Card no.'])
    df = df.iloc[:, [0, 15, 16, 22, 13, 3, 3, 11, 18, 20]]
    df.iloc[:, 0] = df.iloc[:, 0].astype(str)
    df = df[~df.iloc[:, 0].str.startswith('Department:')]

    header_col_3 = df.columns[4]
    header_col_4 = df.columns[5]

    valid_fuel_types = [
        "DIESEL", "HI DIESEL S", "HI DIESEL S B10", "HI DIESEL S B7",
        "HI PREMIUM DIESEL S B7", "GASOHOL E20S EVO", "NGV"
    ]
    fuel_col_idx = 4
    fuel_type_series = df.iloc[:, fuel_col_idx].where(
        df.iloc[:, fuel_col_idx].isin(valid_fuel_types)
    ).ffill()

    if df.iloc[0, 4] in valid_fuel_types:
        df.iloc[0, 4] = header_col_3
        df.iloc[0, 5] = header_col_4

    for i in range(1, len(df)):
        if df.iloc[i, 4] in valid_fuel_types:
            df.iloc[i, 4] = df.iloc[i - 1, 4]
            df.iloc[i, 5] = df.iloc[i - 1, 5]

    df = df[~df.iloc[:, 0].str.startswith('Card no.')]
    df.columns = df.iloc[0]

    new_column_names = {
        df.columns[0]: 'date',
        df.columns[1]: 'amount_DIESEL',
        df.columns[2]: 'amount_NGV',
        df.columns[3]: 'price',
        df.columns[4]: 'number_plate',
        df.columns[5]: 'card_number',
        df.columns[6]: 'station_name',
        df.columns[7]: 'invoice_number',
        df.columns[8]: 'price_without_tax',
        df.columns[9]: 'price_with_tax',
    }
    df.rename(columns=new_column_names, inplace=True)

    df.insert(1, 'fuel_type', fuel_type_series.reindex(df.index).values)

    df["source"] = "Bangchak"
    df['amount'] = df['amount_DIESEL'] + df['amount_NGV']
    df["invoice_number"] = df["invoice_number"].astype(str)

    df = df[['date', 'fuel_type', 'amount', 'price',
             'number_plate', 'card_number', 'station_name',
             'invoice_number', 'price_without_tax', 'price_with_tax', 'source']]

    df['number_plate'] = df['number_plate'].apply(clean_number_plate)
    return df

# ==============================
# 📌 PTT processor (full logic)
# ==============================
def process_ptt(file_bytes):
    df = pd.read_excel(io.BytesIO(file_bytes))
    df = df.iloc[18:].reset_index(drop=True)
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)
    df = df.dropna(subset=['Card no.'])
    df = df.iloc[:, [0, 15, 16, 22, 13, 3, 3, 11, 18, 20]]
    df.iloc[:, 0] = df.iloc[:, 0].astype(str)
    df = df[~df.iloc[:, 0].str.startswith('Department:')]

    header_col_3 = df.columns[4]
    header_col_4 = df.columns[5]

    valid_fuel_types = [
        "DIESEL", "HI DIESEL S", "HI DIESEL S B10", "HI DIESEL S B7",
        "HI PREMIUM DIESEL S B7", "GASOHOL E20S EVO", "NGV"
    ]
    fuel_col_idx = 4
    fuel_type_series = df.iloc[:, fuel_col_idx].where(
        df.iloc[:, fuel_col_idx].isin(valid_fuel_types)
    ).ffill()

    if df.iloc[0, 4] in valid_fuel_types:
        df.iloc[0, 4] = header_col_3
        df.iloc[0, 5] = header_col_4

    for i in range(1, len(df)):
        if df.iloc[i, 4] in valid_fuel_types:
            df.iloc[i, 4] = df.iloc[i - 1, 4]
            df.iloc[i, 5] = df.iloc[i - 1, 5]

    df = df[~df.iloc[:, 0].str.startswith('Card no.')]
    df.columns = df.iloc[0]

    new_column_names = {
        df.columns[0]: 'date',
        df.columns[1]: 'amount_DIESEL',
        df.columns[2]: 'amount_NGV',
        df.columns[3]: 'price',
        df.columns[4]: 'number_plate',
        df.columns[5]: 'card_number',
        df.columns[6]: 'station_name',
        df.columns[7]: 'invoice_number',
        df.columns[8]: 'price_without_tax',
        df.columns[9]: 'price_with_tax',
    }
    df.rename(columns=new_column_names, inplace=True)

    df.insert(1, 'fuel_type', fuel_type_series.reindex(df.index).values)

    df["source"] = "PTT"
    df['amount'] = df['amount_DIESEL'] + df['amount_NGV']
    df["invoice_number"] = df["invoice_number"].astype(str)

    df = df[['date', 'fuel_type', 'amount', 'price',
             'number_plate', 'card_number', 'station_name',
             'invoice_number', 'price_without_tax', 'price_with_tax', 'source']]

    df['number_plate'] = df['number_plate'].apply(clean_number_plate)
    return df

# ==============================
# 📌 Caltex processor
# ==============================
def process_caltex(file_bytes):
    sheets = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None, engine="openpyxl")
    df = pd.concat([d.assign(sheet_name=name) for name, d in sheets.items()], ignore_index=True)
    df = df[['Transaction Date and Time','Product','Quantity', 'Pump Price',
             'License Plate','Card Number','Location Name','Reference No',
             'Customer Value Tax Inclusive','Customer Value Tax Exclusive']]

    df.rename(columns={
        'Transaction Date and Time': 'date',
        'Product': 'fuel_type',
        'Quantity': 'amount',
        'Pump Price': 'price',
        'License Plate': 'number_plate',
        'Card Number': 'card_number',
        'Location Name': 'station_name',
        'Reference No': 'invoice_number',
        'Customer Value Tax Exclusive': 'price_without_tax',
        'Customer Value Tax Inclusive': 'price_with_tax'
    }, inplace=True)

    df["source"] = "Caltex"
    df = df.reindex(columns=STANDARD_COLS)
    df['number_plate'] = df['number_plate'].apply(clean_number_plate)
    return df

# ==============================
# 📌 PT processor
# ==============================
def process_pt(file_bytes):
    df = pd.read_excel(io.BytesIO(file_bytes))
    df = df.iloc[6:].reset_index(drop=True)
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)
    df = df[['DATE','PRODUCT_TYPE','LITRE','UNIT_PRICE',
             'LICENSE_PLATE_NO','CARD_NO','BRANCH_NAME',
             'เลขที่ใบกำกับ','Amount Ex-vat','AMOUNT']]

    df.rename(columns={
        'DATE': 'date',
        'PRODUCT_TYPE': 'fuel_type',
        'LITRE': 'amount',
        'UNIT_PRICE': 'price',
        'LICENSE_PLATE_NO': 'number_plate',
        'CARD_NO': 'card_number',
        'BRANCH_NAME': 'station_name',
        'เลขที่ใบกำกับ': 'invoice_number',
        'Amount Ex-vat': 'price_without_tax',
        'AMOUNT': 'price_with_tax'
    }, inplace=True)

    df["source"] = "PT"
    df = df.reindex(columns=STANDARD_COLS)
    df['number_plate'] = df['number_plate'].apply(clean_number_plate)
    return df

# ==============================
# 📌 Flask Routes
# ==============================
@app.route("/upload/<vendor>", methods=["POST"])
def upload(vendor):
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files["file"]
    file_bytes = file.read()

    try:
        if vendor.lower() == "bangchak":
            df = process_bangchak(file_bytes)
        elif vendor.lower() == "ptt":
            df = process_ptt(file_bytes)
        elif vendor.lower() == "caltex":
            df = process_caltex(file_bytes)
        elif vendor.lower() == "pt":
            df = process_pt(file_bytes)
        else:
            return jsonify({"error": f"Vendor {vendor} not supported"}), 400

        # ✅ return JSON instead of Excel
        return jsonify(df.to_dict(orient="records"))

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "message": "Fuel ETL API running",
        "usage": "POST /upload/<vendor> with multipart/form-data {file: <excel>}",
        "vendors": ["ptt", "bangchak", "caltex", "pt"]
    })

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
