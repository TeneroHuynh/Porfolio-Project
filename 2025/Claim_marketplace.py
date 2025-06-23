# -*- coding: utf-8 -*-
"""
Created on Fri feb 21 09:41:11 2025

@author: TeneroHuynh
"""

import gspread
import pandas as pd
import numpy as np
import sqlalchemy as sc
from oauth2client.service_account import ServiceAccountCredentials as sac


def connect_google_sheet(sheet_url, worksheet_name, credentials_path):
    """Get data from Google Sheet."""
    try:
        scope = ["https://spreadsheets.google.com/feeds",
                 "https://www.googleapis.com/auth/drive"]
        creds = sac.from_json_keyfile_name(credentials_path, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_url(sheet_url)
        worksheet = spreadsheet.worksheet(worksheet_name)
        return pd.DataFrame(worksheet.get_all_records())
    except Exception as e:
        print(f"Google Sheet connection failed: {e}")
        return pd.DataFrame()


def get_orders_from_db(con_engine):
    """Sample: Get sample order data from database."""
    query = """
        SELECT
            order_id,
            sales_channel_id,
            purchased_date
        FROM sample_orders
    """
    return pd.read_sql(query, con_engine, parse_dates=["purchased_date"])


def process_claim_data(df_claim, df_orders, con_engine):
    """Process claim data by merging with orders and mapping claim types."""
    # Sample mapping (fake data)
    df_sub_claim_type = pd.DataFrame({
        "id": [1, 2],
        "name": ["Damaged Item", "Missing Item"]
    })

    sub_claim_type_dict = dict(zip(df_sub_claim_type["name"],
                                   df_sub_claim_type["id"]))

    df_claim["sub_claim_type_id"] = df_claim[
        "Sub claim type"].map(sub_claim_type_dict)

    # Rename columns
    df_claim.rename(columns={
        "Order ID": "order_id",
        "Claim date": "claim_date",
        "Claim type": "claim_type",
        "Sub claim type": "sub_claim_type",
        "Case amount": "case_amount",
        "Cashflow Type": "cashflow_type",
        "Claim amount": "claim_amount"
    }, inplace=True)

    # Format datetime
    df_claim["claim_date"] = pd.to_datetime(
        df_claim["claim_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df_orders["purchased_date"] = pd.to_datetime(
        df_orders["purchased_date"], errors="coerce").dt.strftime(
            "%Y-%m-%d").fillna("null")

    # Merge and clean
    df_merged = df_claim.merge(df_orders, on="order_id", how="left")
    df_merged.drop(columns=["claim_type",
                            "sub_claim_type"], errors='ignore', inplace=True)

    df_merged["id"] = (
        df_merged["order_id"].astype(str) + "_" +
        df_merged["sub_claim_type_id"].astype(str))

    num_cols = ["case_amount", "claim_amount"]
    int_cols = ["sales_channel_id", "sub_claim_type_id"]
    str_cols = ["order_id", "Reason", "id", "cashflow_type"]

    for col in num_cols:
        df_merged[col] = pd.to_numeric(
            df_merged[col], errors="coerce").fillna(0)
    for col in int_cols:
        df_merged[col] = pd.to_numeric(
            df_merged[col], errors="coerce").fillna(0).astype(int)
    df_merged[str_cols] = df_merged[str_cols].astype(str)

    df_merged = df_merged.drop_duplicates()
    df_merged = df_merged[df_merged["sales_channel_id"] != 0]

    return df_merged

def create_query_insert_into(dataframe, name_table_update):
    """
    Parameters.

    ----------
    dataframe : TYPE
        DESCRIPTION.
    name_table_update : TYPE
        DESCRIPTION.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    columns = ""
    values = ""
    odku = ""

    end_col = dataframe.columns[-1]
    for col in dataframe.columns:
        if col == end_col:
            columns += col
            values += "%s"
            odku += col + "=" + "VALUES(" + col + ")"
        else:
            columns += col + ", "
            values += "%s, "
            odku += col + "=" + "VALUES(" + col + "), "

    return (
        "INSERT INTO "
        + name_table_update
        + " ("
        + columns
        + ") "
        + "VALUES("
        + values
        + ") "
        + "ON DUPLICATE KEY UPDATE "
        + odku
    )

def main():
    # Sample connection strings
    con_engine = sc.create_engine
    ("mysql+pymysql://user:password@localhost:3306/sample_db")

    # Sample Google Sheet info 
    sheet_url = "https://docs.google.com/spreadsheets/d/sample_sheet_id/edit#gid=0"
    worksheet_name = "Data table"
    credentials_path = "sample_key.json"

    df_claim = connect_google_sheet(sheet_url, worksheet_name, credentials_path)
    df_orders = get_orders_from_db(con_engine)
    df_final = process_claim_data(df_claim, df_orders, con_engine)

    # Generate query and insert new data
    query = create_query_insert_into(df_final, 'sample_claim_orders')
    data = (
        df_final.replace({"": None})
        .replace({np.nan: None})
        .to_records(index=False)
        .tolist()
    )
    con_engine.execute(query, data)

main()
