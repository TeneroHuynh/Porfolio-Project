# -*- coding: utf-8 -*-
"""
Created on Fri May 02 14:16:55 2025

@author: TeneroHuynh
"""

import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
import sqlalchemy as sc


def get_sales_dashboard(con_engine, start_date, end_date):
    query = f"""
        SELECT
            product_id,
            SUM(quantity) as quantity,
            SUM(revenue) as revenue,
            SUM(quantity_fc) as quantity_fc,
            SUM(revenue_fc) as revenue_fc,
            SUM(profit_fc) as profit_fc,
            country_id,
            report_date AS date
        FROM sample_db.sample_sales_dashboard
        WHERE report_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY product_id, country_id, report_date
    """
    return pd.read_sql(query, con_engine)


def read_excel_data(file_path, con_engine_sta):
    target_columns = ['sku', 'quantity', 'revenue',
                      'profit', 'month', 'year', 'country_id']
    xls = pd.ExcelFile(file_path)

    df_list = []
    for sheet in xls.sheet_names:
        df = xls.parse(sheet)
        df = df[target_columns].rename(str.lower, axis='columns')
        df_list.append(df)

    df_all = pd.concat(df_list, ignore_index=True)

    # Check duplicates
    group_keys = ['sku', 'month', 'year', 'country_id']
    duplicates = df_all.duplicated(subset=group_keys, keep=False)
    if duplicates.any():
        duplicated_rows = df_all.loc[duplicates, group_keys]
        raise ValueError(
            f"Dup SKUs found: \n{duplicated_rows.to_string(index=False)}")

    # Load product mapping from DB
    df_product = pd.read_sql("""
        SELECT id AS product_id, product_sku AS sku, group_id, brand_id
        FROM sample_db.sample_products
        """, con_engine_sta)

    df_all = df_all.merge(df_product, on='sku', how='left')

    if df_all['product_id'].isnull().any():
        invalid_rows = df_all[df_all['product_id'].isnull()]
        order_list = ", ".join(
            invalid_rows['sku'].drop_duplicates().astype(str).tolist())
        raise ValueError(f"Found SKU(s) not exist in database: {order_list}")

    df_all = df_all[df_all['product_id'].notna()].copy()

    df_all['unit_profit'] = df_all.apply(
        lambda row: row['profit'] / row['quantity'] if row['quantity'] > 0 else np.nan, axis=1)
    df_all['date'] = pd.to_datetime(df_all[['year', 'month']].assign(day=1))
    min_date = df_all['date'].dt.strftime('%Y-%m-%d').min()

    return df_all, min_date


def extend_missing_months(df, min_date):
    df = df.sort_values(['product_id', 'country_id', 'date'])
    current_month = datetime.now(dt.UTC).replace(day=1).strftime('%Y-%m-%d')
    full_months = pd.date_range(start=min_date, end=current_month, freq='MS')

    all_groups = df[['product_id', 'country_id',
                     'group_id', 'brand_id']].drop_duplicates()
    results = []
    for _, row in all_groups.iterrows():
        pid = row['product_id']
        cid = row['country_id']
        gid = row['group_id']
        bid = row['brand_id']

        group_df = df[(df['product_id'] == pid) &
                      (df['country_id'] == cid)].copy()
        existing_months = set(group_df['date'])
        missing_months = [d for d in full_months if d not in existing_months]

        for new_date in missing_months:
            new_row = {'product_id': pid,
                       'country_id': cid,
                       'date': new_date,
                       'month': new_date.month,
                       'year': new_date.year,
                       'unit_profit': np.nan,
                       'quantity': np.nan,
                       'revenue': np.nan,
                       'profit': np.nan,
                       'group_id': gid,
                       'brand_id': bid}
            group_df = pd.concat([group_df,
                                  pd.DataFrame([new_row])], ignore_index=True)

        results.append(group_df)

    df_extended = pd.concat(results, ignore_index=True).sort_values(
        ['product_id', 'country_id', 'date'])
    df_extended['month'] = df_extended['date'].dt.month
    df_extended['year'] = df_extended['date'].dt.year

    return df_extended


def fill_null_unit_profit(df, con_engine_sco):
    df = df.sort_values(['product_id',
                         'country_id', 'date']).reset_index(drop=True)
    df['unit_profit_filled'] = df['unit_profit']

    for i, row in df[df['unit_profit_filled'].isna()].iterrows():
        pid = row['product_id']
        cid = row['country_id']
        curr_date = row['date']

        prev_rows = df[(df['product_id'] == pid) &
                       (df['country_id'] == cid) &
                       (df['date'] < curr_date)].sort_values('date').tail(3)

        if not prev_rows.empty:
            median_val = prev_rows['unit_profit_filled'].median()
            df.at[i, 'unit_profit_filled'] = median_val

    # Fill by group and brand averages
    for level in ['group_id', 'brand_id']:
        avg = df.dropna(subset=['unit_profit_filled']).groupby(
            [level, 'month', 'year', 'country_id'])[
                'unit_profit_filled'].mean().reset_index()
        avg_col = f'unit_profit_{level}_avg'
        avg = avg.rename(columns={'unit_profit_filled': avg_col})
        df = df.merge(avg, on=[level, 'month',
                               'year', 'country_id'], how='left')
        df['unit_profit_filled'] = df['unit_profit_filled'].fillna(df[avg_col])

    df['unit_profit'] = df['unit_profit_filled']
    df.drop(columns=['unit_profit_filled'], inplace=True)

    # Insert to database (optional)
    return df


def get_unit_profit(con_engine_sco, start_date, end_date):
    query = f"""
        SELECT date, product_id, country_id, quantity, unit_profit
        FROM sample_db.sample_unit_profit
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """
    return pd.read_sql(query, con_engine_sco, parse_dates=["date"])


def process_and_merge(df, df_sales_db, con_engine_sta):
    df['product_id'] = df['product_id'].astype(int)
    df_sales_db['date'] = pd.to_datetime(df_sales_db['date'])
    df_sales_db['year_month'] = df_sales_db['date'].dt.strftime('%Y%m')
    df['year_month'] = df['date'].dt.strftime('%Y%m')

    df_profit = df[['product_id', 'country_id',
                    'quantity', 'year_month', 'unit_profit']].drop_duplicates()

    df_qty_month = df_sales_db.groupby(
        ['product_id', 'country_id',
         'year_month'])['quantity'].sum().reset_index()

    df_merge = df_profit.merge(df_qty_month, on=[
        'product_id', 'country_id', 'year_month'], how='left')
    df_merge['quantity_x'] = df_merge['quantity_x'].fillna(0)

    condition_outlier = (df_merge['unit_profit'] > 300) & (
        df_merge['quantity_y'] > df_merge['quantity_x'])
    df_outlier = df_merge[condition_outlier]
    df_normal = df_merge[~condition_outlier]

    ratio = df_outlier['quantity_y'] / df_outlier['quantity_x']
    df_outlier.loc[:, 'unit_profit'] = df_outlier[
        'unit_profit'] * (1 + np.log(ratio)) / ratio

    df_combined = pd.concat([df_normal, df_outlier], ignore_index=True)
    df_profit = df_combined[['product_id', 'country_id',
                             'year_month', 'unit_profit']].drop_duplicates()

    df_final = df_sales_db.merge(
        df_profit, on=['product_id', 'country_id', 'year_month'], how='left')
    df_final['profit'] = df_final['quantity'] * df_final['unit_profit']
    df_final['profit'] = df_final['profit'].fillna(0)
    df_final.drop(columns=['unit_profit', 'year_month'], inplace=True)

    df_final['id'] = (df_final['product_id'].astype(str) +
                      '_' + df_final['country_id'].astype(str) +
                      '_' + df_final['date'].dt.strftime('%Y%m%d'))
    df_final = df_final.astype(
        {'product_id': int,
         'country_id': int,
         'quantity': int,
         'profit': float})
    df_final['date'] = df_final['date'].dt.strftime('%Y-%m-%d')

    return df_final


# Sample database connections
con_engine_sta = sc.create_engine(
    "mysql+pymysql://user:password@localhost:3306/sample_db")
con_engine_sco = sc.create_engine(
    "mysql+pymysql://user:password@localhost:3306/sample_db")

end_date = datetime.now(dt.UTC).strftime('%Y-%m-%d')

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

def run_sample_file():
    file_path = "sample_path/data_sample.xlsx"
    df_excel, start_date = read_excel_data(file_path, con_engine_sta)
    df_extended = extend_missing_months(df_excel, start_date)
    df_filled = fill_null_unit_profit(df_extended, con_engine_sco)
    df_sales_db = get_sales_dashboard(con_engine_sco, start_date, end_date)
    df_final = process_and_merge(df_filled, df_sales_db, con_engine_sta)

    query = create_query_insert_into(df_final, 'sample_sales_dashboard_temp')
    data = (df_final.replace({"": None}).replace(
        {np.nan: None}).to_records(index=False).tolist())
    con_engine_sco.execute(query, data)

def run_sample_dtb():
    """
    Run pipeline when using existing data from the database.
    """
    start_date = datetime.now(dt.UTC).strftime('%Y-%m-01')
    df_unit_profit = get_unit_profit(con_engine_sco, start_date, end_date)
    df_sales_db = get_sales_dashboard(con_engine_sco, start_date, end_date)
    df_final = process_and_merge(df_unit_profit, df_sales_db, con_engine_sta)

    query = create_query_insert_into(df_final, 'sample_sales_dashboard_temp')
    data = (df_final.replace({"": None}).replace(
        {np.nan: None}).to_records(index=False).tolist())
    con_engine_sco.execute(query, data)


# run_sample_file()
run_sample_file()

