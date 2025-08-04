import pandas as pd
from sqlalchemy import create_engine
import pyodbc
import logging
import urllib
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE = os.getenv("DATABASE")
UID = os.getenv("UID")
PWD = os.getenv("PWD")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_from_excel(file_path):
    try:
        df = pd.read_excel(file_path, engine='openpyxl')
        logging.info(f"Successfully extracted {len(df)} rows from {file_path}")
        return df
    except Exception as e:
        logging.error(f"Error extracting data from Excel: {e}")
        raise

def transform_data(df):
    try:
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
        df = df.dropna(subset=['client_id', 'name'])
        logging.info(f"After removing rows with missing critical values: {len(df)} rows")
        numeric_columns = [
            'estimated_income', 'superannuation_savings', 'credit_card_balance',
            'bank_loans', 'bank_deposits', 'checking_accounts', 'saving_accounts',
            'foreign_currency_account', 'business_lending'
        ]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(',', '').astype(float)
        integer_columns = ['age', 'location_id', 'amount_of_credit_cards', 'properties_owned', 'risk_weighting']
        for col in integer_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce', downcast='integer')
        date_columns = ['joined_bank', 'last_contact', 'last_meeting']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format='%d-%m-%Y', errors='coerce')
        text_columns = [
            'name', 'sex', 'banking_contact', 'nationality', 'occupation',
            'investment_advisor', 'fee_structure', 'loyalty_classification', 'banking_relationship'
        ]
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.title()
        if 'sex' in df.columns:
            df['sex'] = df['sex'].str.upper().replace({'MALE': 'M', 'FEMALE': 'F'}, regex=True).fillna('Unknown')
        df[numeric_columns] = df[numeric_columns].fillna(0)
        df[integer_columns] = df[integer_columns].fillna(0)
        df[text_columns] = df[text_columns].fillna('Unknown')
        df = df.drop_duplicates(subset=['client_id'], keep='first')
        logging.info(f"After removing duplicates: {len(df)} rows")
        if 'risk_weighting' in df.columns:
            df['risk_category'] = pd.cut(
                df['risk_weighting'],
                bins=[-float('inf'), 1, 2, 3, float('inf')],
                labels=['very low', 'low', 'medium', 'high'],
                include_lowest=True
            )
        logging.info(f"Transformed data: {len(df)} rows after processing")
        return df
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        raise

def load_to_sql_server(df, table_name, params):
    try:
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
        with engine.connect() as conn:
            df.to_sql(table_name, con=conn, if_exists='append', index=False, schema='dbo')
        logging.info(f"Successfully loaded {len(df)} rows into {table_name}")
    except Exception as e:
        logging.error(f"Error loading data to SQL Server: {e}")
        raise

def main():
    excel_file = 'F:\\Dataset_practice\\Atqor project\\Banking_Clients.xlsx'
    
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER=DESKTOP-65CHE84\\SQLEXPRESS;"
        f"DATABASE={DATABASE};"
        f"UID={UID};"
        f"PWD={PWD};"
    )
    table_name = 'client_data'

    try:
        df = extract_from_excel(excel_file)
        df_transformed = transform_data(df)
        load_to_sql_server(df_transformed, table_name, params)
        logging.info("ETL process completed successfully")
    except Exception as e:
        logging.error(f"ETL process failed: {e}")
        raise

if __name__ == "__main__":
    main()