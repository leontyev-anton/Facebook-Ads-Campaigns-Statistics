from google.cloud import bigquery
from google.oauth2 import service_account
import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd


def write_to_snowflake(data, sn_table, sn_user, sn_password, sn_account, sn_database, sn_schema, sn_warehouse, sn_role_name, log_text=None, log_error=None):
    try:
        #log(data)
        df = pd.DataFrame(data)
        #log(df.to_string())
        #log(df.info(verbose=True))
        engine = create_engine('snowflake://{user}:{password}@{account_identifier}/{database_name}/{schema_name}?warehouse={warehouse_name}&role={role_name}'
            .format(user=sn_user, password=sn_password, account_identifier=sn_account, database_name=sn_database,
                    schema_name=sn_schema, warehouse_name=sn_warehouse, role_name=sn_role_name))
        connection = engine.connect()
        df.to_sql(name=sn_table, con=engine, if_exists='replace', index=False,
                  dtype={'account_name': sqlalchemy.types.String(length=30), 'impressions': sqlalchemy.types.Integer(),
                         'date': sqlalchemy.types.String(length=10), 'campaign_id': sqlalchemy.types.String(length=30),
                         'campaign_name': sqlalchemy.types.String(length=150), 'spend': sqlalchemy.types.Numeric(precision=38, scale=9),
                         'clicks': sqlalchemy.types.Integer(), 'impressions': sqlalchemy.types.Integer()})
    except Exception as e:
        log(f'Error. Can\'t transform data or write table in SnowFlake: {e}', log_text=log_text)
        if log_error != None:
            log_error[0] = True
    else:
        log(f'Table in Snowflake {sn_database}.{sn_table} is successfully created. Number of rows: {len(df.index)}', log_text=log_text)
    finally:
        connection.close()
        engine.dispose()


def write_to_bigquery(data, g_file, g_project, g_table_name, schema=[], overwrite='WRITE_TRUNCATE', log_text=None, log_error=None):
    try:
        credentials = service_account.Credentials.from_service_account_file(g_file, scopes=["https://www.googleapis.com/auth/cloud-platform"])
        client = bigquery.Client(credentials=credentials, project=g_project)
        if len(schema) == 0:
            job_config = bigquery.LoadJobConfig(autodetect=True, write_disposition=overwrite, ignore_unknown_values=True)
        else:
            job_config = bigquery.LoadJobConfig(autodetect=False, write_disposition=overwrite, ignore_unknown_values=True, schema=schema)
        client.load_table_from_json(data, g_table_name, job_config=job_config).result()
    except Exception as e:
        log(f'Error. Can\'t write table in BigQuery: {e}', log_text=log_text)
        if log_error != None:
            log_error[0] = True
    else:
        log(f'Table in BigQuery {g_table_name} is successfully created. Number of rows: {len(data)}', log_text=log_text)


def log(text, log_text=None, end='\n'):
    print(text, end=end)
    if log_text != None:
        log_text[0] += text + end
