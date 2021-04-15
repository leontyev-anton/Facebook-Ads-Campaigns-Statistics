from google.cloud import bigquery
from google.oauth2 import service_account

def write_to_bigquery(data, g_file, g_project, g_table_name, schema=[], overwrite='WRITE_TRUNCATE', log_text=None, log_error=None):
    try:
        credentials = service_account.Credentials.from_service_account_file(g_file, scopes=["https://www.googleapis.com/auth/cloud-platform"])
        client = bigquery.Client(credentials=credentials, project=g_project)
        if len(schema) == 0:
            job_config = bigquery.LoadJobConfig(autodetect=True, write_disposition=overwrite, ignoreUnknownValues=True)
        else:
            job_config = bigquery.LoadJobConfig(autodetect=False, write_disposition=overwrite,
                                                ignoreUnknownValues=True, schema=schema)
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