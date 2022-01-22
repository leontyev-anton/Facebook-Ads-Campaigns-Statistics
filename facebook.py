import os
from datetime import datetime, timedelta

import requests
from facebook_config import fb_token, fb_host, fb_account
from facebook_config import sn_user, sn_password, sn_account, sn_database, sn_table, sn_schema, sn_warehouse, sn_role_name
from functions import write_to_snowflake, log

# g_file - file with JSON key from BigQuery authorization
# g_project - BigQuery project name
# g_table_name - BigQuery tablename(with dataset) for write data Facebook Campaigns Spend
# g_logs_table_name - BigQuery tablename(with dataset) for write logs
# sn_ ... - credentials from Snowflake
# fb_token - fb token
# fb_account - fb account id
# fb_host - 'https://graph.facebook.com/v12.0/'

from functions import write_to_bigquery, write_to_snowflake, log

datetime_now = datetime.utcnow() + timedelta(hours=3)
script_begin = datetime_now.strftime('%Y-%m-%d %H:%M:%S')
log_text = ['']
log_error = [False]
log('\nStart:  ' + script_begin)

date_begin = datetime_now - timedelta(days=1)  # 7
date_end = datetime_now - timedelta(days=1)
# date_begin = datetime(2021, 12, 10, hour=0, minute=0, second=0)
# date_end = datetime(2022, 1, 7, hour=0, minute=0, second=0)

date = date_begin
while date <= date_end:
    date_str = date.strftime('%Y-%m-%d')
    try:
        response = requests.get(fb_host + 'act_' + fb_account + '/insights?access_token=' + fb_token +
                                '&fields=campaign_id,campaign_name,spend,clicks,impressions&level=campaign' +
                                '&time_range={"since":"' + date_str + '","until":"' + date_str + '"}')
        #log(response.text)
        data = response.json()['data']
    except Exception as e:
        log(f'Exception at request {date_str}: {e}', log_text=log_text)
        log_error[0] = True
    else:
        if len(data) > 0:
            campaigns_bq=[]
            for d in data:
                row = {'account_name': fb_account, 'date':  date_str, 'campaign_id': d['campaign_id'],
                       'campaign_name': d['campaign_name'], 'spend': d['spend'], 'clicks': d['clicks'],
                       'impressions': d['impressions']}
                campaigns_bq.append(row)
            #write_to_bigquery(campaigns_bq, g_file, g_project, g_table_name + date.strftime('%Y%m%d'), log_text=log_text, log_error=log_error)
            write_to_snowflake(campaigns_bq, sn_table + date.strftime('%Y%m%d'), sn_user, sn_password, sn_account, sn_database, sn_schema, sn_warehouse, sn_role_name, log_text=log_text, log_error=log_error)
        else:
            log(f'Request {date_str} is empty: len(data):{len(data)}', log_text=log_text)
    date += timedelta(days=1)

script_end = (datetime.utcnow() + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
row = [{'time_begin': script_begin, 'time_end': script_end, 'script': os.path.basename(__file__),
        'text': log_text[0], 'error': log_error[0]}]
#write_to_bigquery(row, g_file, g_project, g_logs_table_name, overwrite='WRITE_APPEND')  # write execution log into BigQuery
log('Finish:  ' + script_end)
