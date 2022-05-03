import pandas as pd
import numpy as np
import json
import os
import smtplib
import ssl
import pytz
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint
from PyPDF2 import PdfFileReader
from airflow.models import Variable
from datetime import datetime, timedelta

tzInfo = pytz.timezone('Asia/Bangkok')

def transform():
    datasource_path = "/opt/airflow/dags/data_source/eodb"
    output_path = "/opt/airflow/dags/output/eodb"

    def adding_ranking_eodb(df):
        print("df.coluns: {}".format(df.columns))
        df = df.drop(df[df.unit_2 == 'Rank'].index)
        rank_df = df[df.indicator.isna()]
        rank_df = rank_df.dropna(subset=['value'])
        rank_df['value'] = rank_df['value'].astype(float)
        list_col_use = ['country', 'year', 'master_index', 'organizer',
                        'index', 'sub_index', 'pillar', 'sub_pillar', 'sub_sub_pillar',
                        'indicator', 'sub_indicator', 'others', 'unit_2', 'ingest_date']
        df_no_null = rank_df.drop_duplicates(subset=list_col_use)
        col_list = ['year', 'master_index', 'organizer',
                    'index', 'sub_index', 'pillar', 'sub_pillar', 'sub_sub_pillar',
                    'indicator', 'sub_indicator', 'others', 'unit_2', 'ingest_date']
        select_df = df_no_null[col_list]
        sub_df = select_df.copy()
        sub_df = sub_df.drop_duplicates(subset=col_list)
        sub_df = sub_df.reset_index(drop=True)
        sub_df = sub_df.reset_index()
        sub_df = sub_df.rename(columns={'level_0': 'group'})
        merge_group_df = df_no_null.merge(
            sub_df, how='inner', left_on=col_list, right_on=col_list)
        merge_group_df['Rank'] = merge_group_df.groupby(
            ['group'])['value'].rank("dense", ascending=False)

        rank_df_1 = merge_group_df.melt(id_vars=merge_group_df.columns[:-1].tolist(),
                                        var_name="unit_2_1",
                                        value_name="value_1")
        rank_df_1['unit_2'] = np.where(
            True, rank_df_1['unit_2_1'], rank_df_1['unit_2'])
        rank_df_1['value'] = np.where(
            True, rank_df_1['value_1'], rank_df_1['value'])
        rank_df_1 = rank_df_1.drop(['unit_2_1', 'value_1', 'group'], axis=1)
        concat_df = pd.concat([df, rank_df_1], ignore_index=True)

        return concat_df

    def etl(source_link, meta_data):
        eodb_df = pd.read_excel(f'{datasource_path}/{source_link}',
                                sheet_name='All Data', header=2, engine="openpyxl")
        eodb_df.columns = eodb_df.columns.to_series().replace(
            'Unnamed:\s\d+', np.nan, regex=True).ffill().values
        first_col = eodb_df.columns.tolist()
        first_col = ['' if x != x else x for x in first_col]
        second_col = eodb_df.loc[0].tolist()
        col_lst = list(map('--'.join, zip(first_col, second_col)))
        eodb_df.columns = col_lst
        eodb_df = eodb_df.drop(0)
        eodb_df = eodb_df.reset_index(drop=True)
        var_col = eodb_df.columns[:5].tolist()
        df = eodb_df.melt(id_vars=var_col,
                          var_name="index",
                          value_name="value")
        df = df.dropna(subset=['--Economy'])
        rename_col = ['code', 'country', 'region',
                      'level_income', 'year', 'index', 'value']
        df.columns = rename_col
        df = df.drop(columns='code')
        new = df["index"].str.split("--", n=1, expand=True)
        df["topic"] = new[0]
        df["name"] = new[1]
        # Dropping old Name columns
        df.drop(columns=["index"], inplace=True)
        df['topic'] = df['topic'].str.lower().str.replace(' ', '')
        df['name'] = df['name'].str.lower().str.replace(' ', '')
        meta_data = pd.read_csv(f'{datasource_path}/{meta_data}', header=2)
        meta_data = meta_data.dropna(subset=['Topic'])
        meta_data['Topic'] = meta_data['Topic'].str.lower().str.replace(' ', '')
        meta_data['Indicator'] = meta_data['Indicator'].str.lower(
        ).str.replace(' ', '')
        eodb_full_df = df.merge(meta_data, how='left', left_on=[
                                'topic', 'name'], right_on=['Topic', 'Indicator'])
        eodb_full_df = eodb_full_df.reset_index(drop=True)
        eodb_full_df = eodb_full_df.rename(columns={'usb_index': 'sub_index'})
        eodb_full_df['others'] = np.nan
        eodb_full_df['organizer'] = 'World Bank'
        eodb_full_df['master_index'] = 'EoDB'
        ingest_date = datetime.now(tz=tzInfo)
        eodb_full_df['ingest_date'] = ingest_date.strftime("%Y-%m-%d %H:%M")

        list_col = ['country', 'region', 'level_income', 'year', 'value',
                    'Index', 'sub_index', 'pillar', 'sub_pillar', 'sub_sub_pillar',
                    'indicator', 'sub_indicator', 'others', 'unit_2', 'organizer', 'master_index', 'ingest_date']
        eodb_full_df = eodb_full_df.loc[:, eodb_full_df.columns.isin(list_col)]
        eodb_full_df.columns = eodb_full_df.columns.str.lower()
        eodb_full_df = adding_ranking_eodb(eodb_full_df)
        eodb_full_df = eodb_full_df.drop(['region', 'level_income'], axis=1)
        year_list = eodb_full_df['year'].unique()
        for year in year_list:
            final = eodb_full_df[eodb_full_df['year'] == year].copy()
            current_year = str(year)[0:4]
            final['year'] = current_year
            final.to_csv('{}/EoDB_{}_{}.csv'.format(output_path, current_year,
                         ingest_date.strftime("%Y%m%d%H%M%S")), index=False)

    def ingestion_init():
        # Get config
        json_file = open('{}/config.json'.format(datasource_path))
        conf_main = json.load(json_file)
        json_file.close()
        etl(source_link=conf_main['source_link'],
            meta_data=conf_main['meta_data'])

    ingestion_init()

default_args = {
    'owner': 'ETDA',
    'start_date': datetime(2022, 3, 31),
    'schedule_interval': None,
}

dag = DAG('EoDB',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)

def send_mail():
    index_name = "Ease of Doing Business Ranking (EoDB)"
    smtp_server = Variable.get("smtp_host")
    port = Variable.get("smtp_port")
    email_to = Variable.get("email_to")
    email_from = Variable.get("email_from")
    tzInfo = pytz.timezone('Asia/Bangkok')

    email_string = f"""
    Pipeline Success
    ------------------------------------------
    Index: {index_name}
    Ingestion Date: {datetime.now(tz=tzInfo).strftime("%Y/%m/%d %H:%M")}
    """

    try:
        server = smtplib.SMTP(smtp_server, port)
        server.sendmail(email_from, email_to, email_string)
    except Exception as e:
        print(e)
    finally:
        server.quit()

def store_to_hdfs(**kwargs):
    hdfs = PyWebHdfsClient(host=Variable.get("hdfs_host"),
                           port=Variable.get("hdfs_port"), user_name=Variable.get("hdfs_username"))
    my_dir = kwargs['directory']
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    path = "/opt/airflow/dags/output/eodb"

    os.chdir(path)

    for file in os.listdir():
        if file.endswith(".csv"):
            file_path = f"{path}/{file}"

            with open(file_path, 'r', encoding="utf8") as file_data:
                my_data = file_data.read()
                hdfs.create_file(
                    my_dir+f"/{file}", my_data.encode('utf-8'), overwrite=True)

                pprint("Stored! file: {}".format(file))
                pprint(hdfs.list_dir(my_dir))

def ingest_data():
    hdfs = PyWebHdfsClient(host=Variable.get("hdfs_host"),
                           port=Variable.get("hdfs_port"), user_name=Variable.get("hdfs_username"))
    source_file_byte = '/raw/index_dashboard/File_Upload/EODB/clean2_Historical-data---COMPLETE-dataset-with-scores.xlsx'

    data_source = hdfs.read_file(source_file_byte)

    with open('/opt/airflow/dags/data_source/eodb/clean2_Historical-data---COMPLETE-dataset-with-scores.xlsx', 'wb') as file_out:
        file_out.write(data_source)
        file_out.close()

    pprint("Ingested!")

with dag:
    ingestion = PythonOperator(
        task_id='ingestion',
        python_callable=ingest_data,
    )

    scrap_and_extract_transform = PythonOperator(
        task_id='scrap_and_extract_transform',
        python_callable=transform,
    )

    load_to_hdfs_raw_zone = PythonOperator(
        task_id='load_to_hdfs_raw_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/data/raw/index_dashboard/Global/EODB'},
    )

    load_to_hdfs_processed_zone = PythonOperator(
        task_id='load_to_hdfs_processed_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/data/processed/index_dashboard/Global/EODB'},
    )

    clean_up_output = BashOperator(
        task_id='clean_up_output',
        bash_command='rm -f /opt/airflow/dags/output/eodb/*.csv',
    )

    send_email = PythonOperator(
        task_id='send_email',
        python_callable=send_mail,
    )

ingestion >> scrap_and_extract_transform >> load_to_hdfs_raw_zone >> load_to_hdfs_processed_zone >> clean_up_output >> send_email
