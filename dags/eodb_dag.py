import pandas as pd
import numpy as np
import json
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def transform():
    datasource_path = "/opt/airflow/dags/data_source/eodb"
    output_path = "/opt/airflow/dags/output/eodb"

    def etl(source_link, meta_data):
        eodb_df = pd.read_excel(f'{datasource_path}/{source_link}', sheet_name='All Data', header=2, engine="openpyxl")
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
        list_col = ['country', 'region', 'level_income', 'year', 'value',
                    'Index', 'sub_index', 'pillar', 'sub_pillar', 'sub_sub_pillar',
                    'indicator', 'sub_indicator', 'others', 'organizer', 'master_index']
        eodb_full_df = eodb_full_df.loc[:, eodb_full_df.columns.isin(list_col)]
        ingest_date = datetime.now()

        eodb_full_df.to_csv('{}/EODB_{}_{}.csv'.format(output_path, 2022,
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
    'depends_on_past': False,
    'start_date': '2021-01-25',
    'email': ['oulespaul@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@yearly',
}

dag = DAG('EoDB', default_args=default_args, catchup=False)

with dag:
    ingestion = BashOperator(
        task_id='ingestion',
        bash_command='cd /opt/airflow/dags/data_source/eodb  && ./sources.sh ',
    )

    scrap_and_extract_transform = PythonOperator(
        task_id='scrap_and_extract_transform',
        python_callable=transform,
    )

ingestion >> scrap_and_extract_transform
