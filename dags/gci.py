from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint
from decimal import Decimal
import pandas as pd
import os
import json


def transform(source_file_name, series_dict):
    datasource_path = "/opt/airflow/dags/data_source/gci"
    output_path = "/opt/airflow/dags/output/gci"

    def ingestion_n_transform():
        df = pd.read_excel('{}/{}'.format(datasource_path, source_file_name),
                           sheet_name='Data', skiprows=3, engine="openpyxl")

        i = 3
        df = pd.concat([df.iloc[:, :i],
                        pd.DataFrame('',
                                     columns=['sub_index', 'pillar', 'sub_pillar', 'sub_sub_pillar', 'indicator',
                                              'sub_indicator', 'others'],
                                     index=df.index), df.iloc[:, i:]],
                       axis=1)

        df.rename(columns={'Edition': 'year',
                           'Series unindented': 'index_name'}, inplace=True)
        df.drop('Placement', axis=1, inplace=True)
        df.drop('GLOBAL ID', axis=1, inplace=True)
        df.drop('Series', axis=1, inplace=True)

        def get_value_from_key(key, sub_key):
            return series_dict.get(str(key), {}).get(sub_key, "")

        df["pillar"] = df["Code GCR"].apply(
            get_value_from_key, args=("pillar",))
        df["sub_pillar"] = df["Code GCR"].apply(
            get_value_from_key, args=("sub_pillar",))
        df["sub_sub_pillar"] = df["Code GCR"].apply(
            get_value_from_key, args=("sub_pillar_pillar",))
        df["indicator"] = df["Code GCR"].apply(
            get_value_from_key, args=("indicator",))
        df["sub_index"] = df["Code GCR"].apply(
            get_value_from_key, args=("sub_index",))
        df["sub_indicator"] = df["Code GCR"].apply(
            get_value_from_key, args=("sub_indicator",))
        df["others"] = df["Code GCR"].apply(
            get_value_from_key, args=("others",))

        df.drop(['Code GCR', 'index_name'], axis=1, inplace=True)

        df = df.melt(
            id_vars=['Dataset', 'year', 'sub_index', 'pillar', 'sub_pillar',
                     'sub_sub_pillar', 'indicator', 'sub_indicator', 'others', 'Attribute'],
            var_name="Country",
            value_name="value"
        )

        def is_number(value):
            try:
                Decimal(value)
                return True
            except Exception:
                return False

        ingest_date = datetime.now()

        df['organizer'] = 'WEF'
        df['master_index'] = 'GCI'
        df['ingest_date'] = ingest_date.strftime("%Y/%m/%d %H:%M")
        df.rename(columns={'Attribute': 'unit_2',
                           'Dataset': 'index', 'Country': 'country'}, inplace=True)

        df['unit_2'].replace(['Value'], ['Score'], inplace=True)
        df = df[df['unit_2'].isin(['Rank', 'Score'])]

        # Filter value
        df = df[df['value'].apply(is_number)]
        df = df[~df['value'].isnull()]

        col = ["country", "year", "master_index", "organizer", "index", "sub_index", "pillar", "sub_pillar", "sub_sub_pillar",
               "indicator", "sub_indicator", "others", "unit_2", "value", "ingest_date"]

        df = df[col]

        year_list = df['year'].unique()

        for year in year_list:
            final = df[df['year'] == year].copy()
            current_year = str(year)[0:4]
            final['year'] = current_year

            final.to_csv('{}/GCI_{}_{}.csv'.format(
                output_path,
                current_year,
                ingest_date.strftime("%Y%m%d%H%M%S")),
                index=False)

    def ingestion_init():
        # Get config
        json_file = open('{}/config.json'.format(datasource_path))
        conf_main = json.load(json_file)
        json_file.close()

        for config in conf_main:
            ingestion_n_transform(
                source_file_name=config['source_file_name'],
                series_dict=config['series_dict'],
            )

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

dag = DAG('gci', default_args=default_args, catchup=False)


def store_to_hdfs():
    hdfs = PyWebHdfsClient(host='10.121.101.130',
                           port='50070', user_name='hdfs')
    my_dir = '/raw/index_dashboard/Global/GCI'
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    path = "/opt/airflow/dags/output/gci"

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


with dag:
    ingestion = BashOperator(
        task_id='ingestion',
        bash_command='cd /opt/airflow/dags/data_source/gci &&  curl -LfO "http://www3.weforum.org/docs/GCR2017-2018/GCI_Dataset_2007-2017.xlsx"',
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    load_to_hdfs = PythonOperator(
        task_id='load_to_hdfs',
        python_callable=store_to_hdfs,
    )

    clean_up_output = BashOperator(
        task_id='clean_up_output',
        bash_command='rm -f /opt/airflow/dags/output/gci/*',
    )

ingestion >> transform >> load_to_hdfs >> clean_up_output
