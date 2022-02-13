from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint
import pandas as pd
import os
import tabula
import json


def transform():
    pd.set_option('display.max_columns', None)
    datasource_path = "/opt/airflow/dags/data_source/b2c"
    output_path = "/opt/airflow/dags/output/b2c"

    def ingestion_n_transform(year, source_file_name, page_scrap_list, col_drop, col_rename, series_dict):
        file_path = "{}/{}".format(datasource_path, source_file_name)

        tables = tabula.read_pdf(
            file_path, guess=False, pages=page_scrap_list, silent=True)
        df = pd.concat(tables)

        df.drop(col_drop, axis=1, inplace=True)
        df.rename(columns=col_rename, inplace=True)

        # Add col standard format
        i = 3
        df = pd.concat([df.iloc[:, :i],
                        pd.DataFrame('',
                                     columns=['sub_index', 'pillar', 'sub_pillar', 'sub_sub_pillar', 'indicator',
                                              'sub_indicator', 'others', 'unit_2'],
                                     index=df.index), df.iloc[:, i:]],
                       axis=1)

        df = df.melt(
            id_vars=['country', 'sub_index', 'pillar', 'sub_pillar',
                     'sub_sub_pillar', 'indicator', 'sub_indicator', 'others', 'unit_2'],
            var_name="Indicator",
            value_name="value"
        )

        def get_value_from_key(key, sub_key):
            return series_dict.get(key, {}).get(sub_key, "")

        df["pillar"] = df["Indicator"].apply(
            get_value_from_key, args=("pillar",))
        df["indicator"] = df["Indicator"].apply(
            get_value_from_key, args=("indicator",))
        df["unit_2"] = df["Indicator"].apply(
            get_value_from_key, args=("unit_2",))

        ingest_date = datetime.now()
        df['master_index'] = 'IDI'
        df['index'] = "UNCTAD B2C E-commerce Index "
        df['organizer'] = 'UNCTAD'
        df['date_etl'] = ingest_date.strftime("%Y-%m-%d %H:%M:%S")
        df['year'] = year

        col = ["country", "year", "master_index", "organizer", "index", "sub_index", "pillar", "sub_pillar",
               "sub_sub_pillar",
               "indicator", "sub_indicator", "others", "unit_2", "value", "date_etl"]

        df = df[col]
        df = df.sort_values(by=['country', 'year', 'indicator'])

        df.to_csv('{}/B2C_{}_{}.csv'.format(output_path, year,
                  ingest_date.strftime("%Y%m%d%H%M%S")), index=False)

    def ingestion_init():
        # Get config
        json_file = open('{}/config.json'.format(datasource_path))
        conf_main = json.load(json_file)
        json_file.close()

        for config in conf_main:
            ingestion_n_transform(
                year=config['year'],
                source_file_name=config['source_file_name'],
                page_scrap_list=config['page_scrap_list'],
                col_drop=config['col_drop'],
                col_rename=config['col_rename'],
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

dag = DAG('b2c', default_args=default_args, catchup=False)


def store_to_hdfs():
    hdfs = PyWebHdfsClient(host='10.121.101.130',
                           port='50070', user_name='hdfs')
    my_dir = '/raw/index_dashboard/Global/B2C'
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    path = "/opt/airflow/dags/output/b2c"

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
        bash_command='cd /opt/airflow/dags/data_source/b2c && ./sources.sh ',
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
        bash_command='rm -f /opt/airflow/dags/output/b2c/*',
    )

ingestion >> transform >> load_to_hdfs >> clean_up_output
