from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint
from decimal import Decimal
from airflow.models import Variable
import pandas as pd
import os
import json
import smtplib
import ssl
import pytz

def transform():
    datasource_path = "/opt/airflow/dags/data_source/idi"
    output_path = "/opt/airflow/dags/output/idi"

    def ingestion_n_transform(source_file_name, series_dict):
        df = pd.read_excel(
            f'{datasource_path}/{source_file_name}', sheet_name='IDI', skiprows=1, engine="openpyxl")

        i = 3
        df = pd.concat([df.iloc[:, :i],
                        pd.DataFrame('',
                                     columns=['sub_index', 'pillar', 'sub_pillar', 'sub_sub_pillar', 'indicator',
                                              'sub_indicator', 'others'],
                                     index=df.index), df.iloc[:, i:]],
                       axis=1)

        def get_value_from_key(key, sub_key):
            return series_dict.get(key, {}).get(sub_key, "")

        df["index"] = df["Index Name"].apply(
            get_value_from_key, args=("index",))
        df["pillar"] = df["Index Name"].apply(
            get_value_from_key, args=("pillar",))
        df["indicator"] = df["Index Name"].apply(
            get_value_from_key, args=("indicator",))
        df["others"] = df["Index Name"].apply(
            get_value_from_key, args=("others",))

        df.drop(['Type', 'Index Name', 'Unit 1'], axis=1, inplace=True)
        df.rename(columns={'Index': 'master_index', 'ผู้จัดทำ': 'organizer',
                           'Year': 'year', 'Unit 2': 'unit_2'}, inplace=True)

        df = df.melt(
            id_vars=['master_index', 'organizer', 'year', 'sub_index', 'pillar', 'sub_pillar',
                     'sub_sub_pillar', 'indicator', 'sub_indicator', 'others', 'unit_2'],
            var_name="country",
            value_name="value"
        )

        def is_number(value):
            try:
                Decimal(value)
                return True
            except Exception:
                return False

        ingest_date = datetime.now()

        df['index'] = "ICT Development Index"
        df['ingest_date'] = ingest_date.strftime("%Y/%m/%d %H:%M")
        df['unit_2'].replace(['Score', 'Value'], [
                             'Rank', 'Score'], inplace=True)

        # Filter value
        df = df[df['value'].apply(is_number)]
        df = df[~df['value'].isnull()]

        col = ["country", "year", "master_index", "organizer", "index", "sub_index", "pillar", "sub_pillar", "sub_sub_pillar",
               "indicator", "sub_indicator", "others", "unit_2", "value", "ingest_date"]

        df = df[col]
        df = df.sort_values(by=['country', 'year', 'pillar', 'indicator'])

        year_list = df['year'].unique()

        for year in year_list:
            final = df[df['year'] == year].copy()
            current_year = str(year)[0:4]
            final['year'] = current_year

            final.to_csv('{}/IDI_{}_{}.csv'.format(
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

dag = DAG('idi', default_args=default_args, catchup=False)

def send_mail():
    index_name = "ICT Development Index (IDI)"
    smtp_server = "203.154.120.150"
    port = 25
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

def ingest_data():
    hdfs = PyWebHdfsClient(host='10.121.101.130',
                           port='50070', user_name='hdfs')
    source_file_byte = '/raw/index_dashboard/File_Upload/Global Ranking Dashboard.xlsx'

    data_source = hdfs.read_file(source_file_byte)

    with open('/opt/airflow/dags/data_source/idi/Global Ranking Dashboard.xlsx', 'wb') as file_out:
        file_out.write(data_source)
        file_out.close()

    pprint("Ingested!")

def store_to_hdfs(**kwargs):
    hdfs = PyWebHdfsClient(host='10.121.101.130',
                           port='50070', user_name='hdfs')
    my_dir = kwargs['directory']
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    path = "/opt/airflow/dags/output/idi"

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
    ingestion = PythonOperator(
        task_id='ingestion',
        python_callable=ingest_data,
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    load_to_hdfs_raw_zone = PythonOperator(
        task_id='load_to_hdfs_raw_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/raw/index_dashboard/Global/IDI'},
    )

    load_to_hdfs_processed_zone = PythonOperator(
        task_id='load_to_hdfs_processed_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/processed/index_dashboard/Global/IDI'},
    )

    clean_up_output = BashOperator(
        task_id='clean_up_output',
        bash_command='rm -f /opt/airflow/dags/output/idi/*',
    )

    send_email = PythonOperator(
        task_id='send_email',
        python_callable=send_mail,
    )

ingestion >> transform >> load_to_hdfs_raw_zone >> load_to_hdfs_processed_zone >> clean_up_output >> send_email
