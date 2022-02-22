from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint
import pandas as pd
import os


def transform():
    data_source_dir = '/opt/airflow/dags/data_source/idi'

    df = pd.read_excel(
        f'{data_source_dir}/Global Ranking Dashboard.xlsx', sheet_name='IDI', skiprows=1)

    i = 3
    df = pd.concat([df.iloc[:, :i],
                    pd.DataFrame('',
                                 columns=['sub_index', 'pillar', 'sub_pillar', 'sub_sub_pillar', 'indicator',
                                          'sub_indicator', 'others'],
                                 index=df.index), df.iloc[:, i:]],
                   axis=1)

    def get_value_from_key(key, sub_key):
        return series_dict.get(key, {}).get(sub_key, "")

    df["index"] = df["Index Name"].apply(get_value_from_key, args=("index",))
    df["pillar"] = df["Index Name"].apply(get_value_from_key, args=("pillar",))
    df["indicator"] = df["Index Name"].apply(
        get_value_from_key, args=("indicator",))
    df["others"] = df["Index Name"].apply(get_value_from_key, args=("others",))

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
    df['unit_2'].replace(['Score', 'Value'], ['Rank', 'Score'], inplace=True)

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

        final.to_csv('/opt/airflow/dags/output/idi/IDI_{}_{}.csv'.format(
            current_year, ingest_date.strftime("%Y%m%d%H%M%S")), index=False)


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


def store_to_hdfs():
    hdfs = PyWebHdfsClient(host='10.121.101.130',
                           port='50070', user_name='hdfs')
    my_dir = '/raw/index_dashboard/Global/IDI'
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
    ingestion = BashOperator(
        task_id='ingestion',
        bash_command='cd /opt/airflow/dags/data_source/idi && ./sources.sh ',
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
        bash_command='rm -f /opt/airflow/dags/output/idi/*',
    )

ingestion >> transform >> load_to_hdfs >> clean_up_output
