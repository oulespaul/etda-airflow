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

    df1 = pd.read_excel(
        f'{data_source_dir}/FixedTelephoneSubscriptions_2000-2020.xlsx', sheet_name='i112', engine="openpyxl")
    df2 = pd.read_excel(
        f'{data_source_dir}/MobileCellularSubscriptions_2000-2020.xlsx', sheet_name='i271', engine="openpyxl")
    df3 = pd.read_excel(
        f'{data_source_dir}/MobileBroadbandSubscriptions_2007-2020.xlsx', sheet_name='i271mw', engine="openpyxl")
    df4 = pd.read_excel(
        f'{data_source_dir}/InternationalBandwidthInMbits_2007-2020.xlsx', sheet_name='i4214', engine="openpyxl")
    df5 = pd.read_excel(
        f'{data_source_dir}/FixedBroadbandSubscriptions_2000-2020.xlsx', sheet_name='i4213tfbb', engine="openpyxl")
    df6 = pd.read_excel(
        f'{data_source_dir}/PercentIndividualsUsingInternet.xlsx', sheet_name='Data', engine="openpyxl")

    df = [df1, df2, df3, df4, df5, df6]
    df = pd.concat(df)

    i = 3
    df = pd.concat([df.iloc[:, :i],
                    pd.DataFrame('',
                                 columns=['sub_index', 'pillar', 'sub_pillar', 'sub_sub_pillar', 'indicator',
                                          'sub_indicator', 'others'],
                                 index=df.index), df.iloc[:, i:]],
                   axis=1)

    df.drop(df.columns[df.columns.str.contains(
        '_notes$')], axis=1, inplace=True)
    df.drop(df.columns[df.columns.str.contains(
        '_source$')], axis=1, inplace=True)

    series_dict = {
        "Fixed-telephone subscriptions": {
            "pillar": "ICT Access",
            "indicator": "Fixed-telephone subscriptions per 100 inhabitants"
        },
        "Mobile-cellular telephone subscriptions;  by postpaid/prepaid": {
            "pillar": "ICT Access",
            "indicator": "Mobile-cellular subscriptions per 100 inhabitants"
        },
        "Active mobile-broadband subscriptions": {
            "pillar": "ICT Use",
            "indicator": "Active mobile-broadband subscriptions per 100 inhabitants"
        },
        "International bandwidth;  in Mbit/s": {
            "pillar": "ICT Access",
            "indicator": "International bandwidth (bit/s) per Internet user"
        },
        "Fixed-broadband subscriptions": {
            "pillar": "ICT Use",
            "indicator": "Fixed-broadband subscriptions per 100 inhabitants"
        },
        "Internet users (%)": {
            "pillar": "ICT Access",
            "indicator": "Internet users (%)"
        }
    }

    def get_value_from_key(key, sub_key):
        return series_dict.get(key, {}).get(sub_key, "")

    df["pillar"] = df["Indicator"].apply(get_value_from_key, args=("pillar",))
    df["indicator"] = df["Indicator"].apply(
        get_value_from_key, args=("indicator",))

    df = df.melt(
        id_vars=['Indicator', 'Country', 'sub_index', 'pillar', 'sub_pillar',
                 'sub_sub_pillar', 'indicator', 'sub_indicator', 'others'],
        var_name="unit_2",
        value_name="value"
    )

    ingest_date = datetime.now()

    df.rename(columns={'Country': 'country'}, inplace=True)
    df['index'] = "ICT Development Index"
    df['master_index'] = 'IDI'
    df['organizer'] = 'ITU'
    unit_split = df['unit_2'].str.split('_', 1)
    df['year'] = unit_split.str[0]
    df['unit_2'] = unit_split.str[1]
    df['date_etl'] = ingest_date.strftime("%Y-%m-%d %H:%M:%S")

    col = ["country", "year", "master_index", "organizer", "index", "sub_index", "pillar", "sub_pillar", "sub_sub_pillar",
           "indicator", "sub_indicator", "others", "unit_2", "value", "date_etl"]

    df = df[col]
    df = df.sort_values(by=['country', 'year', 'indicator'])

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
