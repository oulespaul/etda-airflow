from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from pywebhdfs.webhdfs import PyWebHdfsClient
from datetime import datetime, timedelta
from pprint import pprint
from PyPDF2 import PdfFileReader
from airflow.models import Variable
import os
import json
import pandas as pd
import numpy as np
import smtplib
import ssl
import pytz


def transform():
    datasource_path = "/opt/airflow/dags/data_source/gsi"
    output_path = "/opt/airflow/dags/output/gsi"

    def create_security_table(pdf_file, country_file, meta_data_file, page_start, page_end, year):
        # creating a pdf file object
        pdfFileObj = open("{}/{}".format(datasource_path, pdf_file), 'rb')
        # show dataframe about country
        country_df = pd.read_csv("{}/{}".format(datasource_path, country_file))
        # creating a pdf reader object
        pdfReader = PdfFileReader(pdfFileObj)
        # show official column dataframe
        gsi_index_meta_data = pd.read_excel("{}/{}".format(
            datasource_path, meta_data_file),
            sheet_name='Global Cyber_Metadata',
            header=1,
            engine="openpyxl")
        all_num_lst = []
        for i in range(page_start - 1, page_end):
            pageObj = pdfReader.getPage(i)
            pageTxt = pageObj.extractText()
            pageTxt = pageTxt.replace('\n', '')
            Txtlst = pageTxt.split(' ')
            s = 'Source:'
            lst = []
            for i in range(len(Txtlst)):
                if (Txtlst[i] == s):
                    lst.append(i)
            lst1 = []
            lst2 = []
            count = 0
            for i in lst:
                for j in range(i - 6, i):
                    count += 1
                    if count <= 6:
                        lst1.append(Txtlst[j])
                    else:
                        lst2.append(Txtlst[j])
            all_num_lst.append(lst1)
            all_num_lst.append(lst2)
        number_df = pd.DataFrame(all_num_lst)
        number_df = number_df.dropna()
        number_df = pd.DataFrame(all_num_lst)
        number_df = number_df.dropna()
        early_gsi_df = pd.concat(
            [country_df, number_df], axis=1, ignore_index=True)
        early_gsi_df = early_gsi_df.rename(columns={0: 'Country', 1: 'Index score', 2: 'Legal Measures', 3: 'Technical Measures',
                                                    4: 'Organizational Measures', 5: 'Capacity Building', 6: 'Cooperation'})
        early_gsi_df = early_gsi_df.melt(
            id_vars=['Country'], var_name="index_group", value_name="value")
        gsi_df = early_gsi_df.merge(
            gsi_index_meta_data, how='left', left_on='index_group', right_on='Name')
        # add column of year is 2020
        ingest_date = datetime.now()
        gsi_df['year'] = year
        gsi_df['value'] = gsi_df['value'].astype(float)
        gsi_df["value"] = np.where(gsi_df["pillar"].isna(
        ), gsi_df['value'] / 100, gsi_df['value'] / 20)
        drop_col = ['index_group', 'Code', 'Type',
                    'Name', 'Unit', 'Definition', 'Source']
        gsi_df = gsi_df.drop(drop_col, axis=1)
        gsi_df.columns = gsi_df.columns.str.lower()
        gsi_df['index'] = 'Global Cybersecurity Index'
        gsi_df['master_index'] = 'Global Cybersecurity Index'
        gsi_df['organizer'] = 'ITU'
        gsi_df['unit_2'] = 'Score'
        gsi_df['ingest_date'] = ingest_date.strftime("%Y-%m-%d %H:%M:%S")

        gsi_df.to_csv('{}/GSI_{}_{}.csv'.format(output_path, year,
                                                ingest_date.strftime("%Y%m%d%H%M%S")), index=False)

    def ingestion_init():
        # Get config
        json_file = open('{}/config.json'.format(datasource_path))
        conf_main = json.load(json_file)
        json_file.close()

        for config in conf_main:
            create_security_table(
                pdf_file=config['pdf_file'],
                country_file=config['country_file'],
                meta_data_file=config['meta_data_file'],
                page_start=config['page_start'],
                page_end=config['page_end'],
                year=config['year'],
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

dag = DAG('GSI', default_args=default_args, catchup=False)


def ingest_data():
    hdfs = PyWebHdfsClient(host='10.121.101.130',
                           port='50070', user_name='hdfs')
    source_file_byte = '/raw/index_dashboard/File_Upload/GSI/Global Cybersecurity Index 2020.pdf'

    data_source = hdfs.read_file(source_file_byte)

    with open('/opt/airflow/dags/data_source/gsi/Global Cybersecurity Index 2020.pdf', 'wb') as file_out:
        file_out.write(data_source)
        file_out.close()

    pprint("Ingested!")

def send_mail():
    index_name = "Global Cybersecurity Index (GCI)"
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


def store_to_hdfs(**kwargs):
    hdfs = PyWebHdfsClient(host='10.121.101.130',
                           port='50070', user_name='hdfs')
    my_dir = kwargs['directory']
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    path = "/opt/airflow/dags/output/gsi"

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
        op_kwargs={'directory': '/raw/index_dashboard/Global/GSI'},
    )

    load_to_hdfs_processed_zone = PythonOperator(
        task_id='load_to_hdfs_processed_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/processed/index_dashboard/Global/GSI'},
    )

    clean_up_output = BashOperator(
        task_id='clean_up_output',
        bash_command='rm -f /opt/airflow/dags/output/gsi/*',
    )

    send_email = PythonOperator(
        task_id='send_email',
        python_callable=send_mail,
    )

ingestion >> transform >> load_to_hdfs_raw_zone >> load_to_hdfs_processed_zone  >> clean_up_output >> send_email