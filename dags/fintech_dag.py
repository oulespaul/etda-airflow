from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import tabula as tb
import pandas as pd
import smtplib
import ssl
import pytz
import os

def transform():
    datasource_path = "/opt/airflow/dags/data_source/fintech"
    output_path = "/opt/airflow/dags/output/fintech"

    def get_fintech_table(pdf_file,meta_data_file,page,year) :
        file_path = "{}/{}".format(datasource_path, pdf_file)
        df = tb.read_pdf('{}'.format(file_path),pages= '{}'.format(page))[0]
        df = df.dropna()
        df = df.rename(columns={'P1. Regulatory':'Regulatory and Policy Environment','P2.':'Infrastructure'\
                                ,'P3. Demand':'Demand','P4. Innovative':'Innovative Products and Services'})
        df = df.melt(id_vars=['Unnamed: 0','Unnamed: 1'],var_name="index", value_name="value")
        df = df.drop(['Unnamed: 0'],axis=1).rename(columns={'Unnamed: 1':'country'})
        new = df["value"].str.split(" ", n = 1, expand = True) 
        df["Rank"]= new[0]   
        df["Score"]= new[1]   
        # Dropping old Name columns 
        df.drop(columns =["value"], inplace = True) 
        fin_df = df.melt(id_vars= ['country','index'],var_name="unit_2", value_name="value")
        fin_df['year'] = year
        fintech_meta_data = pd.read_excel('{}/{}'.format(datasource_path, meta_data_file),sheet_name='Fintech_Metadata',header=1, engine="openpyxl")
        fin_df = fin_df.merge(fintech_meta_data,how='inner',left_on='index',right_on='Name')
        drop_col = ['index','Series ID','Name','Series units','Series type','Definition','Source']
        fin_df = fin_df.drop(drop_col,axis=1)
        fin_df['master_index'] = 'Fintech'
        fin_df['organizer'] = 'WFE'
        ingest_date = datetime.now()
        fin_df['ingest_date'] = ingest_date.strftime("%Y-%m-%d %H:%M:%S")
        fin_df.columns = fin_df.columns.str.lower()
        df.to_csv('{}/FINTECH_{}_{}.csv'.format(output_path, year,
                                            ingest_date.strftime("%Y%m%d%H%M%S")), index=False)

    def ingestion_init():
        # Get config
        json_file = open('{}/config.json'.format(datasource_path))
        conf_main = json.load(json_file)
        json_file.close()
            
        for config in conf_main:
            get_fintech_table(
                pdf_file =config['pdf_file'],
                meta_data_file=config['meta_data_file'],
                page = config['page'],
                year =config['year'],
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

dag = DAG('fintech', default_args=default_args, catchup=False)

def ingest_data():
    hdfs = PyWebHdfsClient(host='10.121.101.130',
                           port='50070', user_name='hdfs')
    source_file_byte = '/raw/index_dashboard/File_Upload/APEC_Fintech_E-payment_Readiness_Index_2016.pdf'

    data_source = hdfs.read_file(source_file_byte)

    with open('/opt/airflow/dags/data_source/fintech/APEC_Fintech_E-payment_Readiness_Index_2016.pdf', 'wb') as file_out:
        file_out.write(data_source)
        file_out.close()

    pprint("Ingested!")

def send_mail():
    index_name = "Fintech e-Payment Readiness Index (FINTECH)"
    smtp_server = "smtp.gmail.com"
    port = 587
    email_to = Variable.get("email_to")
    email_from = Variable.get("email_from")
    password = Variable.get("email_from_password")
    tzInfo = pytz.timezone('Asia/Bangkok')

    email_string = f"""
    Pipeline Success
    ------------------------------------------
    Index: {index_name}
    Ingestion Date: {datetime.now(tz=tzInfo).strftime("%Y/%m/%d %H:%M")}
    """

    context = ssl.create_default_context()
    try:
        server = smtplib.SMTP(smtp_server, port)
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(email_from, password)
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

    path = "/opt/airflow/dags/output/fintech"

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
        op_kwargs={'directory': '/raw/index_dashboard/Global/FINTECH'},
    )

    load_to_hdfs_processed_zone = PythonOperator(
        task_id='load_to_hdfs_processed_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/processed/index_dashboard/Global/FINTECH'},
    )

    clean_up_output = BashOperator(
        task_id='clean_up_output',
        bash_command='rm -f /opt/airflow/dags/output/fintech/*.csv',
    )

    send_email = PythonOperator(
        task_id='send_email',
        python_callable=send_mail,
    )

ingestion >> transform >> load_to_hdfs_raw_zone >> load_to_hdfs_processed_zone >> clean_up_output >> send_email