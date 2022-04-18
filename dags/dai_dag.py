#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import requests
import csv
import json
from time import sleep
import filecmp
import pandas as pd
import math
import smtplib
import ssl
import pytz

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint
from airflow.models import Variable

tzInfo = pytz.timezone('Asia/Bangkok')
class DAI():

    def __init__(self):
        self._firefox_driver_path = "/usr/local/bin"
        self._airflow_path = "/opt/airflow/dags/data_source/dai"

        self._index = "DAI"
        self._index_name = "Digital Adoption Index"
        self._source = "World Bank"
        self._lts = []
        self.driver = None
        self.year = None

        self._url_link = None
        self._year_start = None
        self._schema = None
        self._header_log = None

        self._file_upload = []
        self._log_tmp = []

        self.date_scrap = datetime.now(tz=tzInfo).strftime('%Y-%m-%d %H:%M:%S')

        self.__initConfig()

    def __del__(self):
        return None

    def __initConfig(self):
        json_file = open('{}/config/config.json'.format(self._airflow_path))
        conf_main = json.load(json_file)
        json_file.close()

        self._url_link = conf_main['url_link']
        self._year_start = conf_main['year_start']
        self._schema = conf_main['schema']
        self._header_log = conf_main['header_log']

        self.year = pd.read_csv(
            '{}/config/year.tsv'.format(self._airflow_path), sep='\t')

        del conf_main

        return None

    def convertString(self, data):
        data = str(data).replace('<BR>', '').replace('\s(nb)', ' ').replace('\s', ' ').replace('\r', '<br />').replace(
            '\r\n', '<br />').replace('\n', '<br />').replace('\t', ' ').replace('&nbsp;', ' ').replace('<br />', '').strip()
        return data

    def comparingFiles(self, year):
        return filecmp.cmp('{}/tmp/raw/{}.csv'.format(self._airflow_path, year), '{}/tmp/raw_check/{}.csv'.format(self._airflow_path, year), shallow=False)

    def downloadCSVFromWeb(self):
        resp = requests.get(self._url_link)
        output = open(
            '{}/tmp/raw_check/DAIforweb.xlsx'.format(self._airflow_path), 'wb')
        output.write(resp.content)
        output.close()

        yid = self.year['year'].tolist()

        run_number = 0

        for year in self._year_start:

            # Check Year
            try:
                yid.index(year)
            except:
                if len(self.year.index.values) == 0:
                    self.year.loc[1] = [year]
                    # print(year ,'add top line')
                else:
                    self.year.loc[self.year.index.values[len(
                        self.year.index.values)-1]+1] = [year]
                    # print(year ,'add ' + str(len(self.year.index.values)-1) + ' line')

            tmp_data = []

            df = pd.read_excel(
                '{}/tmp/raw_check/DAIforweb.xlsx'.format(self._airflow_path), engine="openpyxl")
            for index, row in df.iterrows():
                if int(year) != int(row['Year']):
                    continue
                run_number += 1
                tmp_data.append([run_number, row['country'], int(year), self._index, self._source, self._index_name, '', '',
                                '', '', '', '', '', 'score', float(row['Digital Adoption Index']) if row['Digital Adoption Index'] else None])
                run_number += 1
                tmp_data.append([run_number, row['country'], int(year), self._index, self._source, self._index_name, 'DAI Business Sub-index', '',
                                '', '', '', '', '', 'score', float(row['DAI Business Sub-index']) if not math.isnan(row['DAI Business Sub-index']) else None])
                run_number += 1
                tmp_data.append([run_number, row['country'], int(year), self._index, self._source, self._index_name, 'DAI People Sub-index',
                                '', '', '', '', '', '', 'score', float(row['DAI People Sub-index']) if not math.isnan(row['DAI People Sub-index']) else None])
                run_number += 1
                tmp_data.append([run_number, row['country'], int(year), self._index, self._source, self._index_name, 'DAI Government Sub-index', '',
                                '', '', '', '', '', 'score', float(row['DAI Government Sub-index']) if not math.isnan(row['DAI Government Sub-index']) else None])

            self.writeFileCheck(year, tmp_data)

            # File Check New
            if os.path.exists('{}/tmp/raw/{}.csv'.format(self._airflow_path, year)):
                if not self.comparingFiles(year):
                    os.rename('{}/tmp/raw/{}.csv'.format(self._airflow_path, year),
                              '{}/tmp/raw/{}_{}.csv'.format(self._airflow_path, year, str(self.date_scrap)[:10]))
                    os.rename('{}/tmp/raw_check/{}.csv'.format(self._airflow_path,
                              year), '{}/tmp/raw/{}.csv'.format(self._airflow_path, year))

                    # write file
                    line_count = self.writeFile(year, str(self.date_scrap).replace(
                        '-', '').replace(' ', '').replace(':', ''))
                    self._log_status = 'update'
                else:
                    os.remove(
                        '{}/tmp/raw_check/{}.csv'.format(self._airflow_path, year))
                    line_count = 0
                    self._log_status = 'duplicate'
            else:
                os.rename('{}/tmp/raw_check/{}.csv'.format(self._airflow_path,
                          year), '{}/tmp/raw/{}.csv'.format(self._airflow_path, year))

                # write file
                line_count = self.writeFile(year, str(self.date_scrap).replace(
                    '-', '').replace(' ', '').replace(':', ''))
                self._log_status = 'new'

            self._log_tmp.append(
                [str(self.date_scrap)[:10], self._index, year, line_count, self._log_status])

        # File Delete, Rename
        for filename in os.listdir('{}/tmp/raw_check/'.format(self._airflow_path)):
            if filename.endswith("DAIforweb.xlsx"):
                os.remove(
                    '{}/tmp/raw_check/{}'.format(self._airflow_path, filename))

        with open('{}/config/year.tsv'.format(self._airflow_path), 'w') as write_tsv:
            write_tsv.write(self.year.to_csv(sep='\t', index=False))

        return None

    def writeFileCheck(self, year, data):
        saveFile = open(
            "{}/tmp/raw_check/{}.csv".format(self._airflow_path, year), 'w', newline='')
        saveCSV = csv.writer(saveFile, delimiter=',')

        saveCSV.writerow(self._schema)

        line_count = 0
        for row in data:
            saveCSV.writerow(row)
            line_count += 1

        saveFile.close()

        return line_count

    def writeFile(self, year, date):
        saveFile = open("{}/tmp/data/{}_{}_{}.csv".format(self._airflow_path,
                        self._index, year, date), 'w', newline='')
        saveCSV = csv.writer(saveFile, delimiter=',')

        saveCSV.writerow(self._schema)

        with open("{}/tmp/raw/{}.csv".format(self._airflow_path, year)) as csv_file:

            i = 0
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0

            for row in csv_reader:
                if i == 0 or row[0].strip() == '':
                    i += 1
                    continue

                saveCSV.writerow([row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7],
                                 row[8], row[9], row[10], row[11], row[12], row[13], row[14], self.date_scrap])

                line_count += 1

        saveFile.close()

        self._file_upload.append(
            "{}/tmp/data/{}_{}.csv".format(self._airflow_path, year, date))

        return line_count


default_args = {
    'owner': 'ETDA',
    'depends_on_past': False,
    'start_date': '2021-01-25',
    'email': ['brian2devops@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@yearly',
}

dag = DAG('DAI', default_args=default_args, catchup=False)


def extract_transform():
    cls = DAI()
    cls.downloadCSVFromWeb()

    try:
        f = open("{}/tmp/log/{}_log_{}.tsv".format(cls._airflow_path,
                 cls._index, datetime.now(tz=tzInfo).strftime('%Y')))
        log_file = open("{}/tmp/log/{}_log_{}.tsv".format(cls._airflow_path,
                        cls._index, datetime.now(tz=tzInfo).strftime('%Y')), 'a', newline='')
        log_writer = csv.writer(log_file, delimiter='\t',
                                lineterminator='\n', quotechar="'")
    except IOError:
        log_file = open("{}/tmp/log/{}_log_{}.tsv".format(cls._airflow_path,
                        cls._index, datetime.now(tz=tzInfo).strftime('%Y')), 'a')
        log_writer = csv.writer(log_file, delimiter='\t',
                                lineterminator='\n', quotechar="'")
        log_writer.writerow(cls._header_log)

    for data_log in cls._log_tmp:
        log_writer.writerow(data_log)

    log_file.close()

    pprint("Scrap_data_source and Extract_transform!")


def send_mail():
    index_name = "Digital Adoption Index (DAI)"
    smtp_server = "10.101.111.12"
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
    hdfs = PyWebHdfsClient(host='vm002namenode.aml.etda.local',
                           port='50070', user_name='hdfs')
    my_dir = kwargs['directory']
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    path = "/opt/airflow/dags/data_source/dai/tmp/data"

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
    scrap_and_extract_transform = PythonOperator(
        task_id='scrap_and_extract_transform',
        python_callable=extract_transform,
    )

    load_to_hdfs_raw_zone = PythonOperator(
        task_id='load_to_hdfs_raw_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/raw/index_dashboard/Global/DAI'},
    )

    load_to_hdfs_processed_zone = PythonOperator(
        task_id='load_to_hdfs_processed_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/processed/index_dashboard/Global/DAI'},
    )

    clean_up_output = BashOperator(
        task_id='clean_up_output',
        bash_command='rm -f /opt/airflow/dags/data_source/dai/tmp/data/* && rm -f /opt/airflow/dags/data_source/dai/tmp/raw/*',
    )

    send_email = PythonOperator(
        task_id='send_email',
        python_callable=send_mail,
    )

scrap_and_extract_transform >> load_to_hdfs_raw_zone >> load_to_hdfs_processed_zone >> clean_up_output >>  send_email
