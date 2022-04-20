#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import sys
import getopt
import csv
import json
from pathlib import Path
from time import sleep
import filecmp
import pandas as pd
import smtplib
import ssl
import pytz
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.firefox.options import Options

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint

tzInfo = pytz.timezone('Asia/Bangkok')

class EGDI():

    def __init__(self):
        self._firefox_driver_path = "/usr/local/bin"
        self._airflow_path = "/opt/airflow/dags/data_source/egdi"

        self._index = "EGDI"
        self._index_name = "E-Government Development Index"
        self._source = "UN"
        self._lts = []
        self.driver = None
        self.year = None

        self._url_link = None
        self._year_start = None
        self._schema = None
        self._header_log = None

        self._url_list = []

        self._file_upload = []
        self._log_tmp = []

        self.date_scrap = datetime.now(tz=tzInfo).strftime('%Y-%m-%d %H:%M:%S')
        self.ingest_date = datetime.now(tz=tzInfo).strftime('%d/%m/%Y %H:%M')

        self.__initConfig()
        # self.__initDriver()

    def __del__(self):
        # self.__delDriver()
        return None

    def __initDriver(self):
        options = Options()
        options.set_preference("browser.download.folderList", 2)
        options.set_preference(
            "browser.download.manager.showWhenStarting", False)
        options.set_preference("browser.download.dir",
                               '{}/tmp/raw_check'.format(self._airflow_path))
        options.set_preference("browser.helperApps.neverAsk.openFile", "text/csv,application/x msexcel,application/excel," +
                               "application/x-excel,application/vnd.ms-excel," + "image/png,image/jpeg,text/html,text/plain," + "application/msword,application/xml")
        options.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv,application/x-msexcel," + "application/excel," +
                               "application/x-excel," + "application/vnd.ms- excel,image/png,image/jpeg,text/html," + "text/plain,application/msword,application/xml")
        options.set_preference("pdfjs.disabled", True)
        options.headless = True
        self.driver = webdriver.Firefox(executable_path='{}/geckodriver'.format(
            self._firefox_driver_path), service_log_path=os.devnull, options=options)
        self.driver.wait = WebDriverWait(self.driver, 5)
        return None

    def __delDriver(self):
        self.driver.close()
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

    def getDataFromWeb(self):

        self.__initDriver()

        self.driver.get(self._url_link)
        wait = WebDriverWait(self.driver, 5)

        body = wait.until(EC.presence_of_element_located(
            (By.XPATH, '/html/body')))

        data_select = body.find_element_by_xpath(
            '/html/body/form/div[3]/div/section[2]/div/div[2]/div/div/div/div[1]/div/div/div/div[1]/div[2]/div/button')
        data_select.click()
        sleep(1)
        data_select = body.find_element_by_xpath('/html/body/div[2]/ul/li[1]')
        data_select.click()
        sleep(1)

        yid = self.year['year'].tolist()
        last_row = yid[len(yid)-1] if len(yid) else 0

        run_number = 0

        count_option = len(body.find_elements_by_xpath(
            '/html/body/form/div[3]/div/section[2]/div/div[2]/div/div/div/div[1]/div/div/div/div[1]/div[1]/div/select/option')) + 1
        for option in body.find_elements_by_xpath('/html/body/form/div[3]/div/section[2]/div/div[2]/div/div/div/div[1]/div/div/div/div[1]/div[1]/div/select/option'):

            body = wait.until(EC.presence_of_element_located(
                (By.XPATH, '/html/body')))
            count_option -= 1

            year = body.find_element_by_xpath(
                '/html/body/form/div[3]/div/section[2]/div/div[2]/div/div/div/div[1]/div/div/div/div[1]/div[1]/div/select/option[' + str(count_option) + ']').get_attribute('value')
            # Check Year
            try:
                yid.index(int(year))
                if int(year) < int(last_row):
                    # print(year ,'already')
                    continue

                # continue run_number
                f = open("{}/tmp/raw/{}.csv".format(self._airflow_path, year), 'r')
                run_number = int(f.readlines()[1].split(',')[0]) - 1
                f.close()

                # print(year ,'already', 'check')
            except:
                if len(self.year.index.values) == 0:
                    self.year.loc[1] = [year]
                    # print(year ,'add top line')
                else:
                    self.year.loc[self.year.index.values[len(
                        self.year.index.values)-1]+1] = [year]
                    # print(year ,'add ' + str(len(self.year.index.values)-1) + ' line')

            year_select = body.find_element_by_xpath(
                '/html/body/form/div[3]/div/section[2]/div/div[2]/div/div/div/div[1]/div/div/div/div[1]/div[1]/div/button')
            year_select.click()
            sleep(1)
            year_select = body.find_element_by_xpath(
                '/html/body/div[1]/ul/li[' + str(count_option) + ']')
            year_select.click()
            sleep(1)

            update = wait.until(EC.element_to_be_clickable(
                (By.XPATH, '/html/body/form/div[3]/div/section[2]/div/div[2]/div/div/div/div[1]/div/div/div/div[1]/div[7]/div/input')))
            update.click()
            sleep(1)

            download = wait.until(EC.element_to_be_clickable(
                (By.XPATH, '/html/body/form/div[3]/div/section[2]/div/div[2]/div/div/div/div[1]/div/div/div/div[3]/div[1]/div/div[2]/a')))
            download.click()
            sleep(1)

            # File Check
            while True:
                try:
                    list = os.listdir(
                        '{}/tmp/raw_check'.format(self._airflow_path))
                    if len(list):
                        # write file
                        run_number = self.writeFileCheck(
                            year, '{}/tmp/raw_check/{}'.format(self._airflow_path, list[0]), run_number)
                        sleep(1)

                        # File Delete
                        os.remove(
                            '{}/tmp/raw_check/{}'.format(self._airflow_path, list[0]))
                        sleep(5)
                        break
                except TimeoutException:
                    sleep(1)
                    pass

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

        self.__delDriver()

        with open('{}/config/year.tsv'.format(self._airflow_path), 'w') as write_tsv:
            write_tsv.write(self.year.to_csv(sep='\t', index=False))

        return None

    def writeFileCheck(self, year, file_name, run_number):
        tmp_data = []
        with open(file_name) as csv_file:
            i = 0
            csv_reader = csv.reader(csv_file, delimiter=',')

            for row in csv_reader:
                if i == 0 or row[0].strip() == '':
                    i += 1
                    continue
                run_number += 1
                tmp_data.append([run_number, row[1].strip(), int(row[0]), self._index, self._source,
                                self._index_name, '', '', '', '', '', '', '', 'Score', float(row[3]) if row[3] else 0.0, ''])
                run_number += 1
                tmp_data.append([run_number, row[1].strip(), int(row[0]), self._index, self._source, self._index_name,
                                '', 'Online Service Index', '', '', '', '', '', 'Score', float(row[5]) if row[5] else 0.0, ''])
                run_number += 1
                tmp_data.append([run_number, row[1].strip(), int(row[0]), self._index, self._source, self._index_name,
                                '', 'Human Capital Index', '', '', '', '', '', 'Score', float(row[6]) if row[6] else 0.0, ''])
                run_number += 1
                tmp_data.append([run_number, row[1].strip(), int(row[0]), self._index, self._source, self._index_name, '',
                                'Telecommunication Infrastructure Index', '', '', '', '', '', 'Score', float(row[7]) if row[7] else 0.0, ''])

        df = pd.DataFrame(tmp_data, columns=self._schema, dtype=float)
        df["rank"] = df.groupby('pillar')['value'].rank(
            "dense", ascending=False)

        for index, row in df.iterrows():
            run_number += 1
            tmp_data.append([run_number, row['country'], row['year'], row['master_index'], row['organizer'], row['index'], row['sub_index'], row['pillar'],
                            row['sub_pillar'], row['sub_sub_pillar'], row['indicator'], row['sub_indicator'], row['others'], 'Rank', int(row['rank'])])
        del df

        saveFile = open(
            "{}/tmp/raw_check/{}.csv".format(self._airflow_path, year), 'w', newline='')
        saveCSV = csv.writer(saveFile, delimiter=',')

        saveCSV.writerow(self._schema)

        for data in tmp_data:
            saveCSV.writerow(data)

        saveFile.close()

        return run_number

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
                                 row[8], row[9], row[10], row[11], row[12], row[13], row[14], self.ingest_date])

                line_count += 1

        saveFile.close()

        self._file_upload.append(
            "{}/tmp/data/{}_{}_{}.csv".format(self._airflow_path, self._index, year, date))

        return line_count

default_args = {
    'owner': 'ETDA',
    'start_date': datetime(2022, 3, 31),
    'schedule_interval': None,
}

dag = DAG('EGDI',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)

def extract_transform():
    cls = EGDI()
    cls.getDataFromWeb()

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


def store_to_hdfs(**kwargs):
    hdfs = PyWebHdfsClient(host='vm002namenode.aml.etda.local',
                           port='50070', user_name='hdfs')
    my_dir = kwargs['directory']
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    path = "/opt/airflow/dags/data_source/egdi/tmp/data"

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


def send_mail():
    index_name = "e-Government Development Index (EGDI)"
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


with dag:
    scrap_and_extract_transform = PythonOperator(
        task_id='scrap_and_extract_transform',
        python_callable=extract_transform,
    )

    load_to_hdfs_raw_zone = PythonOperator(
        task_id='load_to_hdfs_raw_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/raw/index_dashboard/Global/EGDI'},
    )

    load_to_hdfs_processed_zone = PythonOperator(
        task_id='load_to_hdfs_processed_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/processed/index_dashboard/Global/EGDI'},
    )

    clean_up_output = BashOperator(
        task_id='clean_up_output',
        bash_command='rm -f /opt/airflow/dags/data_source/gear/tmp/data/* && rm -f /opt/airflow/dags/data_source/gear/tmp/raw/*',
    )

    send_email = PythonOperator(
        task_id='send_email',
        python_callable=send_mail,
    )

scrap_and_extract_transform >> load_to_hdfs_raw_zone >> load_to_hdfs_processed_zone >> clean_up_output >> send_email
