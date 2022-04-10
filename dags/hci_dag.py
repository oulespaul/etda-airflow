#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import sys
import getopt
import requests
import csv
import json
import re
from time import sleep
from datetime import timedelta, date, datetime
import zipfile
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
from datetime import datetime, timedelta
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint
from airflow.models import Variable


class HCI():

    def __init__(self):        
        self._firefox_driver_path = "/usr/local/bin"
        self._airflow_path = "/opt/airflow/dags/data_source/hci"

        self._index = "HCI"
        self._index_name = "Human Capital Index"
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

        self.date_scrap = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        self.ingest_date = datetime.today().strftime('%d/%m/%Y %H:%M')

        self.__initConfig()
        # self.__initDriver()

    def __del__(self):
        # self.__delDriver()
        return None

    def __initDriver(self):
        options = Options()
        options.set_preference("browser.download.folderList", 2)
        options.set_preference("browser.download.manager.showWhenStarting", False)
        options.set_preference("browser.download.dir", '{}/tmp/raw_check'.format(self._airflow_path))
        options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/octet-stream")
        options.headless = True
        self.driver = webdriver.Firefox(executable_path='{}/geckodriver'.format(self._firefox_driver_path), service_log_path=os.devnull, options=options)
        self.driver.wait = WebDriverWait(self.driver, 5)
        return None

    def __delDriver(self):
        self.driver.close()
        return None

    def __initConfig(self):
        json_file = open( '{}/config/config.json'.format(self._airflow_path) )
        conf_main = json.load(json_file)
        json_file.close()
        
        self._url_link = conf_main['url_link']
        self._year_start = conf_main['year_start']
        self._schema = conf_main['schema']
        self._header_log = conf_main['header_log']

        self.year = pd.read_csv('{}/config/year.tsv'.format(self._airflow_path), sep='\t')

        del conf_main

        return None

    def convertString(self, data):
        data = str(data).replace('<BR>', '').replace('\s(nb)', ' ').replace('\s', ' ').replace('\r', '<br />').replace('\r\n', '<br />').replace('\n', '<br />').replace('\t', ' ').replace('&nbsp;', ' ').replace('<br />', '').strip()
        return data

    def comparingFiles(self, year):   
        return filecmp.cmp('{}/tmp/raw/{}.csv'.format(self._airflow_path, year), '{}/tmp/raw_check/{}.csv'.format(self._airflow_path, year), shallow=False)

    def downloadCSVFromWeb(self):   
        year_current = int(datetime.today().strftime('%Y')) - 1
        yid = self.year['year'].tolist()

        run_number = 0

        for year in range(int(self._year_start), int(datetime.today().strftime('%Y'))):

            #Check Year
            try:
                yid.index(year)
                if year != year_current:
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
                    self.year.loc[self.year.index.values[len(self.year.index.values)-1]+1] = [year]
                    # print(year ,'add ' + str(len(self.year.index.values)-1) + ' line')

            self.__initDriver()

            self.driver.get( self._url_link )
            wait = WebDriverWait(self.driver, 5)

            while True:
                try:
                    countryAll = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="newSelection_HCI_Country"]/div/div/div/div/div[1]/div[3]/div[1]/div[1]/div/a[1]')))  
                    break
                except TimeoutException:
                    sleep(1)
                    pass
            countryAll.click()
            sleep(5)

            while True:
                try:
                    series = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="panel_HCI_Series"]/div[1]/h4/a')))
                    series.click()
                    sleep(5)
                    break
                except TimeoutException:
                    sleep(5)
 
            while True:
                try:
                    seriesAll = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="newSelection_HCI_Series"]/div/div/div/div/div[1]/div[3]/div[1]/div[1]/div/a[1]')))
                    seriesAll.click()
                    sleep(5)
                    break
                except TimeoutException:
                    sleep(5)

            while True:
                try:
                    time = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="panel_HCI_Time"]/div[1]/h4/a')))
                    time.click()
                    sleep(5)
                    break
                except TimeoutException:
                    sleep(5)

            while True:
                try:
                    unSelectYear = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="rowTimeDim"]/div/div/div[2]/div[3]/div[1]/div[1]/div/a[2]')))
                    unSelectYear.click()
                    sleep(5)
                    break
                except TimeoutException:
                    sleep(5)
                
            try:
                selectYear = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="chk[HCI_Time].[List].&[YR' + str(year) + ']"]')))
                selectYear.click()
            except TimeoutException:
                continue
            sleep(5)
            while True:
                try:
                    apply = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="applyChangesNoPreview"]')))
                    break
                except TimeoutException:
                    sleep(1)
                    pass
            apply.click()
            sleep(5)

            while True:
                try:
                    checkDataTable = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="grdTableView_DXMainTable"]')))
                    break
                except TimeoutException:
                    sleep(1)
                    pass
            sleep(5)

            showDownload = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="upReportLinks"]/div/ul/li[5]/a')))
            showDownload.click()
            sleep(5)
            download = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="liCSVDownload"]/a')))
            download.click()
            sleep(30)

            # File Check
            while True:
                try:
                    if os.path.exists('{}/tmp/raw_check/Data_Extract_From_Human_Capital_Index.zip'.format(self._airflow_path)):
                        sleep(5)
                        break
                except TimeoutException:
                    sleep(1)
                    pass
            
            # File Unzip
            with zipfile.ZipFile('{}/tmp/raw_check/Data_Extract_From_Human_Capital_Index.zip'.format(self._airflow_path), 'r') as zip_ref:
                zip_ref.extractall('{}/tmp/raw_check'.format(self._airflow_path))
            os.remove('{}/tmp/raw_check/Data_Extract_From_Human_Capital_Index.zip'.format(self._airflow_path))
            sleep(5)

            # File Delete, Rename
            for filename in os.listdir('{}/tmp/raw_check/'.format(self._airflow_path)):
                if filename.endswith("Metadata.csv"):
                    os.remove('{}/tmp/raw_check/{}'.format(self._airflow_path, filename))
                elif filename.endswith("Data.csv"):
                    os.rename('{}/tmp/raw_check/{}'.format(self._airflow_path, filename), '{}/tmp/raw_check/{}.csv'.format(self._airflow_path, year))
            
            # File Check New
            if os.path.exists('{}/tmp/raw/{}.csv'.format(self._airflow_path, year)):

                # add run_number
                raw_tmp = []
                with open("{}/tmp/raw/{}.csv".format(self._airflow_path, year)) as csv_file:
                    i = 0
                    csv_reader = csv.reader(csv_file, delimiter=',', skipinitialspace=True)
                    for row in csv_reader: 
                        if i == 0 or row[0].strip() == '' or row[0].strip()[:19] == 'Data from database:' or row[0].strip()[:13] == 'Last Updated:':
                            if i == 0:
                                col = ['id', 'Series Name', 'Series Code', 'Country Name', 'Country Code', 'value', 'unit_2']
                            i += 1
                            continue

                        run_number += 1
                        raw_tmp.append([run_number, row[0], row[1], row[2], row[3], float(row[4]) if str(row[4]) != '..' else 0.0, 'Score'])
                df = pd.DataFrame(raw_tmp, columns=col)
                
                df["Rank"] = df.groupby(col[1])['value'].rank("dense", ascending=False)

                for index, row in df.iterrows():
                    run_number += 1
                    raw_tmp.append([run_number, row[1], row[2], row[3], row[4], int(row['Rank']), 'Rank'])
                del df

                saveFile = open("{}/tmp/raw/{}.csv".format(self._airflow_path, year), 'w', newline='') 
                saveCSV = csv.writer(saveFile, delimiter=',')
                for row in raw_tmp: 
                    saveCSV.writerow(row)
                saveFile.close()

                # comparingFiles
                if not self.comparingFiles(year):
                    os.rename('{}/tmp/raw/{}.csv'.format(self._airflow_path, year), '{}/tmp/raw/{}_{}.csv'.format(self._airflow_path, year, str(self.date_scrap)[:10]))
                    os.rename('{}/tmp/raw_check/{}.csv'.format(self._airflow_path, year), '{}/tmp/raw/{}.csv'.format(self._airflow_path, year))

                    # write file
                    line_count = self.writeFile(year, str(self.date_scrap).replace('-', '').replace(' ', '').replace(':', ''))
                    self._log_status = 'update'
                else:
                    os.remove('{}/tmp/raw_check/{}.csv'.format(self._airflow_path, year))
                    line_count = 0
                    self._log_status = 'duplicate'
            else:
                os.rename('{}/tmp/raw_check/{}.csv'.format(self._airflow_path, year), '{}/tmp/raw/{}.csv'.format(self._airflow_path, year))

                # add run_number
                raw_tmp = []
                with open("{}/tmp/raw/{}.csv".format(self._airflow_path, year)) as csv_file:
                    i = 0
                    csv_reader = csv.reader(csv_file, delimiter=',', skipinitialspace=True)
                    for row in csv_reader: 
                        if i == 0 or row[0].strip() == '' or row[0].strip()[:19] == 'Data from database:' or row[0].strip()[:13] == 'Last Updated:':
                            if i == 0:
                                col = ['id', 'Series Name', 'Series Code', 'Country Name', 'Country Code', 'value', 'unit_2']
                            i += 1
                            continue

                        run_number += 1
                        raw_tmp.append([run_number, row[0], row[1], row[2], row[3], round(float(row[4]), 2) if str(row[4]) != '..' else 0.0, 'Score'])

                df = pd.DataFrame(raw_tmp, columns=col)
                df["Rank"] = df.groupby(col[1])['value'].rank("dense", ascending=False)

                for index, row in df.iterrows():
                    run_number += 1
                    raw_tmp.append([run_number, row[1], row[2], row[3], row[4], int(row['Rank']), 'Rank'])
                del df

                saveFile = open("{}/tmp/raw/{}.csv".format(self._airflow_path, year), 'w', newline='') 
                saveCSV = csv.writer(saveFile, delimiter=',')
                for row in raw_tmp: 
                    saveCSV.writerow(row)
                saveFile.close()

                # write file
                line_count = self.writeFile(year, str(self.date_scrap).replace('-', '').replace(' ', '').replace(':', ''))
                self._log_status = 'new'
            
            self._log_tmp.append([str(self.date_scrap)[:10], self._index, year, line_count, self._log_status])
            # print(self._log_tmp)
            self.__delDriver()

        with open('{}/config/year.tsv'.format(self._airflow_path), 'w') as write_tsv:
            write_tsv.write(self.year.to_csv(sep='\t', index=False))
         
        return None

    def writeFile(self, year, date):    

        saveFile = open("{}/tmp/data/{}_{}_{}.csv".format(self._airflow_path, self._index, year, date), 'w', newline='') 
        saveCSV = csv.writer(saveFile, delimiter=',')

        saveCSV.writerow(self._schema)

        with open("{}/tmp/raw/{}.csv".format(self._airflow_path, year)) as csv_file:

            i = 0
            csv_reader = csv.reader(csv_file, delimiter=',', skipinitialspace=True)
            line_count = 0

            for row in csv_reader: 
                if row[0].strip() == '' or row[0].strip()[:19] == 'Data from database:' or row[0].strip()[:13] == 'Last Updated:':
                    i += 1
                    continue
                if row[1] == 'Human Capital Index (HCI) (scale 0-1)':
                    saveCSV.writerow([row[0], row[3], year, self._index, self._source, self._index_name, '', '', '', '', '', '', '', row[6], row[5] if row[5] != '..' else '', self.ingest_date])
                else:
                    saveCSV.writerow([row[0], row[3], year, self._index, self._source, self._index_name, '', '', '', '', row[1], '', '', row[6], row[5] if row[5] != '..' else '', self.ingest_date])
                line_count += 1

        saveFile.close()

        self._file_upload.append("{}/tmp/data/{}_{}_{}.csv".format(self._airflow_path, self._index, year, date))

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

dag = DAG('HCI', default_args=default_args, catchup=False)


def extract_transform():
    cls = HCI()
    cls.downloadCSVFromWeb()

    try:
        f = open("{}/tmp/log/{}_log_{}.tsv".format(cls._airflow_path,
                 cls._index, datetime.today().strftime('%Y')))
        log_file = open("{}/tmp/log/{}_log_{}.tsv".format(cls._airflow_path,
                        cls._index, datetime.today().strftime('%Y')), 'a', newline='')
        log_writer = csv.writer(log_file, delimiter='\t',
                                lineterminator='\n', quotechar="'")
    except IOError:
        log_file = open("{}/tmp/log/{}_log_{}.tsv".format(cls._airflow_path,
                        cls._index, datetime.today().strftime('%Y')), 'a')
        log_writer = csv.writer(log_file, delimiter='\t',
                                lineterminator='\n', quotechar="'")
        log_writer.writerow(cls._header_log)

    for data_log in cls._log_tmp:
        log_writer.writerow(data_log)

    log_file.close()

    pprint("Scrap_data_source and Extract_transform!")


def send_mail():
    index_name = "Human Capital Index (HCI)"
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
    hdfs = PyWebHdfsClient(host='10.121.101.101',
                           port='50070', user_name='hdfs')
    my_dir = kwargs['directory']
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    path = "/opt/airflow/dags/data_source/hci/tmp/data"

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
        op_kwargs={'directory': '/raw/index_dashboard/Global/HCI'},
    )

    load_to_hdfs_processed_zone = PythonOperator(
        task_id='load_to_hdfs_processed_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/processed/index_dashboard/Global/HCI'},
    )

    clean_up_output = BashOperator(
        task_id='clean_up_output',
        bash_command='rm -f /opt/airflow/dags/data_source/hci/tmp/data/* && rm -f /opt/airflow/dags/data_source/hci/tmp/raw/*',
    )

    send_email = PythonOperator(
        task_id='send_email',
        python_callable=send_mail,
    )

scrap_and_extract_transform >> load_to_hdfs_raw_zone >> load_to_hdfs_processed_zone >> clean_up_output >> send_email
