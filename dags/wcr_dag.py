#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import csv
import json
import glob
from datetime import timedelta, datetime
import filecmp
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint


class WCR():

    def __init__(self):
        self._firefox_driver_path = "/usr/local/bin"
        self._airflow_path = "/opt/airflow/dags/data_source/wcr"

        self._index = "WCR"
        self._index_name = "World Competitiveness Ranking"
        self._source = "IMD"
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

        self.date_scrap = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

        self._data = {}

        self.__initConfig()

    def __del__(self):
        return None

    def __initDriver(self):
        options = Options()
        options.set_preference("browser.download.folderList", 2)
        options.set_preference(
            "browser.download.manager.showWhenStarting", False)
        options.set_preference("browser.download.dir",
                               '{}/tmp/raw_check'.format(self._airflow_path))
        options.set_preference(
            "browser.helperApps.neverAsk.saveToDisk", "application/octet-stream")
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
        year_current = int(datetime.today().strftime('%Y'))

        # Loop Create Dict Year
        for year in range(int(self._year_start), int(year_current)+1):
            self._data[year] = []

        self.__initDriver()

        self.driver.get(self._url_link)
        wait = WebDriverWait(self.driver, 5)

        sections = wait.until(EC.presence_of_element_located(
            (By.XPATH, '/html/body/div[2]/section[5]/div[2]/section')))
        for section in sections.find_elements_by_xpath('/html/body/div[2]/section[5]/div[2]/section/div'):
            # section
            # print(section.find_element_by_xpath('.//div/a/div[1]/div').text)
            section_txt = section.find_element_by_xpath(
                './/div/a/div[1]/div').text
            for tr in section.find_elements_by_xpath('.//div/div/div/div/table/tbody/tr'):
                for td in tr.find_elements_by_xpath('.//td'):
                    year = td.find_element_by_xpath(
                        './/div[1]').get_attribute('innerHTML')
                    value = td.find_element_by_xpath(
                        './/div[2]').get_attribute('innerHTML'),
                    if year == 'Country':
                        country = value[0]
                        continue
                    self._data[int(year)].append([country, int(year), self._index, self._source, self._index_name, '', section_txt, '', '', '', '', '', 'rank', int(value[0].replace(
                        '<strong>', '').replace('</strong>', '').replace('-', '')) if value[0].replace('<strong>', '').replace('</strong>', '').replace('-', '') else None])

        self.__delDriver()

        run_number = 0

        # Loop Check Data
        yid = self.year['year'].tolist()
        for i, year in enumerate(self._data):
            # Check Year
            try:
                yid.index(year)
                if year != year_current:
                    # print(year ,'already')
                    self._data[year] = []
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

            # write file
            run_number = self.writeFileCheck(
                year, self._data[year], run_number)

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

        with open('{}/config/year.tsv'.format(self._airflow_path), 'w') as write_tsv:
            write_tsv.write(self.year.to_csv(sep='\t', index=False))

        return None

    def writeFileCheck(self, year, data, run_number):
        saveFile = open(
            "{}/tmp/raw_check/{}.csv".format(self._airflow_path, year), 'w', newline='')
        saveCSV = csv.writer(saveFile, delimiter=',')

        saveCSV.writerow(self._schema)

        for row in data:
            run_number += 1
            saveCSV.writerow([run_number, row[0].replace(',', ''), row[1], row[2], row[3], row[4], row[5], row[6].replace(
                ',', ''), row[7], row[8], row[9], row[10], row[11], row[12], row[13]])

        saveFile.close()

        return run_number

    def writeFile(self, year, date):
        saveFile = open(
            "{}/tmp/data/{}_{}.csv".format(self._airflow_path, year, date), 'w', newline='')
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

dag = DAG('WCR', default_args=default_args, catchup=False)


def extract_transform():
    cls = WCR()
    cls.getDataFromWeb()

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


def store_to_hdfs():
    my_dir = '/raw/index_dashboard/Global/WCR'
    hdfs = PyWebHdfsClient(host='10.121.101.130',
                           port='50070', user_name='hdfs')
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    for file in glob.glob('/opt/airflow/dags/data_source/wcr/tmp/data/*.csv'):
        with open(file, 'r', encoding="utf8") as file_data:
            my_data = file_data.read()
            hdfs.create_file(
                f"{my_dir}/" + file.split('/')[-1], my_data.encode('utf-8'), overwrite=True)
        os.remove(file)

    pprint("Stored!")


with dag:
    scrap_and_extract_transform = PythonOperator(
        task_id='scrap_and_extract_transform',
        python_callable=extract_transform,
    )

    load_to_hdfs = PythonOperator(
        task_id='load_to_hdfs',
        python_callable=store_to_hdfs,
    )

scrap_and_extract_transform >> load_to_hdfs
