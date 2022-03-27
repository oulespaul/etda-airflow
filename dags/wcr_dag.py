import os, sys, getopt
import csv
import json 
import glob
from datetime import timedelta, date, datetime
import filecmp 
import numpy as np
import pandas as pd
import smtplib
import ssl
import pytz
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
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
        self.ingest_date = datetime.today().strftime('%d/%m/%Y %H:%M')

        self._data = {}

        self.__initConfig()
        # self.__initDriver()

    def __del__(self):
        # self.__delDriver()
        return None

    def __initDriver(self):
        return None

    def __delDriver(self):
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
        data = str(data).replace('<BR>', '').replace('\s(nb)', ' ').replace('\s', ' ').replace('\r', '<br />').replace('\r\n', '<br />').replace('\n', '<br />').replace('\t', ' ').replace('&nbsp;', ' ').strip()
        return data

    def comparingFiles(self, year, pillar, unit_2):  
        if pillar == 'pillar': 
            return filecmp.cmp('{}/tmp/raw/{}_{}_{}.csv'.format(self._airflow_path, year, pillar, unit_2), '{}/tmp/raw_check/{}_{}_{}.csv'.format(self._airflow_path, year, pillar, unit_2), shallow=False)
        else:
            return filecmp.cmp('{}/tmp/raw/{}.csv'.format(self._airflow_path, year), '{}/tmp/raw_check/{}.csv'.format(self._airflow_path, year), shallow=False)

    def getDataFromExcel(self, file, pillar, unit_2):   

        yid = self.year['year'].tolist()
        last_row = yid[len(yid)-1] if len(yid) else 0

        run_number = 0

        schema = self._schema
        schema.pop()

        try: 
            sheets_dict = pd.read_excel(open('{}/tmp/raw_check/{}.xlsx'.format(self._airflow_path, file), 'rb'),
                    sheet_name=None)  
        except:
            exit()

        for name, sheet in sheets_dict.items():
            tmp_data = []

            year = name

            #Check Year
            try:
                yid.index(int(year))
                if int(year) < int(last_row):
                    # print(year ,'already')
                    continue

                # continue run_number
                if pillar == 'pillar':
                    f = open("{}/tmp/raw/{}.csv".format(self._airflow_path, year), 'r')
                else:
                    f = open("{}/tmp/raw/{}_{}_{}.csv".format(self._airflow_path, year, pillar, unit_2), 'r')
                run_number = int(f.readlines()[1].split(',')[0]) - 1
                f.close()

                # print(year ,'already', 'check')
            except:
                if len(self.year.index.values) == 0:
                    if year not in self.year['year'].tolist():
                        self.year.loc[1] = [year]
                    # print(year ,'add top line')
                else:
                    if year not in self.year['year'].tolist():
                        self.year.loc[self.year.index.values[len(self.year.index.values)-1]+1] = [year]
                    # print(year ,'add ' + str(len(self.year.index.values)-1) + ' line')

            if pillar == 'pillar':
                for column in sheet:
                    i = 0
                    for val in sheet[column].values:
                        if str(sheet[name].values[i]) != 'nan' and sheet[name].values[i] and str(sheet[name].values[i])[:5] != '© IMD' and str(sheet[name].values[i]) != val:
                            run_number += 1
                            if unit_2 == 'Score':
                                if column == 'Overall (WCY)':
                                    tmp_data.append([run_number, sheet[name].values[i], int(name), self._index, self._source, self._index_name, '', '', '', '', '', '', '', unit_2, float(val) if val and str(val).strip() != '-' else None])
                                else:
                                    tmp_data.append([run_number, sheet[name].values[i], int(name), self._index, self._source, self._index_name, '', column, '', '', '', '', '', unit_2, float(val) if val and str(val).strip() != '-' else None])
                            else:
                                if column == 'Overall (WCY)':
                                    tmp_data.append([run_number, sheet[name].values[i], int(name), self._index, self._source, self._index_name, '', '', '', '', '', '', '', unit_2, int(val) if val and str(val).strip() != '-' else None])
                                else:
                                    tmp_data.append([run_number, sheet[name].values[i], int(name), self._index, self._source, self._index_name, '', column, '', '', '', '', '', unit_2, int(val) if val and str(val).strip() != '-' else None])
                        i += 1
            else:
                pillar_name = {
                    'Domestic Economy (WCY)': 'Domestic Economy (WCY)',
                    'International Trade (WCY)': 'Domestic Economy (WCY)',
                    'International Investment (WCY)': 'Domestic Economy (WCY)',
                    'Employment (WCY)': 'Domestic Economy (WCY)',
                    'Prices (WCY)': 'Domestic Economy (WCY)',
                    'Public Finance (WCY)': 'Government Efficiency (WCY)',
                    'Tax Policy (WCY)': 'Government Efficiency (WCY)',
                    'Institutional Framework (WCY)': 'Government Efficiency (WCY)',
                    'Business Legislation (WCY)': 'Government Efficiency (WCY)',
                    'Societal Framework (WCY)': 'Government Efficiency (WCY)',
                    'Productivity & Efficiency (WCY)': 'Business Efficiency (WCY)',
                    'Labor Market (WCY)': 'Business Efficiency (WCY)',
                    'Finance (WCY)': 'Business Efficiency (WCY)',
                    'Management Practices (WCY)': 'Business Efficiency (WCY)',
                    'Attitudes and Values (WCY)': 'Business Efficiency (WCY)',
                    'Basic Infrastructure (WCY)': 'Infrastructure (WCY)',
                    'Technological Infrastructure (WCY)': 'Infrastructure (WCY)',
                    'Scientific Infrastructure (WCY)': 'Infrastructure (WCY)',
                    'Health and Environment (WCY)': 'Infrastructure (WCY)',
                    'Education (WCY)': 'Infrastructure (WCY)',
                }
                for column in sheet:
                    i = 0
                    for val in sheet[column].values:
                        if str(sheet[name].values[i]) != 'nan' and sheet[name].values[i] and str(sheet[name].values[i])[:5] != '© IMD' and str(sheet[name].values[i]) != val:
                            run_number += 1
                            try:
                                tmp_data.append([run_number, sheet[name].values[i], int(name), self._index, self._source, self._index_name, '', pillar_name[column], column, '', '', '', '', 'Score', float(val) if val and str(val).strip() != '-' else 0.0])
                            except:
                                continue
                                # tmp_data.append([run_number, sheet[name].values[i], int(name), self._index, self._source, self._index_name, '', 'unknown', column, '', '', '', '', 'Score', float(val) if val and str(val).strip() != '-' else 0.0])
                        i += 1

                df = pd.DataFrame(tmp_data, columns=schema)
                df["rank"] = df.groupby('sub_pillar')['value'].rank("dense", ascending=False)

                for index, row in df.iterrows():
                    run_number += 1
                    tmp_data.append([run_number, row['country'], row['year'], row['master_index'], row['organizer'], row['index'], row['sub_index'], row['pillar'], row['sub_pillar'], row['sub_sub_pillar'], row['indicator'], row['sub_indicator'], row['others'], 'Rank', int(row['rank'])])
                del df

                indicator_name = {
                    "Exchange Rate (WCY)"                                    : "Domestic Economy (WCY)",
                    "Gross Domestic Product (GDP) (WCY)"                     : "Domestic Economy (WCY)",
                    "GDP (PPP) (WCY)"                                        : "Domestic Economy (WCY)",
                    "World GDP contribution (WCY)"                           : "Domestic Economy (WCY)",
                    "Household consumption expenditure ($bn) (WCY)"          : "Domestic Economy (WCY)",
                    "Household consumption expenditure (%) (WCY)"            : "Domestic Economy (WCY)",
                    "Government consumption expenditure ($bn) (WCY)"         : "Domestic Economy (WCY)",
                    "Government consumption expenditure (%) (WCY)"           : "Domestic Economy (WCY)",
                    "Gross fixed capital formation (%) (WCY)"                : "Domestic Economy (WCY)",
                    "Gross domestic savings ($bn) (WCY)"                     : "Domestic Economy (WCY)",
                    "Gross domestic savings (%) (WCY)"                       : "Domestic Economy (WCY)",
                    "Economic complexity index (WCY)"                        : "Domestic Economy (WCY)",
                    "Real GDP growth (WCY)"                                  : "Domestic Economy (WCY)",
                    "Real GDP growth per capita (WCY)"                       : "Domestic Economy (WCY)",
                    "Household consumption expenditure - real growth (WCY)"  : "Domestic Economy (WCY)",
                    "Government consumption expenditure - real growth (WCY)" : "Domestic Economy (WCY)",
                    "Gross fixed capital formation - real growth (WCY)"      : "Domestic Economy (WCY)",
                    "Resilience of the economy (WCY)"                        : "Domestic Economy (WCY)",
                    "GDP per capita (WCY)"                                   : "Domestic Economy (WCY)",
                    "Forecast: Real GDP growth (WCY)"                        : "Domestic Economy (WCY)",
                    "Forecast: Inflation (WCY)"                              : "Domestic Economy (WCY)",
                    "Forecast: Unemployment (WCY)"                           : "Domestic Economy (WCY)",
                    "Forecast: Current account balance (WCY)"                : "Domestic Economy (WCY)",
                    "Current account balance ($bn) (WCY)"                    : "International Trade (WCY)",
                    "Current account balance (WCY)"                          : "International Trade (WCY)",
                    "Balance of trade ($bn) (WCY)"                           : "International Trade (WCY)",
                    "Balance of trade (%) (WCY)"                             : "International Trade (WCY)",
                    "Balance of commercial services ($bn) (WCY)"             : "International Trade (WCY)",
                    "Balance of commercial services (%) (WCY)"               : "International Trade (WCY)",
                    "World exports contribution (WCY)"                       : "International Trade (WCY)",
                    "Exports of goods ($bn) (WCY)"                           : "International Trade (WCY)",
                    "Exports of goods (%) (WCY)"                             : "International Trade (WCY)",
                    "Exports of goods per capita (WCY)"                      : "International Trade (WCY)",
                    "Exports of goods - growth (WCY)"                        : "International Trade (WCY)",
                    "Exports of commercial services ($bn) (WCY)"             : "International Trade (WCY)",
                    "Exports of commercial services (%) (WCY)"               : "International Trade (WCY)",
                    "Exports of commercial services - growth (WCY)"          : "International Trade (WCY)",
                    "Exports of goods & commercial services (WCY)"           : "International Trade (WCY)",
                    "Export concentration by partner (WCY)"                  : "International Trade (WCY)",
                    "Export concentration by product (WCY)"                  : "International Trade (WCY)",
                    "Imports of goods & commercial services ($bn) (WCY)"     : "International Trade (WCY)",
                    "Imports of goods & commercial services (%) (WCY)"       : "International Trade (WCY)",
                    "Imports of goods & commercial services - growth (WCY)"  : "International Trade (WCY)",
                    "Trade to GDP ratio (WCY)"                               : "International Trade (WCY)",
                    "Terms of trade index (WCY)"                             : "International Trade (WCY)",
                    "Tourism receipts (WCY)"                                 : "International Trade (WCY)",
                    "Direct investment flows abroad ($bn) (WCY)"             : "International Investment (WCY)",
                    "Direct investment flows abroad (% of GDP) (WCY)"        : "International Investment (WCY)",
                    "Direct investment stocks abroad ($bn) (WCY)"            : "International Investment (WCY)",
                    "Direct investment stocks abroad (% of GDP) (WCY)"       : "International Investment (WCY)",
                    "Direct investment flows inward ($bn) (WCY)"             : "International Investment (WCY)",
                    "Direct investment flows inward (% of GDP) (WCY)"        : "International Investment (WCY)",
                    "Direct investment stocks inward ($bn) (WCY)"            : "International Investment (WCY)",
                    "Direct investment stocks inward (% of GDP) (WCY)"       : "International Investment (WCY)",
                    "Balance of direct investment flows ($bn) (WCY)"         : "International Investment (WCY)",
                    "Balance of direct investment flows (%) (WCY)"           : "International Investment (WCY)",
                    "Net position in direct investment stocks ($bn) (WCY)"   : "International Investment (WCY)",
                    "Net position in direct investment stocks (%) (WCY)"     : "International Investment (WCY)",
                    "Relocation threats of business (WCY)"                   : "International Investment (WCY)",
                    "Portfolio investment assets (WCY)"                      : "International Investment (WCY)",
                    "Portfolio investment liabilities (WCY)"                 : "International Investment (WCY)",
                    "Employment (WCY)"                                       : "Employment (WCY)",
                    "Employment (%) (WCY)"                                   : "Employment (WCY)",
                    "Employment - growth (WCY)"                              : "Employment (WCY)",
                    "Employment - long-term growth (WCY)"                    : "Employment (WCY)",
                    "Employment in the public sector (WCY)"                  : "Employment (WCY)",
                    "Unemployment rate (WCY)"                                : "Employment (WCY)",
                    "Long-term unemployment (WCY)"                           : "Employment (WCY)",
                    "Youth unemployment (WCY)"                               : "Employment (WCY)",
                    "Youth exclusion (WCY)"                                  : "Employment (WCY)",
                    "Consumer price inflation (WCY)"                         : "Prices (WCY)",
                    "Cost-of-living index (WCY)"                             : "Prices (WCY)",
                    "Apartment rent (WCY)"                                   : "Prices (WCY)",
                    "Office rent (WCY)"                                      : "Prices (WCY)",
                    "Food costs (WCY)"                                       : "Prices (WCY)",
                    "Gasoline prices (WCY)"                                  : "Prices (WCY)",
                    "Government budget surplus/deficit ($bn) (WCY)"          : "Public Finance (WCY)",
                    "Government budget surplus/deficit (%) (WCY)"            : "Public Finance (WCY)",
                    "Total general government debt ($bn) (WCY)"              : "Public Finance (WCY)",
                    "Total general government debt (%) (WCY)"                : "Public Finance (WCY)",
                    "Total general government debt-real growth (WCY)"        : "Public Finance (WCY)",
                    "Interest payment (%) (WCY)"                             : "Public Finance (WCY)",
                    "Public finances (WCY)"                                  : "Public Finance (WCY)",
                    "Tax evasion (WCY)"                                      : "Public Finance (WCY)",
                    "Pension funding (WCY)"                                  : "Public Finance (WCY)",
                    "General government expenditure (WCY)"                   : "Public Finance (WCY)",
                    "Collected total tax revenues (WCY)"                     : "Tax Policy (WCY)",
                    "Collected personal income tax (WCY)"                    : "Tax Policy (WCY)",
                    "Collected corporate taxes (WCY)"                        : "Tax Policy (WCY)",
                    "Collected indirect tax revenues (WCY)"                  : "Tax Policy (WCY)",
                    "Collected capital and property taxes (WCY)"             : "Tax Policy (WCY)",
                    "Collected social security contribution (WCY)"           : "Tax Policy (WCY)",
                    "Corporate tax rate on profit (WCY)"                     : "Tax Policy (WCY)",
                    "Consumption tax rate (WCY)"                             : "Tax Policy (WCY)",
                    "Employee social security tax rate (WCY)"                : "Tax Policy (WCY)",
                    "Employer social security tax rate (WCY)"                : "Tax Policy (WCY)",
                    "Real personal taxes (WCY)"                              : "Tax Policy (WCY)",
                    "Real short-term interest rate (WCY)"                    : "Institutional Framework (WCY)",
                    "Cost of capital (WCY)"                                  : "Institutional Framework (WCY)",
                    "Interest rate spread (WCY)"                             : "Institutional Framework (WCY)",
                    "Country credit rating (WCY)"                            : "Institutional Framework (WCY)",
                    "Central bank policy (WCY)"                              : "Institutional Framework (WCY)",
                    "Foreign currency reserves (WCY)"                        : "Institutional Framework (WCY)",
                    "Foreign currency reserves per capita (WCY)"             : "Institutional Framework (WCY)",
                    "Exchange rate stability (WCY)"                          : "Institutional Framework (WCY)",
                    "Legal and regulatory framework (WCY)"                   : "Institutional Framework (WCY)",
                    "Adaptability of government policy (WCY)"                : "Institutional Framework (WCY)",
                    "Transparency (WCY)"                                     : "Institutional Framework (WCY)",
                    "Bureaucracy (WCY)"                                      : "Institutional Framework (WCY)",
                    "Bribery and corruption (WCY)"                           : "Institutional Framework (WCY)",
                    "Rule of law (WCY)"                                      : "Institutional Framework (WCY)",
                    "Tariff barriers (WCY)"                                  : "Business Legislation (WCY)",
                    "Protectionism (WCY)"                                    : "Business Legislation (WCY)",
                    "Public sector contracts (WCY)"                          : "Business Legislation (WCY)",
                    "Foreign investors (WCY)"                                : "Business Legislation (WCY)",
                    "Capital markets (WCY)"                                  : "Business Legislation (WCY)",
                    "Investment incentives (WCY)"                            : "Business Legislation (WCY)",
                    "Government subsidies (WCY)"                             : "Business Legislation (WCY)",
                    "Subsidies (WCY)"                                        : "Business Legislation (WCY)",
                    "State ownership of enterprises (WCY)"                   : "Business Legislation (WCY)",
                    "Competition legislation (WCY)"                          : "Business Legislation (WCY)",
                    "Parallel economy (WCY)"                                 : "Business Legislation (WCY)",
                    "New business density (WCY)"                             : "Business Legislation (WCY)",
                    "Creation of firms (WCY)"                                : "Business Legislation (WCY)",
                    "Start-up days (WCY)"                                    : "Business Legislation (WCY)",
                    "Start-up procedures (WCY)"                              : "Business Legislation (WCY)",
                    "Labor regulations (WCY)"                                : "Business Legislation (WCY)",
                    "Unemployment legislation (WCY)"                         : "Business Legislation (WCY)",
                    "Immigration laws (WCY)"                                 : "Business Legislation (WCY)",
                    "Redundancy costs (WCY)"                                 : "Business Legislation (WCY)",
                    "Justice (WCY)"                                          : "Societal Framework (WCY)",
                    "Homicide (WCY)"                                         : "Societal Framework (WCY)",
                    "Ageing of population (WCY)"                             : "Societal Framework (WCY)",
                    "Risk of political instability (WCY)"                    : "Societal Framework (WCY)",
                    "Social cohesion (WCY)"                                  : "Societal Framework (WCY)",
                    "Gini coefficient (WCY)"                                 : "Societal Framework (WCY)",
                    "Income distribution - lowest 10% (WCY)"                 : "Societal Framework (WCY)",
                    "Income distribution - highest 10% (WCY)"                : "Societal Framework (WCY)",
                    "Income distribution - lowest 40% growth (WCY)"          : "Societal Framework (WCY)",
                    "Equal opportunity (WCY)"                                : "Societal Framework (WCY)",
                    "Forecast: Real GDP growth (WCY)"                        : "Societal Framework (WCY)",
                    "Unemployment rate - gender ratio (WCY)"                 : "Societal Framework (WCY)",
                    "Gender inequality (WCY)"                                : "Societal Framework (WCY)",
                    "Disposable Income (WCY)"                                : "Societal Framework (WCY)",
                    "Overall productivity (PPP) (WCY)"                       : "Productivity & Efficiency (WCY)",
                    "Overall productivity (PPP) - real growth (WCY)"         : "Productivity & Efficiency (WCY)",
                    "Labor productivity (PPP) (WCY)"                         : "Productivity & Efficiency (WCY)",
                    "Agricultural productivity (PPP) (WCY)"                  : "Productivity & Efficiency (WCY)",
                    "Productivity in industry (PPP) (WCY)"                   : "Productivity & Efficiency (WCY)",
                    "Productivity in services (PPP) (WCY)"                   : "Productivity & Efficiency (WCY)",
                    "Workforce productivity (WCY)"                           : "Productivity & Efficiency (WCY)",
                    "Large corporations (WCY)"                               : "Productivity & Efficiency (WCY)",
                    "Small and medium-size enterprises (WCY)"                : "Productivity & Efficiency (WCY)",
                    "Use of digital tools and technologies (WCY)"            : "Productivity & Efficiency (WCY)",
                    "Compensation levels (WCY)"                              : "Labor Market (WCY)",
                    "Unit labor costs for total economy (WCY)"               : "Labor Market (WCY)",
                    "Remuneration in services professions (WCY)"             : "Labor Market (WCY)",
                    "Remuneration of management (WCY)"                       : "Labor Market (WCY)",
                    "Remuneration spread (WCY)"                              : "Labor Market (WCY)",
                    "Working hours (WCY)"                                    : "Labor Market (WCY)",
                    "Worker motivation (WCY)"                                : "Labor Market (WCY)",
                    "Industrial disputes (WCY)"                              : "Labor Market (WCY)",
                    "Apprenticeships (WCY)"                                  : "Labor Market (WCY)",
                    "Employee training (WCY)"                                : "Labor Market (WCY)",
                    "Labor force (WCY)"                                      : "Labor Market (WCY)",
                    "Labor force (%) (WCY)"                                  : "Labor Market (WCY)",
                    "Labor force growth (WCY)"                               : "Labor Market (WCY)",
                    "Labor force long-term growth (WCY)"                     : "Labor Market (WCY)",
                    "Part-time employment (WCY)"                             : "Labor Market (WCY)",
                    "Female labor force (WCY)"                               : "Labor Market (WCY)",
                    "Foreign labor force - migrant stock (WCY)"              : "Labor Market (WCY)",
                    "Skilled labor (WCY)"                                    : "Labor Market (WCY)",
                    "Finance skills (WCY)"                                   : "Labor Market (WCY)",
                    "Attracting and retaining talents (WCY)"                 : "Labor Market (WCY)",
                    "Brain drain (WCY)"                                      : "Labor Market (WCY)",
                    "Foreign highly-skilled personnel (WCY)"                 : "Labor Market (WCY)",
                    "International experience (WCY)"                         : "Labor Market (WCY)",
                    "Competent senior managers (WCY)"                        : "Labor Market (WCY)",
                    "Banking sector assets (WCY)"                            : "Finance (WCY)",
                    "Financial cards in circulation (WCY)"                   : "Finance (WCY)",
                    "Financial card transactions (WCY)"                      : "Finance (WCY)",
                    "Access to financial services (WCY)"                     : "Finance (WCY)",
                    "Access to financial services - gender ratio (WCY)"      : "Finance (WCY)",
                    "Banking and financial services (WCY)"                   : "Finance (WCY)",
                    "Regulatory compliance (banking laws) (WCY)"             : "Finance (WCY)",
                    "Stock markets (WCY)"                                    : "Finance (WCY)",
                    "Stock market capitalization ($bn) (WCY)"                : "Finance (WCY)",
                    "Stock market capitalization (%) (WCY)"                  : "Finance (WCY)",
                    "Value traded on stock markets (WCY)"                    : "Finance (WCY)",
                    "Listed domestic companies (WCY)"                        : "Finance (WCY)",
                    "Stock market index (WCY)"                               : "Finance (WCY)",
                    "Shareholders' rights (WCY)"                             : "Finance (WCY)",
                    "Initial Public Offerings (WCY)"                         : "Finance (WCY)",
                    "Credit (WCY)"                                           : "Finance (WCY)",
                    "Venture capital (WCY)"                                  : "Finance (WCY)",
                    "M&A Activity (WCY)"                                     : "Finance (WCY)",
                    "Corporate debt (WCY)"                                   : "Finance (WCY)",
                    "Agility of companies (WCY)"                             : "Management Practices (WCY)",
                    "Changing market conditions (WCY)"                       : "Management Practices (WCY)",
                    "Opportunities and threats (WCY)"                        : "Management Practices (WCY)",
                    "Credibility of managers (WCY)"                          : "Management Practices (WCY)",
                    "Corporate boards (WCY)"                                 : "Management Practices (WCY)",
                    "Auditing and accounting practices (WCY)"                : "Management Practices (WCY)",
                    "Use of big data and analytics (WCY)"                    : "Management Practices (WCY)",
                    "Customer satisfaction (WCY)"                            : "Management Practices (WCY)",
                    "Entrepreneurship (WCY)"                                 : "Management Practices (WCY)",
                    "Social responsibility (WCY)"                            : "Management Practices (WCY)",
                    "Women in management (WCY)"                              : "Management Practices (WCY)",
                    "Women on boards (WCY)"                                  : "Management Practices (WCY)",
                    "Attitudes toward globalization (WCY)"                   : "Attitudes and Values (WCY)",
                    "Image abroad or branding (WCY)"                         : "Attitudes and Values (WCY)",
                    "National culture (WCY)"                                 : "Attitudes and Values (WCY)",
                    "Flexibility and adaptability (WCY)"                     : "Attitudes and Values (WCY)",
                    "Need for economic and social reforms (WCY)"             : "Attitudes and Values (WCY)",
                    "Digital transformation in companies (WCY)"              : "Attitudes and Values (WCY)",
                    "Value system (WCY)"                                     : "Attitudes and Values (WCY)",
                    "Land area (WCY)"                                        : "Basic Infrastructure (WCY)",
                    "Arable area (WCY)"                                      : "Basic Infrastructure (WCY)",
                    "Water resources (WCY)"                                  : "Basic Infrastructure (WCY)",
                    "Access to water (WCY)"                                  : "Basic Infrastructure (WCY)",
                    "Management of cities (WCY)"                             : "Basic Infrastructure (WCY)",
                    "Population - market size (WCY)"                         : "Basic Infrastructure (WCY)",
                    "Population - growth (WCY)"                              : "Basic Infrastructure (WCY)",
                    "Population under 15 years (WCY)"                        : "Basic Infrastructure (WCY)",
                    "Population over 65 years (WCY)"                         : "Basic Infrastructure (WCY)",
                    "Dependency ratio (WCY)"                                 : "Basic Infrastructure (WCY)",
                    "Roads (WCY)"                                            : "Basic Infrastructure (WCY)",
                    "Railroads (WCY)"                                        : "Basic Infrastructure (WCY)",
                    "Air transportation (WCY)"                               : "Basic Infrastructure (WCY)",
                    "Quality of air transportation (WCY)"                    : "Basic Infrastructure (WCY)",
                    "Distribution infrastructure (WCY)"                      : "Basic Infrastructure (WCY)",
                    "Energy infrastructure (WCY)"                            : "Basic Infrastructure (WCY)",
                    "Total indigenous energy production (WCY)"               : "Basic Infrastructure (WCY)",
                    "Total indigenous energy production (%) (WCY)"           : "Basic Infrastructure (WCY)",
                    "Total final energy consumption (WCY)"                   : "Basic Infrastructure (WCY)",
                    "Total final energy consumption per capita (WCY)"        : "Basic Infrastructure (WCY)",
                    "Electricity costs for industrial clients (WCY)"         : "Basic Infrastructure (WCY)",
                    "Investment in Telecommunications (WCY)"                 : "Technological Infrastructure (WCY)",
                    "Mobile Broadband subscribers (WCY)"                     : "Technological Infrastructure (WCY)",
                    "Mobile Telephone costs (WCY)"                           : "Technological Infrastructure (WCY)",
                    "Communications technology (WCY)"                        : "Technological Infrastructure (WCY)",
                    "Computers in use (WCY)"                                 : "Technological Infrastructure (WCY)",
                    "Computers per capita (WCY)"                             : "Technological Infrastructure (WCY)",
                    "Internet users (WCY)"                                   : "Technological Infrastructure (WCY)",
                    "Broadband subscribers (WCY)"                            : "Technological Infrastructure (WCY)",
                    "Internet bandwidth speed (WCY)"                         : "Technological Infrastructure (WCY)",
                    "Digital/Technological skills (WCY)"                     : "Technological Infrastructure (WCY)",
                    "Qualified engineers (WCY)"                              : "Technological Infrastructure (WCY)",
                    "Public-private partnerships (WCY)"                      : "Technological Infrastructure (WCY)",
                    "Development & application of tech. (WCY)"               : "Technological Infrastructure (WCY)",
                    "Funding for technological development (WCY)"            : "Technological Infrastructure (WCY)",
                    "High-tech exports ($) (WCY)"                            : "Technological Infrastructure (WCY)",
                    "High-tech exports (%) (WCY)"                            : "Technological Infrastructure (WCY)",
                    "ICT service exports (WCY)"                              : "Technological Infrastructure (WCY)",
                    "Cyber security (WCY)"                                   : "Technological Infrastructure (WCY)",
                    "Total expenditure on R&D ($) (WCY)"                     : "Scientific Infrastructure (WCY)",
                    "Total expenditure on R&D (%) (WCY)"                     : "Scientific Infrastructure (WCY)",
                    "Total expenditure on R&D per capita ($) (WCY)"          : "Scientific Infrastructure (WCY)",
                    "Business expenditure on R&D ($) (WCY)"                  : "Scientific Infrastructure (WCY)",
                    "Business expenditure on R&D (%) (WCY)"                  : "Scientific Infrastructure (WCY)",
                    "Total R&D personnel (WCY)"                              : "Scientific Infrastructure (WCY)",
                    "Total R&D personnel per capita (WCY)"                   : "Scientific Infrastructure (WCY)",
                    "Total R&D personnel in business enterprise (WCY)"       : "Scientific Infrastructure (WCY)",
                    "Total R&D personnel in business per capita (WCY)"       : "Scientific Infrastructure (WCY)",
                    "Researchers in R&D per capita (WCY)"                    : "Scientific Infrastructure (WCY)",
                    "Safely treated waste water (WCY)"                       : "Scientific Infrastructure (WCY)",
                    "Scientific articles (WCY)"                              : "Scientific Infrastructure (WCY)",
                    "Nobel prizes (WCY)"                                     : "Scientific Infrastructure (WCY)",
                    "Nobel prizes per capita (WCY)"                          : "Scientific Infrastructure (WCY)",
                    "Patent applications (WCY)"                              : "Scientific Infrastructure (WCY)",
                    "Patent applications per capita (WCY)"                   : "Scientific Infrastructure (WCY)",
                    "Patent grants (WCY)"                                    : "Scientific Infrastructure (WCY)",
                    "Number of patents in force (WCY)"                       : "Scientific Infrastructure (WCY)",
                    "Medium- and high-tech value added (WCY)"                : "Scientific Infrastructure (WCY)",
                    "Scientific research legislation (WCY)"                  : "Scientific Infrastructure (WCY)",
                    "Intellectual property rights (WCY)"                     : "Scientific Infrastructure (WCY)",
                    "Knowledge transfer (WCY)"                               : "Scientific Infrastructure (WCY)",
                    "Total health expenditure (WCY)"                         : "Health and Environment (WCY)",
                    "Total health expenditure per capita (WCY)"              : "Health and Environment (WCY)",
                    "Public expenditure on health (%) (WCY)"                 : "Health and Environment (WCY)",
                    "Health infrastructure (WCY)"                            : "Health and Environment (WCY)",
                    "Universal health care coverage index (WCY)"             : "Health and Environment (WCY)",
                    "Life expectancy at birth (WCY)"                         : "Health and Environment (WCY)",
                    "Healthy life expectancy (WCY)"                          : "Health and Environment (WCY)",
                    "Infant mortality (WCY)"                                 : "Health and Environment (WCY)",
                    "Medical assistance (WCY)"                               : "Health and Environment (WCY)",
                    "Urban population (WCY)"                                 : "Health and Environment (WCY)",
                    "Human development index (WCY)"                          : "Health and Environment (WCY)",
                    "Energy intensity (WCY)"                                 : "Health and Environment (WCY)",
                    "Venture capital (WCY)"                                  : "Health and Environment (WCY)",
                    "Venture capital (WCY)"                                  : "Health and Environment (WCY)",
                    "CO2 emissions (WCY)"                                    : "Health and Environment (WCY)",
                    "CO2 emissions intensity (WCY)"                          : "Health and Environment (WCY)",
                    "Exposure to particle pollution (WCY)"                   : "Health and Environment (WCY)",
                    "Renewable energies (%) (WCY)"                           : "Health and Environment (WCY)",
                    "Forest area growth (WCY)"                               : "Health and Environment (WCY)",
                    "Total biocapacity (WCY)"                                : "Health and Environment (WCY)",
                    "Ecological footprint (WCY)"                             : "Health and Environment (WCY)",
                    "Ecological balance (reserve/deficit) (WCY)"             : "Health and Environment (WCY)",
                    "Environment-related technologies (WCY)"                 : "Health and Environment (WCY)",
                    "Environmental agreements (WCY)"                         : "Health and Environment (WCY)",
                    "Sustainable development (WCY)"                          : "Health and Environment (WCY)",
                    "Pollution problems (WCY)"                               : "Health and Environment (WCY)",
                    "Environmental laws (WCY)"                               : "Health and Environment (WCY)",
                    "Quality of life (WCY)"                                  : "Health and Environment (WCY)",
                    "Total public expenditure on education (WCY)"            : "Education (WCY)",
                    "Total indigenous energy production (%) (WCY)"           : "Education (WCY)",
                    "Total public expenditure on education per capita (WCY)" : "Education (WCY)",
                    "Pupil-teacher ratio (primary education) (WCY)"          : "Education (WCY)",
                    "Pupil-teacher ratio (secondary education) (WCY)"        : "Education (WCY)",
                    "Secondary school enrollment (WCY)"                      : "Education (WCY)",
                    "Higher education achievement (WCY)"                     : "Education (WCY)",
                    "Women with degrees (WCY)"                               : "Education (WCY)",
                    "Student mobility inbound (WCY)"                         : "Education (WCY)",
                    "Student mobility outbound (WCY)"                        : "Education (WCY)",
                    "Educational assessment - PISA (WCY)"                    : "Education (WCY)",
                    "English proficiency - TOEFL (WCY)"                      : "Education (WCY)",
                    "Primary and secondary education (WCY)"                  : "Education (WCY)",
                    "University education (WCY)"                             : "Education (WCY)",
                    "Maintenance and development (WCY)"                      : "Education (WCY)",
                    "University education index (WCY)"                       : "Education (WCY)",
                    "Illiteracy (WCY)"                                       : "Education (WCY)",
                    "Language skills (WCY)"                                  : "Education (WCY)"
                }

                for column in sheet:
                    i = 0
                    for val in sheet[column].values:
                        if str(sheet[name].values[i]) != 'nan' and sheet[name].values[i] and str(sheet[name].values[i])[:5] != '© IMD' and str(sheet[name].values[i]) != val:
                            run_number += 1
                            try:
                                tmp_data.append([run_number, sheet[name].values[i], int(name), self._index, self._source, self._index_name, '', pillar_name[indicator_name[column]], indicator_name[column], '', column, '', '', 'Score', float(val) if val and str(val).strip() != '-' else 0.0])
                            except:
                                continue
                                # tmp_data.append([run_number, sheet[name].values[i], int(name), self._index, self._source, self._index_name, '', 'unknown', column, '', '', '', '', 'Score', float(val) if val and str(val).strip() != '-' else 0.0])
                        i += 1

                df = pd.DataFrame(tmp_data, columns=schema)
                df["rank"] = df.groupby('indicator')['value'].rank("dense", ascending=False)

                for index, row in df.iterrows():
                    run_number += 1
                    tmp_data.append([run_number, row['country'], row['year'], row['master_index'], row['organizer'], row['index'], row['sub_index'], row['pillar'], row['sub_pillar'], row['sub_sub_pillar'], row['indicator'], row['sub_indicator'], row['others'], 'Rank', int(row['rank'])])
                del df

            run_number = self.writeFileCheck(name, tmp_data, run_number, pillar, unit_2)

             # File Check New
            if pillar == 'pillar':
                if os.path.exists('{}/tmp/raw/{}_{}_{}.csv'.format(self._airflow_path, year, pillar, unit_2)):
                    if not self.comparingFiles(year, pillar, unit_2):
                        os.rename('{}/tmp/raw/{}_{}_{}.csv'.format(self._airflow_path, year, pillar, unit_2), '{}/tmp/raw/{}_{}_{}_{}.csv'.format(self._airflow_path, year, pillar, unit_2, str(self.date_scrap)[:10]))
                        os.rename('{}/tmp/raw_check/{}_{}_{}.csv'.format(self._airflow_path, year, pillar, unit_2), '{}/tmp/raw/{}_{}_{}.csv'.format(self._airflow_path, year, pillar, unit_2))

                        # write file
                        line_count = self.writeFile(year, str(self.date_scrap).replace('-', '').replace(' ', '').replace(':', ''), pillar, unit_2)
                        self._log_status = 'update'
                    else:
                        os.remove('{}/tmp/raw_check/{}_{}_{}.csv'.format(self._airflow_path, year, pillar, unit_2))
                        line_count = 0
                        self._log_status = 'duplicate'
                else:
                    os.rename('{}/tmp/raw_check/{}_{}_{}.csv'.format(self._airflow_path, year, pillar, unit_2), '{}/tmp/raw/{}_{}_{}.csv'.format(self._airflow_path, year, pillar, unit_2))

                    # write file
                    line_count = self.writeFile(year, str(self.date_scrap).replace('-', '').replace(' ', '').replace(':', ''), pillar, unit_2)
                    self._log_status = 'new'
            else:
                if os.path.exists('{}/tmp/raw/{}.csv'.format(self._airflow_path, year)):
                    if not self.comparingFiles(year, pillar, unit_2):
                        os.rename('{}/tmp/raw/{}.csv'.format(self._airflow_path, year), '{}/tmp/raw/{}_{}.csv'.format(self._airflow_path, year, str(self.date_scrap)[:10]))
                        os.rename('{}/tmp/raw_check/{}.csv'.format(self._airflow_path, year), '{}/tmp/raw/{}.csv'.format(self._airflow_path, year))

                        # write file
                        line_count = self.writeFile(year, str(self.date_scrap).replace('-', '').replace(' ', '').replace(':', ''), pillar, unit_2)
                        self._log_status = 'update'
                    else:
                        os.remove('{}/tmp/raw_check/{}.csv'.format(self._airflow_path, year))
                        line_count = 0
                        self._log_status = 'duplicate'
                else:
                    os.rename('{}/tmp/raw_check/{}.csv'.format(self._airflow_path, year), '{}/tmp/raw/{}.csv'.format(self._airflow_path, year))

                    # write file
                    line_count = self.writeFile(year, str(self.date_scrap).replace('-', '').replace(' ', '').replace(':', ''), pillar, unit_2)
                    self._log_status = 'new'

            
            self._log_tmp.append([str(self.date_scrap)[:10], self._index, year, line_count, self._log_status])

        # os.rename('{}/tmp/raw_check/{}.xlsx'.format(self._airflow_path, file), '{}/tmp/raw_save/{}.xlsx'.format(self._airflow_path, file))
         
        return None  

    def writeYear(self):
        with open('{}/config/year.tsv'.format(self._airflow_path), 'w') as write_tsv:
            write_tsv.write(self.year.to_csv(sep='\t', index=False))
         
        return None  

    def writeFileCheck(self, year, data, run_number, pillar, unit_2):
        if pillar == 'pillar':
            saveFile = open("{}/tmp/raw_check/{}_{}_{}.csv".format(self._airflow_path, year, pillar, unit_2), 'w', newline='') 
        else:
            saveFile = open("{}/tmp/raw_check/{}.csv".format(self._airflow_path, year), 'w', newline='') 
        saveCSV = csv.writer(saveFile, delimiter=',')

        saveCSV.writerow(self._schema)
        
        for row in data: 
            saveCSV.writerow([row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14]])
            run_number = row[0]
            
        saveFile.close()
        return run_number

    def writeFile(self, year, date, pillar, unit_2):
        if pillar == 'pillar':
            saveFile = open("{}/tmp/data/{}_{}_{}_{}_{}.csv".format(self._airflow_path, self._index, pillar, unit_2, year, date), 'w', newline='') 
        else:
            saveFile = open("{}/tmp/data/{}_{}_{}.csv".format(self._airflow_path, self._index, year, date), 'w', newline='') 
        saveCSV = csv.writer(saveFile, delimiter=',')

        saveCSV.writerow(self._schema)

        if pillar == 'pillar':
            with open("{}/tmp/raw/{}_{}_{}.csv".format(self._airflow_path, year, pillar, unit_2)) as csv_file:

                i = 0
                csv_reader = csv.reader(csv_file, delimiter=',')
                line_count = 0

                for row in csv_reader: 
                    if i == 0 or row[0].strip() == '':
                        i += 1
                        continue
                    
                    saveCSV.writerow([row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], self.ingest_date])

                    line_count += 1
        else:
            with open("{}/tmp/raw/{}.csv".format(self._airflow_path, year)) as csv_file:

                i = 0
                csv_reader = csv.reader(csv_file, delimiter=',')
                line_count = 0

                for row in csv_reader: 
                    if i == 0 or row[0].strip() == '':
                        i += 1
                        continue
                    
                    saveCSV.writerow([row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], self.ingest_date])

                    line_count += 1

        saveFile.close()
        if pillar == 'pillar':
            self._file_upload.append("{}/tmp/data/{}_{}_{}_{}_{}.csv".format(self._airflow_path, self._index, year, pillar, unit_2, date))
        else:
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

dag = DAG('WCR', default_args=default_args, catchup=False)

def extract_transform():
    cls = WCR()
    cls.getDataFromExcel("WCR 1995-2021", "", "")
    cls.getDataFromExcel("WCR pillar 1995-2021 rank", "pillar", "Rank")
    cls.getDataFromExcel("WCR pillar 1995-2021 score", "pillar", "Score")

    cls.writeYear()

    try:
        f = open("{}/tmp/log/{}_log_{}.tsv".format(cls._airflow_path, cls._index, datetime.today().strftime('%Y')))
        log_file = open("{}/tmp/log/{}_log_{}.tsv".format(cls._airflow_path, cls._index, datetime.today().strftime('%Y')), 'a', newline='')
        log_writer = csv.writer(log_file, delimiter='\t', lineterminator='\n', quotechar = "'")
    except IOError:
        log_file = open("{}/tmp/log/{}_log_{}.tsv".format(cls._airflow_path, cls._index, datetime.today().strftime('%Y')), 'a')
        log_writer = csv.writer(log_file, delimiter='\t', lineterminator='\n', quotechar = "'")
        log_writer.writerow(cls._header_log)

    for data_log in cls._log_tmp:
        log_writer.writerow(data_log)

    log_file.close()

    pprint("Scrap_data_source and Extract_transform!")

def store_to_hdfs(**kwargs):
    hdfs = PyWebHdfsClient(host='10.121.101.130',
                           port='50070', user_name='hdfs')
    my_dir = kwargs['directory']
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    path = "/opt/airflow/dags/data_source/wcr/tmp/data"

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
    index_name = "World Competitiveness Ranking (WCR)"
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


with dag:
    scrap_and_extract_transform = PythonOperator(
        task_id='scrap_and_extract_transform',
        python_callable=extract_transform,
    )

    load_to_hdfs_raw_zone = PythonOperator(
        task_id='load_to_hdfs_raw_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/raw/index_dashboard/Global/WCR'},
    )

    load_to_hdfs_processed_zone = PythonOperator(
        task_id='load_to_hdfs_processed_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/processed/index_dashboard/Global/WCR'},
    )

    clean_up_output = BashOperator(
        task_id='clean_up_output',
        bash_command='rm -f /opt/airflow/dags/data_source/wcr/tmp/data/* && rm -f /opt/airflow/dags/data_source/wcr/tmp/raw/*',
    )

    send_email = PythonOperator(
        task_id='send_email',
        python_callable=send_mail,
    )

scrap_and_extract_transform >> load_to_hdfs_raw_zone >> load_to_hdfs_processed_zone >> clean_up_output >> send_email