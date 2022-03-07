from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pprint import pprint
from airflow.models import Variable
from pywebhdfs.webhdfs import PyWebHdfsClient
import numpy as np
import pandas as pd
import smtplib
import ssl
import pytz
import os

def transform():
    datasource_path = "/opt/airflow/dags/data_source/gear"
    output_path = "/opt/airflow/dags/output/gear"

    start_page = 55
    end_page = 127
    nation_page = 54

    # Get National ----------------------------------------------
    def listLineNation():
        lines = []
        results = []
        with open('{}/text-extract/nation.txt'.format(datasource_path)) as f:
            lines = f.readlines()

        count = 0
        for line in lines:
            if (line.rstrip() != ''):
                count += 1
                print(f'line {count}: {line}')
                results.append(line)
        return results

    def splitData(results):
        count = 0
        result2 = []
        for line in results:
            ll = line.rstrip().replace('Hong Kong, China', 'HongKongChina  ').replace('United Arab Emirates',
                                                                                      'UnitedArabEmirates ').replace('   ',
                                                                                                                     '=').replace(
                '  ', '=').replace(' ', '').replace('====', '=').replace('===', '=').replace('==', '=')
            print(ll)
            if (count != 0):
                result2.append(ll.split('='))
            count += 1
        return result2

    def tranformRow(result2):
        result3 = []
        count = 0
        for line in result2:
            xx = []
            #         print(line[0])
            if (count <= 22):
                result3.append([line[0], int(line[1])])
                result3.append([line[2], int(line[3])])
                result3.append([line[4], int(line[5])])
            else:
                result3.append([line[0], int(line[1])])
                result3.append([line[2], int(line[3])])

            count += 1
        return result3

    def getListNation():
        results = listLineNation()
        result2 = splitData(results)
        return tranformRow(result2)
    # ----------------------------------------------Get National

    #  Get Title----------------------------------------------

    def getTitleList(results5):
        list_title = []
        for line in results5:
            list_title.append(line[0])
        return list_title
    #  ----------------------------------------------Get Title

    # Clean Data ----------------------------------------------
    def cleanDate(nation, value):
        page = value + 1
        print(nation, "page text: ", page)
        destination = '{}/text-extract/image'.format(
            datasource_path) + str(page) + '.txt'
        #     print(destination)

        lines = []
        results = []
        with open(destination) as f:
            lines = f.readlines()

        count = 0
        for line in lines:
            if (line.rstrip() != ''):
                count += 1
                results.append(line)

        results1 = results
        results2 = []
        for line in results1:

            l = line.replace('     ', '=').replace('10,000', '10000').replace('*', '').replace('=   =', '=').replace('=  =',
                                                                                                                     '=').replace(
                '= =', '=').replace('57= ', '57 ')
            if (
                    page == 57 or page == 66 or page == 67 or page == 75 or page == 96 or page == 106 or page == 117 or page == 122):
                l = line.replace('         ', ' ').replace('      ', '=').replace('     ', '=').replace(' - ', '').replace(
                    '10,000', '10000').replace('*', '=').replace('=   =', '=').replace('=  =', '=').replace('=   ',
                                                                                                            '=').replace(
                    '= =', '=').replace('46=', '46 ')
            if (page == 63):
                l = line.replace('     ', '=').replace('10,000', '10000').replace('*', '').replace('-', '').replace('=   =',
                                                                                                                    '=').replace(
                    '=  =', '=').replace('= =', '=').replace(';', '').replace('---', '=').replace('4=', '4 ')
            if (page == 68):
                l = line.replace('             ', ' ').replace('     ', ' ').replace('    ', '=').replace('10,000',
                                                                                                          '10000').replace(
                    '*', '=').replace('=   =', '=').replace('=  =', '=').replace('=   ', '=').replace('= =', '=').replace(
                    '---', '=').replace('46=', '46 ').replace('INFRASTRUCTURE ', 'INFRASTRUCTURE=')
            if (page == 77):
                l = line.replace('         ', ' ').replace('        ', ' ').replace('          ', ' ').replace('     ',
                                                                                                               '=').replace(
                    '10,000', '10000').replace('*', '').replace('=   =', '=').replace('=  =', '=').replace('=   ',
                                                                                                           '=').replace(
                    '= =', '=').replace('46=', '46 ').replace('NETWORK COVERAGE (MINIMUM 4G)',
                                                              'NETWORK COVERAGE (MINIMUM 4G)=').replace(
                    'USED A MOBILE PHONE OR THE INTERNET TO ACCESS AN ACCOUNT ',
                    'USED A MOBILE PHONE OR THE INTERNET TO ACCESS AN ACCOUNT=').replace('MOBILE SUBSCRIBERS',
                                                                                         'MOBILE SUBSCRIBERS=').replace(
                    'INFRASTRUCTURE ', 'INFRASTRUCTURE=').replace('POLICY CONTEXT ', 'POLICY CONTEXT=').replace(
                    'POINT-OF-SALE (POS) TERMINALS PER 10000 PEOPLE ',
                    'POINT-OF-SALE (POS) TERMINALS PER 10000 PEOPLE=').replace('EDUCATIONAL ATTAINMENT ',
                                                                               'EDUCATIONAL ATTAINMENT=').replace(
                    'FIXED-LINE BROADBAND SUBSCRIBERS ', 'FIXED-LINE BROADBAND SUBSCRIBERS=').replace('INTERNET ACCESS ',
                                                                                                      'INTERNET ACCESS=')

            #         print(l)
            results2.append(l)

        results3 = []
        for line in results2:
            l = line.split('=')
            xx = []
            for ll in l:
                xx.append(ll.rstrip().lstrip())
            results3.append(xx)

        results4 = []
        for line in results3:

            xx = []
            for l in line:
                if (len(l) != 0):
                    #                 print(l.replace('\ufeff','').replace('\n','').rstrip().lstrip())
                    xx.append(l.replace('\ufeff', '').replace(
                        '\n', '').rstrip().lstrip())
            #         print(xx)
            results4.append(xx)

        results5 = []
        for line in results4:
            #         print(line)
            l = line[1].split(' ')
            xx = []
            for ll in l:
                if (len(ll) != 0):
                    xx.append(ll)
            results5.append([line[0], *xx])

        list_title = getTitleList(results5)
        #     for line in results5 :
        #         print(line)
        transformToFormat(nation, page, list_title, results5)
    #  ----------------------------------------------Clean Data

    # Transformation Format -------------------------------------
    def transformToFormat(nation, page, list_title, results5):
        print('nation', nation, page)

        country = nation
        year = '2018'
        master = 'GEAR'
        organizer = 'EconomistIntelligenceUnit'
        index = 'GEAR'
        sub_index = ''

        count = 0
        for line in results5:

            if (count == 0):
                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[1])
                i.insert(0, 'Rank')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[2])
                i.insert(0, 'Score')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

            #     ---------------CITIZEN-TO-GOVERNMENT (C2G)
            if (count == 1):
                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[1])
                i.insert(0, 'Rank')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[1])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[2])
                i.insert(0, 'Score')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[1])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

            if (count >= 2 and count <= 7):
                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[1])
                i.insert(0, 'Rank')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[1])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[2])
                i.insert(0, 'Score')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[1])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

            #         ---------------GOVERNMENT-TO-CITIZEN (G2C)
            if (count == 8):
                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[1])
                i.insert(0, 'Rank')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[8])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[2])
                i.insert(0, 'Score')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[8])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

            if (count >= 9 and count <= 12):
                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[1])
                i.insert(0, 'Rank')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[8])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[2])
                i.insert(0, 'Score')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[8])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

            #         ---------------BUSINESS-TO-GOVERNMENT (B2G)
            if (count == 13):
                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[1])
                i.insert(0, 'Rank')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[13])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[2])
                i.insert(0, 'Score')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[13])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

            if (count >= 14 and count <= 17):
                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[1])
                i.insert(0, 'Rank')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[13])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[2])
                i.insert(0, 'Score')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[13])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

            #     ---------------GOVERNMENT-TO-BUSINESS (G2B)
            if (count == 18):
                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[1])
                i.insert(0, 'Rank')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[18])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[2])
                i.insert(0, 'Score')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[18])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

            if (count >= 19 and count <= 22):
                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[1])
                i.insert(0, 'Rank')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[18])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[2])
                i.insert(0, 'Score')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[18])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

            #     ---------------INFRASTRUCTURE
            if (count == 23):
                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[1])
                i.insert(0, 'Rank')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[23])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[2])
                i.insert(0, 'Score')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[23])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

            if (count >= 24 and count <= 29):
                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[1])
                i.insert(0, 'Rank')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, 'Internet infrastructure')
                i.insert(0, list_title[23])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[2])
                i.insert(0, 'Score')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, 'Internet infrastructure')
                i.insert(0, list_title[23])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

            if (count >= 30 and count <= 32):
                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[1])
                i.insert(0, 'Rank')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, 'Payment infrastructure')
                i.insert(0, list_title[23])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[2])
                i.insert(0, 'Score')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, 'Payment infrastructure')
                i.insert(0, list_title[23])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

            #     ---------------SOCIAL AND ECONOMIC CONTEXT
            if (count == 33):
                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[1])
                i.insert(0, 'Rank')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[33])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[2])
                i.insert(0, 'Score')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[33])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

            if (count >= 34 and count <= 35):
                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[1])
                i.insert(0, 'Rank')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, 'Readiness')
                i.insert(0, list_title[33])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[2])
                i.insert(0, 'Score')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, 'Readiness')
                i.insert(0, list_title[33])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

            if (count >= 36 and count <= 37):
                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[1])
                i.insert(0, 'Rank')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, 'Banking access')
                i.insert(0, list_title[33])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[2])
                i.insert(0, 'Score')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, 'Banking access')
                i.insert(0, list_title[33])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

            if (count >= 38 and count <= 41):
                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[1])
                i.insert(0, 'Rank')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, 'E-commerce')
                i.insert(0, list_title[33])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[2])
                i.insert(0, 'Score')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, 'E-commerce')
                i.insert(0, list_title[33])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

            #     ---------------POLICY CONTEXT
            if (count == 42):
                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[1])
                i.insert(0, 'Rank')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[42])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[2])
                i.insert(0, 'Score')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, list_title[42])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

            if (count >= 43 and count <= 47):
                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[1])
                i.insert(0, 'Rank')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, 'Operating environment')
                i.insert(0, list_title[42])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[2])
                i.insert(0, 'Score')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, 'Operating environment')
                i.insert(0, list_title[42])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

            if (count >= 48 and count <= 49):
                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[1])
                i.insert(0, 'Rank')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, 'Trust and Safety')
                i.insert(0, list_title[42])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[2])
                i.insert(0, 'Score')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, 'Trust and Safety')
                i.insert(0, list_title[42])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

            if (count >= 50 and count <= 52):
                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[1])
                i.insert(0, 'Rank')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, 'Inclusiveness')
                i.insert(0, list_title[42])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

                i = []
                i.insert(0, datetime.now().strftime("%d/%m/%Y %H:%M"))
                i.insert(0, line[2])
                i.insert(0, 'Score')
                i.insert(0, '')
                i.insert(0, '')
                i.insert(0, line[0])
                i.insert(0, '')
                i.insert(0, 'Inclusiveness')
                i.insert(0, list_title[42])
                i.insert(0, sub_index)
                i.insert(0, index)
                i.insert(0, organizer)
                i.insert(0, master)
                i.insert(0, year)
                i.insert(0, country)
                result7List.append(i)

            count += 1
        pd.set_option("display.max_rows", None, "display.max_columns", None)
        pd.DataFrame(result7List)
    # ------------------------------------- Transformation Format

    # Generate CSV--------------------------------------------

    def genCSV():
        count = 0
        final_result = []
        for line in result7List:
            line[6] = line[6].replace(
                'CITIZENTOGOVERNMENT(C2G)', 'CITIZEN-TO-GOVERNMENT (C2G)')
            line[6] = line[6].replace(
                'CITIZENTOGOVERNMENT (C2G)', 'CITIZEN-TO-GOVERNMENT (C2G)')
            line[6] = line[6].replace(
                '1 CITIZEN-TO-GOVERNMENT (C2G)', 'CITIZEN-TO-GOVERNMENT (C2G)')
            line[6] = line[6].replace(
                'I SOCIALAND ECONOMIC CONTEXT', 'SOCIALAND ECONOMIC CONTEXT')

            line[6] = line[6].replace(
                'IGOVERNMENT-TO-CITIZEN (G2C)', 'GOVERNMENT-TO-CITIZEN (G2C)')

            line[6] = line[6].replace(
                'BUSINESSTOGOVERNMENT (B2G)', 'BUSINESS-TO-GOVERNMENT (B2G)')

            line[6] = line[6].replace(
                'GOVERNMENTTOBUSINESS (G2B)', 'GOVERNMENT-TO-BUSINESS (G2B)')

            line[6] = line[6].replace(
                'SOCIALAND ECONOMIC CONTEXT', 'SOCIAL AND ECONOMIC CONTEXT')

            line[6] = line[6].replace(
                'GOVERNMENTTOCITIZEN (G2C)', 'GOVERNMENT-TO-CITIZEN (G2C)')
            line[6] = line[6].replace(
                'I GOVERNMENT-TO-CITIZEN (G2C)', 'GOVERNMENT-TO-CITIZEN (G2C)')

            line[6] = line[6].replace('I POLICY CONTEXT', 'POLICY CONTEXT')
            line[0] = line[0].replace('HongKongChina', 'HongKong/China')

            if (count == 0):
                xx = ['id']
                xx.extend(line)
            if (count != 0):
                xx = [count]
                xx.extend(line)
            final_result.append(xx)
            # #     print(line)
            #     xx = []
            count += 1

        final_result

        pd.DataFrame(final_result)
        ingest_date = datetime.now()

        a = np.asarray(final_result)
        np.savetxt('{}/GEAR_{}_{}.csv'.format(output_path, 2018,
                                              ingest_date.strftime("%Y%m%d%H%M%S")), a, delimiter=",", fmt='%s')
    # ------------------------------------------------- Generate CSV

    # Start Program ---------------------------------------------
    result7List = [['country', 'year', 'master_index', 'organizer', 'index', 'sub_index', 'pillar',
                    'sub_pillar', 'sub_sub_pillar', 'indicator', 'sub_indicator', 'others', 'unit_2', 'value', 'date_etl']]

    def main():

        nation_list = getListNation()
        # sort national
        df = pd.DataFrame(nation_list)
        df.sort_values(by=[1], inplace=True)
        nation_list = df.values.tolist()

        for line in nation_list:
            print('--------------------&&&&&&&&----------------------')

            print(line)
            if (line[1] + 1 >= start_page and line[1] + 1 <= end_page):
                cleanDate(line[0], line[1])
        genCSV()

    main()


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

dag = DAG('GEAR', default_args=default_args, catchup=False)

def store_to_hdfs(**kwargs):
    hdfs = PyWebHdfsClient(host='10.121.101.130',
                           port='50070', user_name='hdfs')
    my_dir = kwargs['directory']
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    path = "/opt/airflow/dags/output/gear"

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
    index_name = "Government e-Payment Adoption Ranking (GEAR)"
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

with dag:
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    load_to_hdfs_raw_zone = PythonOperator(
        task_id='load_to_hdfs_raw_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/raw/index_dashboard/Global/GEAR'},
    )

    load_to_hdfs_processed_zone = PythonOperator(
        task_id='load_to_hdfs_processed_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/processed/index_dashboard/Global/GEAR'},
    )

    clean_up_output = BashOperator(
        task_id='clean_up_output',
        bash_command='rm -f /opt/airflow/dags/output/gear/*',
    )

    send_email = PythonOperator(
        task_id='send_email',
        python_callable=send_mail,
    )

transform >> load_to_hdfs_raw_zone >> load_to_hdfs_processed_zone >> clean_up_output >> send_email