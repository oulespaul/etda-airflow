

# ----------------------------------------------------------------Import Library
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import time
import pandas as pd
import numpy as np
import os
import json
import smtplib
import ssl
import pytz
from pprint import pprint
from datetime import datetime, timedelta
from pywebhdfs.webhdfs import PyWebHdfsClient

from azure.cognitiveservices.vision.computervision import ComputerVisionClient
from azure.cognitiveservices.vision.computervision.models import OperationStatusCodes
from msrest.authentication import CognitiveServicesCredentials

from pdf2image import convert_from_path
from PyPDF2 import PdfFileWriter, PdfFileReader

tzInfo = pytz.timezone('Asia/Bangkok')

start_page = 55
end_page = 127
nation_page = 54

file_name = 'government-e-payment-adoption-ranking-study-2018.pdf'
# Import Library----------------------------------------------------------------


# Extract PDF to Image list---------------------------------------------------
def extract_pdf_file():
    # Store Pdf with convert_from_path function
    images = convert_from_path(
        "/opt/airflow/dags/data_source/gear/" + file_name, 200)

    for i in range(len(images)):
        # Save pages as images in the pdf ต้องไปสร้างโพลเดอร์ไว้ก่อน
        print('/opt/airflow/dags/data_source/gear/files-extract/file' +
              str(i+1) + '.pdf')
        # images[i] = images[i].convert('L')
        images[i].save(
            '/opt/airflow/dags/data_source/gear/files-extract/file' + str(i+1) + '.pdf', 'PDF')
# extract_image()
# -Extract PDF to Image list


# --------------------------------------------------------------------crop_pdf
def crop_pdf_nation_detail():
    count = 1
    # เพิ่มตัวแปร count all page
    for i in range(end_page):

        if (count >= start_page and count <= end_page):
            output = PdfFileWriter()
            input = PdfFileReader(
                open('/opt/airflow/dags/data_source/gear/files-extract/file' + str(count) + '.pdf', 'rb'))

            n = input.getNumPages()

            for i in range(n):
                page = input.getPage(i)

                if count == 59 or count == 79 or count == 99 or count == 114:
                    page.cropBox.upperLeft = (657, 142)
                    page.cropBox.lowerRight = (1390, 1792)
                    # quality 200  ok 59 79 99 114
                elif count == 116:
                    page.cropBox.upperLeft = (657, 142)
                    page.cropBox.lowerRight = (1392, 1792)
                    # quality 200  ok 116
                else:
                    page.cropBox.upperLeft = (657, 142)
                    page.cropBox.lowerRight = (1402, 1792)
                    # quality 200 ok

                output.addPage(page)

            outputStream = open(
                '/opt/airflow/dags/data_source/gear/files-crop/file' + str(count) + '.pdf', 'wb')
            print('/opt/airflow/dags/data_source/gear/files-crop/file' +
                  str(count) + '.pdf')
            output.write(outputStream)
            outputStream.close()
        count += 1


def crop_pdf_nation_title():
    output = PdfFileWriter()
    input = PdfFileReader(
        open('/opt/airflow/dags/data_source/gear/files-extract/file' + str(nation_page) + '.pdf', 'rb'))

    n = input.getNumPages()

    for i in range(n):
        page = input.getPage(i)

        # quality = 200
        page.cropBox.upperLeft = (396, 200)
        page.cropBox.lowerRight = (1557, 1352)
        output.addPage(page)

    outputStream = open(
        '/opt/airflow/dags/data_source/gear/files-crop/nation.pdf', 'wb')
    output.write(outputStream)
    outputStream.close()


# crop_pdf---------------------------------------------------------


# Read OCR to Text to Azure --------------------------------------------------
def RunGetText(file_name, count_newline):
    global cv_client
    try:

        # รับค่าการกำหนดค่าในไฟล์ .env
        cog_endpoint = "https://visionocretda.cognitiveservices.azure.com/"
        cog_key = "88ff19f1035545f8b7803587103d031e"

        # Authenticate Computer Vision client
        credential = CognitiveServicesCredentials(cog_key)
        cv_client = ComputerVisionClient(cog_endpoint, credential)

        # เมนูสำหรับฟังก์ชั่นการอ่านข้อความ

        path_file = "/opt/airflow/dags/data_source/gear/files-crop/" + file_name + ".pdf"
        GetTextRead(path_file, file_name, count_newline)

    except Exception as ex:
        print('Error')
        print(ex)


def GetTextRead(path_file, file_name, count_newline):
    text_array = ''
    text_list = ''
    print('Reading text Read in {} \n'.format(path_file))

    # ใช้ Read API เพื่ออ่านข้อความในภาพ
    with open(path_file, mode="rb") as image_data:
        read_op = cv_client.read_in_stream(image_data, raw=True)

    # รับ ID การดำเนินการ async  เพื่อให้เราสามารถตรวจสอบผลลัพธ์ได้
    operation_location = read_op.headers["Operation-Location"]
    operation_id = operation_location.split("/")[-1]
    # รอให้การดำเนินการแบบอะซิงโครนัสเสร็จสิ้น
    while True:
        read_results = cv_client.get_read_result(operation_id)
        if read_results.status not in [OperationStatusCodes.running, OperationStatusCodes.not_started]:
            break
        time.sleep(1)

    # หากดำเนินการสำเร็จ ให้ประมวลผลข้อความทีละบรรทัด
    if read_results.status == OperationStatusCodes.succeeded:
        for page in read_results.analyze_result.read_results:
            count = 0
            for line in page.lines:
                count += 1
                print(line.text)

                if (count == count_newline):
                    text_array += line.text
                    text_list += text_array + '\n'
                    text_array = ''
                    count = 0
                else:
                    text_array += line.text + ','

                # if (count == count_newline) :
                #     text_list += text_array + '\n'
                #     text_array = ''
                #     count = 0

    # readFileText(textAll)
    f = open("/opt/airflow/dags/data_source/gear/files-text-azure/" +
             file_name+".txt", "a")
    f.write(text_list)
    f.close()

# -----------------------------------------------------------Read OCR to Text


# Get National ----------------------------------------------
def listLineNation():
    lines = []
    results = []
    with open('/opt/airflow/dags/data_source/gear/files-text-azure/nation.txt') as f:
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
        # print(ll)
        if (count != 0):
            result2.append(ll.split(','))
        count += 1
    return result2


def tranformRow(result2):
    result3 = []
    count = 0
    for line in result2:
        # print(line[0])
        result3.append([line[0], int(line[1])])

        count += 1
    return result3


def getListNation():
    results = listLineNation()
    result2 = splitData(results)
    return tranformRow(result2)
# ----------------------------------------------Get National


# Forloop  Nation TextFile For Azure-------------------------------------------------------
def getNationTitleTextAzure():
    RunGetText('nation', 2)


# Forloop  Get Nation Detial Azure-------------------------------------------------------
def getNationDetailTextAzure():
    count = 1
    for i in range(end_page):
        if (count >= start_page and count <= end_page):
            orgin = 'file' + str(count)
            RunGetText(orgin, 3)
        count += 1
# -------------------------------------------------------Get Nation TextFile


# Clean Data----------------------------------------------

def cleanDate(nation, value):
    page = value + 1
    print(nation, "page text: ", page)
    destination = '/opt/airflow/dags/data_source/gear/files-text-azure/file' + \
        str(page) + '.txt'
    #     print(destination)

    lines = []
    results = []
    with open(destination) as f:
        lines = f.readlines()

    count = 0
    for line in lines:
        if (line.rstrip() != ''):
            count += 1
            #             print(f'line {count}: {line}')
            results.append(line)

    results1 = results
    results2 = []
    for line in results1:
        l = line.replace('', '').replace('10,000', '10000').replace('*', '')
        print(l)
        results2.append(l)

    results3 = []
    for line in results2:
        l = line.split(',')
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

    list_title = getTitleList(results4)
    #     for line in results5 :
    #         print(line)
    transformToFormat(nation, page, list_title, results4)


# ----------------------------------------------Clean Data


# Title List-----------------------------------------
def getTitleList(results5):
    list_title = []
    for line in results5:
        list_title.append(line[0])
    return list_title
# ----------------------------------------- Title List


# Transformation Format------------------------------------------

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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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
            i.insert(0, datetime.now(tz=tzInfo).strftime("%d/%m/%Y %H:%M"))
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


# ------------------------------------------Transformation Format


# Generate CSV--------------------------------------------
def genCSV():
    count = 0
    final_result = []
    for line in result7List:

        if (count == 0):
            xx = ['id']
            xx.extend(line)
        if (count != 0):
            xx = [count]
            xx.extend(line)
        final_result.append(xx)

        count += 1

    final_result

    pd.DataFrame(final_result)

    a = np.asarray(final_result)
    np.savetxt('/opt/airflow/dags/output/gear/GEAR_{}_{}.csv'.format(
        2018,
        datetime.now(tz=tzInfo).strftime("%Y%m%d%H%M%S")), a, delimiter=",", fmt=' % s')
# --------------------------------------------Generate CSV


# เตรียม header ของไฟล์ CSv
result7List = [
    ['country', 'year', 'master_index', 'organizer', 'index', 'sub_index', 'pillar', 'sub_pillar', 'sub_sub_pillar',
     'indicator', 'sub_indicator', 'others', 'unit_2', 'value', 'date_etl']]


def extract_transform():
    extract_pdf_file()
    crop_pdf_nation_detail()
    crop_pdf_nation_title()

    getNationDetailTextAzure()
    getNationTitleTextAzure()

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

    pprint("Scrap_data_source and Extract_transform!")


default_args = {
    'owner': 'ETDA',
    'start_date': datetime(2022, 3, 31),
    'schedule_interval': None,
}

dag = DAG('GEAR',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)

def store_to_hdfs(**kwargs):
    hdfs = PyWebHdfsClient(host='vm002namenode.aml.etda.local',
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
        op_kwargs={'directory': '/data/raw/index_dashboard/Global/GEAR'},
    )

    load_to_hdfs_processed_zone = PythonOperator(
        task_id='load_to_hdfs_processed_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/data/processed/index_dashboard/Global/GEAR'},
    )

    clean_up_output = BashOperator(
        task_id='clean_up_output',
        bash_command='rm -f /opt/airflow/dags/output/gear/*',
    )

    send_email = PythonOperator(
        task_id='send_email',
        python_callable=send_mail,
    )

# ingestion >> scrap_and_extract_transform >> load_to_hdfs_raw_zone >> load_to_hdfs_processed_zone >> clean_up_output >> send_email
scrap_and_extract_transform >> load_to_hdfs_raw_zone >> load_to_hdfs_processed_zone >> clean_up_output >> send_email
