import os
import pandas as pd
import smtplib
import ssl
import pytz
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint
from playwright.sync_api import sync_playwright
from airflow.models import Variable

downloadLink = "https://www.globalinnovationindex.org/analysis-indicator"
outputRawDir = "/opt/airflow/dags/output/gii/raw"
outputDir = "/opt/airflow/dags/output/gii/data"
tzInfo = pytz.timezone('Asia/Bangkok')


class nestDict(dict):
    def __missing__(self, key):
        value = self[key] = type(self)()
        return value


####################################################################################

columns = (
    "country",
    "year",
    "master_index",
    "organizer",
    "index",
    "sub_index",
    "pillar",
    "sub_pillar",
    "sub_sub_pillar",
    "indicator",
    "sub_indicator",
    "others",
    "unit_2",
    "value",
    "ingest_date",
)


meta = {
    "1": {"sub-index": "Innovation Input", "pillar": "INSTITUTIONS"},
    "2": {"sub-index": "Innovation Input", "pillar": "HUMAN CAPITAL AND RESEARCH"},
    "3": {"sub-index": "Innovation Input", "pillar": "INFRASTRUCTURE"},
    "4": {"sub-index": "Innovation Input", "pillar": "MARKET SOPHISTICATION"},
    "5": {"sub-index": "Innovation Input", "pillar": "BUSINESS SOPHISTICATION"},
    "6": {
        "sub-index": "Innovation Output",
        "pillar": "KNOWLEDGE AND TECHNOLOGY OUTPUTS",
    },
    "7": {"sub-index": "Innovation Output", "pillar": "CREATIVE OUTPUTS"},
    "1.1": {
        "sub-index": "Innovation Input",
        "pillar": "INSTITUTIONS",
        "sub-pillar": "Political environment",
    },
    "1.1.1": {
        "sub-index": "Innovation Input",
        "pillar": "INSTITUTIONS",
        "sub-pillar": "Political environment",
        "indicator": "Political and operational stability",
    },
    "1.1.2": {
        "sub-index": "Innovation Input",
        "pillar": "INSTITUTIONS",
        "sub-pillar": "Political environment",
        "indicator": "Government effectiveness",
    },
    "1.1.3": {
        "sub-index": "Innovation Input",
        "pillar": "INSTITUTIONS",
        "sub-pillar": "Political environment",
        "indicator": "Press freedom",
    },
    "1.2": {
        "sub-index": "Innovation Input",
        "pillar": "INSTITUTIONS",
        "sub-pillar": "Regulatory environment",
    },
    "1.2.1": {
        "sub-index": "Innovation Input",
        "pillar": "INSTITUTIONS",
        "sub-pillar": "Regulatory environment",
        "indicator": "Regulatory quality",
    },
    "1.2.2": {
        "sub-index": "Innovation Input",
        "pillar": "INSTITUTIONS",
        "sub-pillar": "Regulatory environment",
        "indicator": "Rule of law",
    },
    "1.2.3": {
        "sub-index": "Innovation Input",
        "pillar": "INSTITUTIONS",
        "sub-pillar": "Regulatory environment",
        "indicator": "Cost of redundancy dismissal, salary weeks",
    },
    "1.3": {
        "sub-index": "Innovation Input",
        "pillar": "INSTITUTIONS",
        "sub-pillar": "Business environment",
    },
    "1.3.1": {
        "sub-index": "Innovation Input",
        "pillar": "INSTITUTIONS",
        "sub-pillar": "Business environment",
        "indicator": "Ease of starting a business",
    },
    "1.3.2": {
        "sub-index": "Innovation Input",
        "pillar": "INSTITUTIONS",
        "sub-pillar": "Business environment",
        "indicator": "Ease of resolving insolvency",
    },
    "1.3.3": {
        "sub-index": "Innovation Input",
        "pillar": "INSTITUTIONS",
        "sub-pillar": "Business environment",
        "indicator": "Ease of paying taxes",
    },
    "2.1": {
        "sub-index": "Innovation Input",
        "pillar": "HUMAN CAPITAL AND RESEARCH",
        "sub-pillar": "Education",
    },
    "2.1.1": {
        "sub-index": "Innovation Input",
        "pillar": "HUMAN CAPITAL AND RESEARCH",
        "sub-pillar": "Education",
        "indicator": "Expenditure on education",
    },
    "2.1.2": {
        "sub-index": "Innovation Input",
        "pillar": "HUMAN CAPITAL AND RESEARCH",
        "sub-pillar": "Education",
        "indicator": "Government funding per secondary student",
    },
    "2.1.3": {
        "sub-index": "Innovation Input",
        "pillar": "HUMAN CAPITAL AND RESEARCH",
        "sub-pillar": "Education",
        "indicator": "School life expectancy",
    },
    "2.1.4": {
        "sub-index": "Innovation Input",
        "pillar": "HUMAN CAPITAL AND RESEARCH",
        "sub-pillar": "Education",
        "indicator": "Assessment in reading, mathematics, and science",
    },
    "2.1.5": {
        "sub-index": "Innovation Input",
        "pillar": "HUMAN CAPITAL AND RESEARCH",
        "sub-pillar": "Education",
        "indicator": "Pupil-teacher ratio, secondary",
    },
    "2.2": {
        "sub-index": "Innovation Input",
        "pillar": "HUMAN CAPITAL AND RESEARCH",
        "sub-pillar": "Tertiary education",
    },
    "2.2.1": {
        "sub-index": "Innovation Input",
        "pillar": "HUMAN CAPITAL AND RESEARCH",
        "sub-pillar": "Tertiary education",
        "indicator": "Tertiary enrolment",
    },
    "2.2.2": {
        "sub-index": "Innovation Input",
        "pillar": "HUMAN CAPITAL AND RESEARCH",
        "sub-pillar": "Tertiary education",
        "indicator": "Graduates in science and engineering",
    },
    "2.2.3": {
        "sub-index": "Innovation Input",
        "pillar": "HUMAN CAPITAL AND RESEARCH",
        "sub-pillar": "Tertiary education",
        "indicator": "Tertiary inbound mobility, %",
    },
    "2.2.4": {
        "sub-index": "Innovation Input",
        "pillar": "HUMAN CAPITAL AND RESEARCH",
        "sub-pillar": "Tertiary education",
        "indicator": "Gross tertiary outbound enrolment",
    },
    "2.3": {
        "sub-index": "Innovation Input",
        "pillar": "HUMAN CAPITAL AND RESEARCH",
        "sub-pillar": "Research and development (R&D)",
    },
    "2.3.1": {
        "sub-index": "Innovation Input",
        "pillar": "HUMAN CAPITAL AND RESEARCH",
        "sub-pillar": "Research and development (R&D)",
        "indicator": "Researchers FTE",
    },
    "2.3.2": {
        "sub-index": "Innovation Input",
        "pillar": "HUMAN CAPITAL AND RESEARCH",
        "sub-pillar": "Research and development (R&D)",
        "indicator": "Gross expenditure on R&D (GERD)",
    },
    "2.3.3": {
        "sub-index": "Innovation Input",
        "pillar": "HUMAN CAPITAL AND RESEARCH",
        "sub-pillar": "Research and development (R&D)",
        "indicator": "Global R&D companies, average expenditure, top 3",
    },
    "2.3.4": {
        "sub-index": "Innovation Input",
        "pillar": "HUMAN CAPITAL AND RESEARCH",
        "sub-pillar": "Research and development (R&D)",
        "indicator": "QS university ranking score of top 3 universities",
    },
    "3.1": {
        "sub-index": "Innovation Input",
        "pillar": "INFRASTRUCTURE",
        "sub-pillar": "Information and communication technologies (ICTs)",
    },
    "3.1.1": {
        "sub-index": "Innovation Input",
        "pillar": "INFRASTRUCTURE",
        "sub-pillar": "Information and communication technologies (ICTs)",
        "indicator": "ICT access",
    },
    "3.1.2": {
        "sub-index": "Innovation Input",
        "pillar": "INFRASTRUCTURE",
        "sub-pillar": "Information and communication technologies (ICTs)",
        "indicator": "ICT use",
    },
    "3.1.3": {
        "sub-index": "Innovation Input",
        "pillar": "INFRASTRUCTURE",
        "sub-pillar": "Information and communication technologies (ICTs)",
        "indicator": "Government’s online service",
    },
    "3.1.4": {
        "sub-index": "Innovation Input",
        "pillar": "INFRASTRUCTURE",
        "sub-pillar": "Information and communication technologies (ICTs)",
        "indicator": "Online e-participation",
    },
    "3.2": {
        "sub-index": "Innovation Input",
        "pillar": "INFRASTRUCTURE",
        "sub-pillar": "General infrastructure",
    },
    "3.2.1": {
        "sub-index": "Innovation Input",
        "pillar": "INFRASTRUCTURE",
        "sub-pillar": "General infrastructure",
        "indicator": "Electricity output",
    },
    "3.2.2": {
        "sub-index": "Innovation Input",
        "pillar": "INFRASTRUCTURE",
        "sub-pillar": "General infrastructure",
        "indicator": "Logistics performance",
    },
    "3.2.3": {
        "sub-index": "Innovation Input",
        "pillar": "INFRASTRUCTURE",
        "sub-pillar": "General infrastructure",
        "indicator": "Gross capital formation",
    },
    "3.2.4": {
        "sub-index": "Innovation Input",
        "pillar": "INFRASTRUCTURE",
        "sub-pillar": "General infrastructure",
        "indicator": "Gross capital formation",
    },
    "3.3": {
        "sub-index": "Innovation Input",
        "pillar": "INFRASTRUCTURE",
        "sub-pillar": "Ecological sustainability",
    },
    "3.3.1": {
        "sub-index": "Innovation Input",
        "pillar": "INFRASTRUCTURE",
        "sub-pillar": "Ecological sustainability",
        "indicator": "GDP/unit of energy use",
    },
    "3.3.2": {
        "sub-index": "Innovation Input",
        "pillar": "INFRASTRUCTURE",
        "sub-pillar": "Ecological sustainability",
        "indicator": "Environmental performance",
    },
    "3.3.3": {
        "sub-index": "Innovation Input",
        "pillar": "INFRASTRUCTURE",
        "sub-pillar": "Ecological sustainability",
        "indicator": "ISO 14001 environmental certificates/bn PPP$ GDP",
    },
    "4.1": {
        "sub-index": "Innovation Input",
        "pillar": "MARKET SOPHISTICATION",
        "sub-pillar": "Credit",
    },
    "4.1.1": {
        "sub-index": "Innovation Input",
        "pillar": "MARKET SOPHISTICATION",
        "sub-pillar": "Credit",
        "indicator": "Ease of getting credit",
    },
    "4.1.2": {
        "sub-index": "Innovation Input",
        "pillar": "MARKET SOPHISTICATION",
        "sub-pillar": "Credit",
        "indicator": "Domestic credit to private sector",
    },
    "4.1.3": {
        "sub-index": "Innovation Input",
        "pillar": "MARKET SOPHISTICATION",
        "sub-pillar": "Credit",
        "indicator": "Microfinance institutions gross loan portfolio",
    },
    "4.2": {
        "sub-index": "Innovation Input",
        "pillar": "MARKET SOPHISTICATION",
        "sub-pillar": "Investment",
    },
    "4.2.1": {
        "sub-index": "Innovation Input",
        "pillar": "MARKET SOPHISTICATION",
        "sub-pillar": "Investment",
        "indicator": "Ease of protecting minority investors",
    },
    "4.2.2": {
        "sub-index": "Innovation Input",
        "pillar": "MARKET SOPHISTICATION",
        "sub-pillar": "Investment",
        "indicator": "Market capitalization",
    },
    "4.2.3": {
        "sub-index": "Innovation Input",
        "pillar": "MARKET SOPHISTICATION",
        "sub-pillar": "Investment",
        "indicator": "Venture capital deals",
    },
    "4.2.4": {
        "sub-index": "Innovation Input",
        "pillar": "MARKET SOPHISTICATION",
        "sub-pillar": "Investment",
        "indicator": "Venture capital deals",
    },
    "4.3": {
        "sub-index": "Innovation Input",
        "pillar": "MARKET SOPHISTICATION",
        "sub-pillar": "Trade, competition, and market scale",
    },
    "4.3.1": {
        "sub-index": "Innovation Input",
        "pillar": "MARKET SOPHISTICATION",
        "sub-pillar": "Trade, competition, and market scale",
        "indicator": "Applied tariff rate, weighted average",
    },
    "4.3.2": {
        "sub-index": "Innovation Input",
        "pillar": "MARKET SOPHISTICATION",
        "sub-pillar": "Trade, competition, and market scale",
        "indicator": "Intensity of local competition",
    },
    "4.3.3": {
        "sub-index": "Innovation Input",
        "pillar": "MARKET SOPHISTICATION",
        "sub-pillar": "Trade, competition, and market scale",
        "indicator": "Domestic market scale",
    },
    "5.1": {
        "sub-index": "Innovation Input",
        "pillar": "BUSINESS SOPHISTICATION",
        "sub-pillar": "Knowledge workers",
    },
    "5.1.1": {
        "sub-index": "Innovation Input",
        "pillar": "BUSINESS SOPHISTICATION",
        "sub-pillar": "Knowledge workers",
        "indicator": "Knowledge-intensive employment",
    },
    "5.1.2": {
        "sub-index": "Innovation Input",
        "pillar": "BUSINESS SOPHISTICATION",
        "sub-pillar": "Knowledge workers",
        "indicator": "Firms offering formal training",
    },
    "5.1.3": {
        "sub-index": "Innovation Input",
        "pillar": "BUSINESS SOPHISTICATION",
        "sub-pillar": "Knowledge workers",
        "indicator": "GERD performed by business enterprise",
    },
    "5.1.4": {
        "sub-index": "Innovation Input",
        "pillar": "BUSINESS SOPHISTICATION",
        "sub-pillar": "Knowledge workers",
        "indicator": "GERD financed by business enterprise",
    },
    "5.1.5": {
        "sub-index": "Innovation Input",
        "pillar": "BUSINESS SOPHISTICATION",
        "sub-pillar": "Knowledge workers",
        "indicator": "Females employed with advanced degrees",
    },
    "5.1.6": {
        "sub-index": "Innovation Input",
        "pillar": "BUSINESS SOPHISTICATION",
        "sub-pillar": "Knowledge workers",
        "indicator": "GMAT test takers",
    },
    "5.2": {
        "sub-index": "Innovation Input",
        "pillar": "BUSINESS SOPHISTICATION",
        "sub-pillar": "Innovation linkages",
    },
    "5.2.1": {
        "sub-index": "Innovation Input",
        "pillar": "BUSINESS SOPHISTICATION",
        "sub-pillar": "Innovation linkages",
        "indicator": "University/industry research collaboration",
    },
    "5.2.2": {
        "sub-index": "Innovation Input",
        "pillar": "BUSINESS SOPHISTICATION",
        "sub-pillar": "Innovation linkages",
        "indicator": "State of cluster development",
    },
    "5.2.3": {
        "sub-index": "Innovation Input",
        "pillar": "BUSINESS SOPHISTICATION",
        "sub-pillar": "Innovation linkages",
        "indicator": "GERD financed by abroad",
    },
    "5.2.4": {
        "sub-index": "Innovation Input",
        "pillar": "BUSINESS SOPHISTICATION",
        "sub-pillar": "Innovation linkages",
        "indicator": "Joint venture/strategic alliance deals",
    },
    "5.2.5": {
        "sub-index": "Innovation Input",
        "pillar": "BUSINESS SOPHISTICATION",
        "sub-pillar": "Innovation linkages",
        "indicator": "Patent families filed in two offices",
    },
    "5.3": {
        "sub-index": "Innovation Input",
        "pillar": "BUSINESS SOPHISTICATION",
        "sub-pillar": "Knowledge absorption",
    },
    "5.3.1": {
        "sub-index": "Innovation Input",
        "pillar": "BUSINESS SOPHISTICATION",
        "sub-pillar": "Knowledge absorption",
        "indicator": "Intellectual property payments",
    },
    "5.3.2": {
        "sub-index": "Innovation Input",
        "pillar": "BUSINESS SOPHISTICATION",
        "sub-pillar": "Knowledge absorption",
        "indicator": "High-tech imports",
    },
    "5.3.3": {
        "sub-index": "Innovation Input",
        "pillar": "BUSINESS SOPHISTICATION",
        "sub-pillar": "Knowledge absorption",
        "indicator": "ICT services imports",
    },
    "5.3.4": {
        "sub-index": "Innovation Input",
        "pillar": "BUSINESS SOPHISTICATION",
        "sub-pillar": "Knowledge absorption",
        "indicator": "Foreign direct investment net inflows",
    },
    "5.3.5": {
        "sub-index": "Innovation Input",
        "pillar": "BUSINESS SOPHISTICATION",
        "sub-pillar": "Knowledge absorption",
        "indicator": "Research talent in business enterprise",
    },
    "6.1": {
        "sub-index": "Innovation Output",
        "pillar": "KNOWLEDGE AND TECHNOLOGY OUTPUTS",
        "sub-pillar": "Knowledge creation",
    },
    "6.1.1": {
        "sub-index": "Innovation Output",
        "pillar": "KNOWLEDGE AND TECHNOLOGY OUTPUTS",
        "sub-pillar": "Knowledge creation",
        "indicator": "Patent applications by origin",
    },
    "6.1.2": {
        "sub-index": "Innovation Output",
        "pillar": "KNOWLEDGE AND TECHNOLOGY OUTPUTS",
        "sub-pillar": "Knowledge creation",
        "indicator": "PCT applications by origin",
    },
    "6.1.3": {
        "sub-index": "Innovation Output",
        "pillar": "KNOWLEDGE AND TECHNOLOGY OUTPUTS",
        "sub-pillar": "Knowledge creation",
        "indicator": "Utility models by origin",
    },
    "6.1.4": {
        "sub-index": "Innovation Output",
        "pillar": "KNOWLEDGE AND TECHNOLOGY OUTPUTS",
        "sub-pillar": "Knowledge creation",
        "indicator": "Scientific and technical publications",
    },
    "6.1.5": {
        "sub-index": "Innovation Output",
        "pillar": "KNOWLEDGE AND TECHNOLOGY OUTPUTS",
        "sub-pillar": "Knowledge creation",
        "indicator": "Citable documents H-index",
    },
    "6.2": {
        "sub-index": "Innovation Output",
        "pillar": "KNOWLEDGE AND TECHNOLOGY OUTPUTS",
        "sub-pillar": "Knowledge impact",
    },
    "6.2.1": {
        "sub-index": "Innovation Output",
        "pillar": "KNOWLEDGE AND TECHNOLOGY OUTPUTS",
        "sub-pillar": "Knowledge impact",
        "indicator": "Growth rate of GDP per person engaged",
    },
    "6.2.2": {
        "sub-index": "Innovation Output",
        "pillar": "KNOWLEDGE AND TECHNOLOGY OUTPUTS",
        "sub-pillar": "Knowledge impact",
        "indicator": "New business density",
    },
    "6.2.3": {
        "sub-index": "Innovation Output",
        "pillar": "KNOWLEDGE AND TECHNOLOGY OUTPUTS",
        "sub-pillar": "Knowledge impact",
        "indicator": "Total computer software spending",
    },
    "6.2.4": {
        "sub-index": "Innovation Output",
        "pillar": "KNOWLEDGE AND TECHNOLOGY OUTPUTS",
        "sub-pillar": "Knowledge impact",
        "indicator": "ISO 9001 quality certificates",
    },
    "6.2.5": {
        "sub-index": "Innovation Output",
        "pillar": "KNOWLEDGE AND TECHNOLOGY OUTPUTS",
        "sub-pillar": "Knowledge impact",
        "indicator": "High- and medium-high-tech manufacturing",
    },
    "6.3": {
        "sub-index": "Innovation Output",
        "pillar": "KNOWLEDGE AND TECHNOLOGY OUTPUTS",
        "sub-pillar": "Knowledge diffusion",
    },
    "6.3.1": {
        "sub-index": "Innovation Output",
        "pillar": "KNOWLEDGE AND TECHNOLOGY OUTPUTS",
        "sub-pillar": "Knowledge diffusion",
        "indicator": "Intellectual property receipts",
    },
    "6.3.2": {
        "sub-index": "Innovation Output",
        "pillar": "KNOWLEDGE AND TECHNOLOGY OUTPUTS",
        "sub-pillar": "Knowledge diffusion",
        "indicator": "High-tech exports",
    },
    "6.3.3": {
        "sub-index": "Innovation Output",
        "pillar": "KNOWLEDGE AND TECHNOLOGY OUTPUTS",
        "sub-pillar": "Knowledge diffusion",
        "indicator": "ICT services exports",
    },
    "6.3.4": {
        "sub-index": "Innovation Output",
        "pillar": "KNOWLEDGE AND TECHNOLOGY OUTPUTS",
        "sub-pillar": "Knowledge diffusion",
        "indicator": "Foreign direct investments net outflows",
    },
    "7.1": {
        "sub-index": "Innovation Output",
        "pillar": "CREATIVE OUTPUTS",
        "sub-pillar": "Intangible assets",
    },
    "7.1.1": {
        "sub-index": "Innovation Output",
        "pillar": "CREATIVE OUTPUTS",
        "sub-pillar": "Intangible assets",
        "indicator": "Trademark application class count by origin",
    },
    "7.1.2": {
        "sub-index": "Innovation Output",
        "pillar": "CREATIVE OUTPUTS",
        "sub-pillar": "Intangible assets",
        "indicator": "Global brand value",
    },
    "7.1.3": {
        "sub-index": "Innovation Output",
        "pillar": "CREATIVE OUTPUTS",
        "sub-pillar": "Intangible assets",
        "indicator": "Industrial designs by origin",
    },
    "7.1.4": {
        "sub-index": "Innovation Output",
        "pillar": "CREATIVE OUTPUTS",
        "sub-pillar": "Intangible assets",
        "indicator": "ICTs & organizational model creation",
    },
    "7.2": {
        "sub-index": "Innovation Output",
        "pillar": "CREATIVE OUTPUTS",
        "sub-pillar": "Creative goods and services",
    },
    "7.2.1": {
        "sub-index": "Innovation Output",
        "pillar": "CREATIVE OUTPUTS",
        "sub-pillar": "Creative goods and services",
        "indicator": "Cultural and creative services exports",
    },
    "7.2.2": {
        "sub-index": "Innovation Output",
        "pillar": "CREATIVE OUTPUTS",
        "sub-pillar": "Creative goods and services",
        "indicator": "National feature films produced",
    },
    "7.2.3": {
        "sub-index": "Innovation Output",
        "pillar": "CREATIVE OUTPUTS",
        "sub-pillar": "Creative goods and services",
        "indicator": "Entertainment and media market",
    },
    "7.2.4": {
        "sub-index": "Innovation Output",
        "pillar": "CREATIVE OUTPUTS",
        "sub-pillar": "Creative goods and services",
        "indicator": "Printing publications and other media output",
    },
    "7.2.5": {
        "sub-index": "Innovation Output",
        "pillar": "CREATIVE OUTPUTS",
        "sub-pillar": "Creative goods and services",
        "indicator": "Creative goods exports",
    },
    "7.3": {
        "sub-index": "Innovation Output",
        "pillar": "CREATIVE OUTPUTS",
        "sub-pillar": "Online creativity",
    },
    "7.3.1": {
        "sub-index": "Innovation Output",
        "pillar": "CREATIVE OUTPUTS",
        "sub-pillar": "Online creativity",
        "indicator": "Generic top-level domains (gTLDs)",
    },
    "7.3.2": {
        "sub-index": "Innovation Output",
        "pillar": "CREATIVE OUTPUTS",
        "sub-pillar": "Online creativity",
        "indicator": "Country-code top-level domains (ccTLDs)",
    },
    "7.3.3": {
        "sub-index": "Innovation Output",
        "pillar": "CREATIVE OUTPUTS",
        "sub-pillar": "Online creativity",
        "indicator": "Wikipedia yearly edits",
    },
    "7.3.4": {
        "sub-index": "Innovation Output",
        "pillar": "CREATIVE OUTPUTS",
        "sub-pillar": "Online creativity",
        "indicator": "Mobile app creation",
    },
}


####################################################################################


def ingress_data():
    print("- Start Download")
    print("- Start Playwright")

    with sync_playwright() as pw:
        print("- Open browser")
        browser = pw.chromium.launch()
        page = browser.new_page(accept_downloads=True)
        print(f"- Goto {downloadLink}")
        page.goto(downloadLink)

        print("- Select All options in select year")
        year_options_obj = page.query_selector_all(
            "select#ctl33_lstYear >> option")

        if len(year_options_obj) == 0:
            print("- No year options find")
            print("- Close browser")
            browser.close()
            raise "Not Found options"

        tmp = os.listdir(outputRawDir)
        year_options = [x.get_attribute("value") for x in year_options_obj]
        for year in year_options:
            filename = f"year_{year}.csv"
            print("- Check file.")
            if filename in tmp:
                print(f"- {year} is downloaded")
                continue
            page.select_option("select#ctl33_lstYear", year)
            page.wait_for_load_state(state="networkidle")
            with page.expect_download(timeout=0) as download_info:
                page.click("input#ctl33_BtnCsvFullReport")
            download = download_info.value
            _ = download.path()
            page.wait_for_load_state(state="networkidle")

            print("- Save as {}".format(os.path.join(outputRawDir, filename)))
            download.save_as(os.path.join(outputRawDir, filename))
            print("- End download")

        browser.close()


####################################################################################

mi = "GII"
index = "Global Innovation Index"
org = "World Intellectual Property Organization"
now = datetime.now(tz=tzInfo)
etl = now.strftime("%d/%m/%Y %H:%M")


def prepare_head(df: pd.DataFrame, prop: str, year: int):
    tail = 0 if prop == "Rank" else -2
    prelist = []
    for county, series in df.items():
        if tail < 0:
            county = county[:tail]

        for id, value in series.items():
            if type(value) is str:
                print(value)
                value = value.replace("[", "")
                value = value.replace("]", "")
            if value == "n/a":
                value = 0
            if value == "":
                value = 0
            si = id
            if id.endswith("Global Innovation Index"):
                si = ""
            elif id.endswith(" Index"):
                si = id.replace(" Index", " Ratio")
            elif id.lower().endswith("sub-index"):
                si = id[:-10]
            p = None
            sp = None
            ssp = None
            ind = None
            sind = None
            o = None
            prelist.append(
                [
                    county,
                    year,
                    mi,
                    org,
                    index,
                    si,
                    p,
                    sp,
                    ssp,
                    ind,
                    sind,
                    o,
                    prop,
                    value,
                    etl,
                ]
            )
    return pd.DataFrame(prelist, columns=columns)


def prepare(df: pd.DataFrame, prop: str, year: int):
    tail = 0 if prop == "Rank" else -2
    prelist = []
    for county, series in df.items():
        if tail < 0:
            county = county[:tail]
        for i, value in series.items():
            if type(value) is str:
                print(value)
                value = value.replace("[", "")
                value = value.replace("]", "")
            if value == "n/a":
                value = 0
            if value == "":
                value = 0
            id = i[:-1]
            d = meta[id]
            si = d["sub-index"] if "sub-index" in d else None
            p = d["pillar"] if "pillar" in d else None
            sp = d["sub-pillar"] if "sub-pillar" in d else None
            ssp = None
            ind = d["indicator"] if "indicator" in d else None
            sind = None
            o = None
            prelist.append(
                [
                    county,
                    year,
                    mi,
                    org,
                    index,
                    si,
                    p,
                    sp,
                    ssp,
                    ind,
                    sind,
                    o,
                    prop,
                    value,
                    etl,
                ]
            )
    return pd.DataFrame(prelist, columns=columns)


def extract_transform():
    files_list = list(filter(lambda x: '.csv' in x, os.listdir(outputRawDir)))
    files_list.sort()

    for filename in files_list:
        year = filename[5:9]
        df = pd.read_csv(os.path.join(outputRawDir, filename), index_col=0)
        print(df.index.tolist())
        idx = df.index.tolist().index("Index") + 1

        dh: pd.DataFrame = df.iloc[1: idx - 1]
        df: pd.DataFrame = df.iloc[idx:]

        dh_rank = dh.loc[:, "Albania":"Zimbabwe"]
        dh_score = dh.loc[:, "Albania.1":"Zimbabwe.1"]

        df_rank = df.loc[:, "Albania":"Zimbabwe"]
        df_score = df.loc[:, "Albania.1":"Zimbabwe.1"]

        dh_rank = prepare_head(dh_rank, "Rank", year)
        dh_score = prepare_head(dh_score, "Score", year)

        df_rank = prepare(df_rank, "Rank", year)
        df_score = prepare(df_score, "Score", year)

        print(f"- year {year} save file")
        save_filename = f"{mi}_{year}_{now.strftime('%Y%m%d%H%M%S')}"
        dm = pd.concat(
            [dh_rank, dh_score, df_rank, df_score],
            ignore_index=True,
        )
        dm.reset_index()
        dm.index += 1
        dm.to_csv(os.path.join(
            outputDir, f"{save_filename}.csv"), index_label="id")

        print(f"- year {year} end")


####################################################################################

default_args = {
    "owner": "ETDA",
    "depends_on_past": False,
    "start_date": "2021-01-25",
    "email": ["oulespaul@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=5),
    "schedule_interval": "@yearly",
}


dag = DAG("GII", default_args=default_args, catchup=False)

def store_to_hdfs(**kwargs):
    hdfs = PyWebHdfsClient(host='vm002namenode.aml.etda.local',
                           port='50070', user_name='hdfs')
    my_dir = kwargs['directory']
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    path = "/opt/airflow/dags/output/gii/data"

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
    index_name = "Global Innovation Index (GII)"
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
    load_data_source = PythonOperator(
        task_id="load_data_source", python_callable=ingress_data
    )

    extract_transform = PythonOperator(
        task_id="extract_transform",
        python_callable=extract_transform,
    )

    load_to_hdfs_raw_zone = PythonOperator(
        task_id='load_to_hdfs_raw_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/raw/index_dashboard/Global/GII'},
    )

    load_to_hdfs_processed_zone = PythonOperator(
        task_id='load_to_hdfs_processed_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/processed/index_dashboard/Global/GII'},
    )

    clean_up_output = BashOperator(
        task_id='clean_up_output',
        bash_command='rm -f /opt/airflow/dags/output/gii/data/*.csv && rm -f /opt/airflow/dags/output/gii/raw/*.csv',
    )

    send_email = PythonOperator(
        task_id='send_email',
        python_callable=send_mail,
    )

load_data_source >> extract_transform >> load_to_hdfs_raw_zone >> load_to_hdfs_processed_zone  >> clean_up_output >> send_email
