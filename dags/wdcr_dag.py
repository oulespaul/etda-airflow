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

outputRawDir = "/opt/airflow/dags/output/wdcr/raw"
outputDir = "/opt/airflow/dags/output/wdcr/data"
tzInfo = pytz.timezone('Asia/Bangkok')

if not os.path.exists(outputDir):
    os.makedirs(outputDir, exist_ok=True)
if not os.path.exists(outputRawDir):
    os.makedirs(outputRawDir, exist_ok=True)

username = "werachon.w@zealtechinter.com"
password = "ETDAIndex@2022"

index_filename = "WDCR_Index.xlsx"
indicator_filename = "WDCR_Indicator.xlsx"


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
    "Knowledge (DIGITAL)": {"id": "1", "pillar": "Knowledge", "sub-pillar": ""},
    "Technology (DIGITAL)": {"id": "2", "pillar": "Technology", "sub-pillar": ""},
    "Future readiness (DIGITAL)": {
        "id": "3",
        "pillar": "Future Readiness",
        "sub-pillar": "",
    },
    "Talent (DIGITAL)": {"id": "1.1", "pillar": "Knowledge", "sub-pillar": "Talent"},
    "Educational assessment PISA - Math (DIGITAL)": {
        "id": "1.1.1",
        "pillar": "Knowledge",
        "sub-pillar": "Talent",
        "indicator": "Educational assessment PISA - Math",
    },
    "International experience (DIGITAL)": {
        "id": "1.1.2",
        "pillar": "Knowledge",
        "sub-pillar": "Talent",
        "indicator": "International experience",
    },
    "Foreign highly-skilled personnel (DIGITAL)": {
        "id": "1.1.3",
        "pillar": "Knowledge",
        "sub-pillar": "Talent",
        "indicator": "Foreign highly-skilled personnel",
    },
    "Management of cities (DIGITAL)": {
        "id": "1.1.4",
        "pillar": "Knowledge",
        "sub-pillar": "Talent",
        "indicator": "Management of cities",
    },
    "Digital/Technological skills (DIGITAL)": {
        "id": "1.1.5",
        "pillar": "Knowledge",
        "sub-pillar": "Talent",
        "indicator": "Digital/Technological skills",
    },
    "Net flow of international students (DIGITAL)": {
        "id": "1.1.6",
        "pillar": "Knowledge",
        "sub-pillar": "Talent",
        "indicator": "Net flow of international students",
    },
    "Exchange Rate (DIGITAL)": {
        "id": "1.1.7",
        "pillar": "Knowledge",
        "sub-pillar": "Talent",
        "indicator": "Exchange Rate",
    },
    "Population - market size (DIGITAL)": {
        "id": "1.1.8",
        "pillar": "Knowledge",
        "sub-pillar": "Talent",
        "indicator": "Population - market size",
    },
    "GDP per capita (DIGITAL)": {
        "id": "1.1.9",
        "pillar": "Knowledge",
        "sub-pillar": "Talent",
        "indicator": "GDP per capita",
    },
    "Training & education (DIGITAL)": {
        "id": "1.2",
        "pillar": "Knowledge",
        "sub-pillar": "Training & education",
    },
    "Employee training (DIGITAL)": {
        "id": "1.2.1",
        "pillar": "Knowledge",
        "sub-pillar": "Training & education",
        "indicator": "Employee training",
    },
    "Total public expenditure on education (DIGITAL)": {
        "id": "1.2.2",
        "pillar": "Knowledge",
        "sub-pillar": "Training & education",
        "indicator": "Total public expenditure on education",
    },
    "Higher education achievement (DIGITAL)": {
        "id": "1.2.3",
        "pillar": "Knowledge",
        "sub-pillar": "Training & education",
        "indicator": "Higher education achievement",
    },
    "Pupil-teacher ratio (tertiary education) (DIGITAL)": {
        "id": "1.2.4",
        "pillar": "Knowledge",
        "sub-pillar": "Training & education",
        "indicator": "Pupil-teacher ratio (tertiary education)",
    },
    "Graduates in Sciences (DIGITAL)": {
        "id": "1.2.5",
        "pillar": "Knowledge",
        "sub-pillar": "Training & education",
        "indicator": "Graduates in Sciences",
    },
    "Women with degrees (DIGITAL)": {
        "id": "1.2.6",
        "pillar": "Knowledge",
        "sub-pillar": "Training & education",
        "indicator": "Women with degrees",
    },
    "Scientific concentration (DIGITAL)": {
        "id": "1.3",
        "pillar": "Knowledge",
        "sub-pillar": "Scientific concentration",
    },
    "Total expenditure on R&D (%) (DIGITAL)": {
        "id": "1.3.1",
        "pillar": "Knowledge",
        "sub-pillar": "Scientific concentration",
        "indicator": "Total expenditure on R&D (%)",
    },
    "Total R&D personnel per capita (DIGITAL)": {
        "id": "1.3.2",
        "pillar": "Knowledge",
        "sub-pillar": "Scientific concentration",
        "indicator": "Total R&D personnel per capita",
    },
    "Female researchers (DIGITAL)": {
        "id": "1.3.3",
        "pillar": "Knowledge",
        "sub-pillar": "Scientific concentration",
        "indicator": "Female researchers",
    },
    "R&D productivity by publication (DIGITAL)": {
        "id": "1.3.4",
        "pillar": "Knowledge",
        "sub-pillar": "Scientific concentration",
        "indicator": "R&D productivity by publication",
    },
    "Scientific and technical employment (DIGITAL)": {
        "id": "1.3.5",
        "pillar": "Knowledge",
        "sub-pillar": "Scientific concentration",
        "indicator": "Scientific and technical employment",
    },
    "High-tech patent grants (DIGITAL)": {
        "id": "1.3.6",
        "pillar": "Knowledge",
        "sub-pillar": "Scientific concentration",
        "indicator": "High-tech patent grants",
    },
    "Robots in Education and R&D (DIGITAL)": {
        "id": "1.3.7",
        "pillar": "Knowledge",
        "sub-pillar": "Scientific concentration",
        "indicator": "Robots in Education and R&D",
    },
    "Regulatory framework (DIGITAL)": {
        "id": "2.1",
        "pillar": "Technology",
        "sub-pillar": "Regulatory framework",
    },
    "Starting a business (DIGITAL)": {
        "id": "2.1.1",
        "pillar": "Technology",
        "sub-pillar": "Regulatory framework",
        "indicator": "Starting a business",
    },
    "Enforcing contracts (DIGITAL)": {
        "id": "2.1.2",
        "pillar": "Technology",
        "sub-pillar": "Regulatory framework",
        "indicator": "Enforcing contracts",
    },
    "Immigration laws (DIGITAL)": {
        "id": "2.1.3",
        "pillar": "Technology",
        "sub-pillar": "Regulatory framework",
        "indicator": "Immigration laws",
    },
    "Development & application of tech. (DIGITAL)": {
        "id": "2.1.4",
        "pillar": "Technology",
        "sub-pillar": "Regulatory framework",
        "indicator": "Development & application of tech.",
    },
    "Scientific research legislation (DIGITAL)": {
        "id": "2.1.5",
        "pillar": "Technology",
        "sub-pillar": "Regulatory framework",
        "indicator": "Scientific research legislation",
    },
    "Intellectual property rights (DIGITAL)": {
        "id": "2.1.6",
        "pillar": "Technology",
        "sub-pillar": "Regulatory framework",
        "indicator": "Intellectual property rights",
    },
    "Capital (DIGITAL)": {"id": "2.2", "pillar": "Technology", "sub-pillar": "Capital"},
    "IT & media stock market capitalization (DIGITAL)": {
        "id": "2.2.1",
        "pillar": "Technology",
        "sub-pillar": "Capital",
        "indicator": "IT & media stock market capitalization",
    },
    "Funding for technological development (DIGITAL)": {
        "id": "2.2.2",
        "pillar": "Technology",
        "sub-pillar": "Capital",
        "indicator": "Funding for technological development",
    },
    "Banking and financial services (DIGITAL)": {
        "id": "2.2.3",
        "pillar": "Technology",
        "sub-pillar": "Capital",
        "indicator": "Banking and financial services",
    },
    "Country credit rating (DIGITAL)": {
        "id": "2.2.4",
        "pillar": "Technology",
        "sub-pillar": "Capital",
        "indicator": "Country credit rating",
    },
    "Venture capital (DIGITAL)": {
        "id": "2.2.5",
        "pillar": "Technology",
        "sub-pillar": "Capital",
        "indicator": "Venture capital",
    },
    "Investment in Telecommunications (DIGITAL)": {
        "id": "2.2.6",
        "pillar": "Technology",
        "sub-pillar": "Capital",
        "indicator": "Investment in Telecommunications",
    },
    "Technological framework (DIGITAL)": {
        "id": "2.3",
        "pillar": "Technology",
        "sub-pillar": "Technological framework",
    },
    "Communications technology (DIGITAL)": {
        "id": "2.3.1",
        "pillar": "Technology",
        "sub-pillar": "Technological framework",
        "indicator": "Communications technology",
    },
    "Mobile Broadband subscribers (DIGITAL)": {
        "id": "2.3.2",
        "pillar": "Technology",
        "sub-pillar": "Technological framework",
        "indicator": "Mobile Broadband subscribers",
    },
    "Wireless broadband (DIGITAL)": {
        "id": "2.3.3",
        "pillar": "Technology",
        "sub-pillar": "Technological framework",
        "indicator": "Wireless broadband",
    },
    "Internet users (DIGITAL)": {
        "id": "2.3.4",
        "pillar": "Technology",
        "sub-pillar": "Technological framework",
        "indicator": "Internet users",
    },
    "Internet bandwidth speed (DIGITAL)": {
        "id": "2.3.5",
        "pillar": "Technology",
        "sub-pillar": "Technological framework",
        "indicator": "Internet bandwidth speed",
    },
    "High-tech exports (%) (DIGITAL)": {
        "id": "2.3.6",
        "pillar": "Technology",
        "sub-pillar": "Technological framework",
        "indicator": "High-tech exports (%)",
    },
    "Adaptive attitudes (DIGITAL)": {
        "id": "3.1",
        "pillar": "Future Readiness",
        "sub-pillar": "Adaptive attitudes",
    },
    "E-Participation (DIGITAL)": {
        "id": "3.1.1",
        "pillar": "Future Readiness",
        "sub-pillar": "Adaptive attitudes",
        "indicator": "E-Participation",
    },
    "Internet retailing (DIGITAL)": {
        "id": "3.1.2",
        "pillar": "Future Readiness",
        "sub-pillar": "Adaptive attitudes",
        "indicator": "Internet retailing",
    },
    "Tablet possession (DIGITAL)": {
        "id": "3.1.3",
        "pillar": "Future Readiness",
        "sub-pillar": "Adaptive attitudes",
        "indicator": "Tablet possession",
    },
    "Smartphone possession (DIGITAL)": {
        "id": "3.1.4",
        "pillar": "Future Readiness",
        "sub-pillar": "Adaptive attitudes",
        "indicator": "Smartphone possession",
    },
    "Attitudes toward globalization (DIGITAL)": {
        "id": "3.1.5",
        "pillar": "Future Readiness",
        "sub-pillar": "Adaptive attitudes",
        "indicator": "Attitudes toward globalization",
    },
    "Business agility (DIGITAL)": {
        "id": "3.2",
        "pillar": "Future Readiness",
        "sub-pillar": "Business agility",
    },
    "Opportunities and threats (DIGITAL)": {
        "id": "3.2.1",
        "pillar": "Future Readiness",
        "sub-pillar": "Business agility",
        "indicator": "Opportunities and threats",
    },
    "World robots distribution (DIGITAL)": {
        "id": "3.2.2",
        "pillar": "Future Readiness",
        "sub-pillar": "Business agility",
        "indicator": "World robots distribution",
    },
    "Agility of companies (DIGITAL)": {
        "id": "3.2.3",
        "pillar": "Future Readiness",
        "sub-pillar": "Business agility",
        "indicator": "Agility of companies",
    },
    "Use of big data and analytics (DIGITAL)": {
        "id": "3.2.4",
        "pillar": "Future Readiness",
        "sub-pillar": "Business agility",
        "indicator": "Use of big data and analytics",
    },
    "Knowledge transfer (DIGITAL)": {
        "id": "3.2.5",
        "pillar": "Future Readiness",
        "sub-pillar": "Business agility",
        "indicator": "Knowledge transfer",
    },
    "Entrepreneurial fear of failure (DIGITAL)": {
        "id": "3.2.6",
        "pillar": "Future Readiness",
        "sub-pillar": "Business agility",
        "indicator": "Entrepreneurial fear of failure",
    },
    "IT integration (DIGITAL)": {
        "id": "3.3",
        "pillar": "Future Readiness",
        "sub-pillar": "IT integration",
    },
    "E-Government (DIGITAL)": {
        "id": "3.3.1",
        "pillar": "Future Readiness",
        "sub-pillar": "IT integration",
        "indicator": "E-Government",
    },
    "Public-private partnerships (DIGITAL)": {
        "id": "3.3.2",
        "pillar": "Future Readiness",
        "sub-pillar": "IT integration",
        "indicator": "Public-private partnerships",
    },
    "Cyber security (DIGITAL)": {
        "id": "3.3.3",
        "pillar": "Future Readiness",
        "sub-pillar": "IT integration",
        "indicator": "Cyber security",
    },
    "Software piracy (DIGITAL)": {
        "id": "3.3.4",
        "pillar": "Future Readiness",
        "sub-pillar": "IT integration",
        "indicator": "Software piracy",
    },
}


####################################################################################
def Download(types="index"):
    with sync_playwright() as pw:
        login_link = "https://worldcompetitiveness.imd.org"

        print("- Open browser")
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(accept_downloads=True)
        print(f"- Goto {login_link}")
        page.goto(login_link)
        print("- Wait EULA Accept")
        page.wait_for_selector("#btnOk")
        page.click("#btnOk")
        page.wait_for_load_state(state="networkidle")
        print("- Wait to login")
        page.click("#user")
        page.wait_for_load_state("networkidle")
        page.wait_for_selector("#login-bttn")
        print("- Login")
        print(f"- Username: {username}")
        print(f"- Password: {password}")
        page.fill("#Email", username)
        page.click("#login-bttn")
        page.fill("#Password", password)
        page.click("#login-bttn")
        page.wait_for_load_state("networkidle")
        print("- Wait for login sso success")
        page.wait_for_selector("#scroll-part > ul > li:nth-child(4) > a")
        page.click("#scroll-part > ul > li:nth-child(4) > a")

        # Select Rankings
        print("- Wait custom select page")
        page.wait_for_selector("#DIGITAL")

        print("- Check Digital")
        # Check box
        page.click(
            "#collapseranking > div.rankingsList > div:nth-child(2) > div:nth-child(1) > div > label"
        )
        page.wait_for_load_state(state="networkidle")
        page.wait_for_selector("#selectRankingNavSection > div > button")
        page.click("#selectRankingNavSection > div > button")

        # Select data type
        print("- Check Cri")
        if types == "index":
            page.wait_for_selector(
                "#dataTypeList > div:nth-child(1) > div:nth-child(1) > div > label"
            )
            page.click(
                "#dataTypeList > div:nth-child(1) > div:nth-child(1) > div > label"
            )
        else:
            page.wait_for_selector(
                "#dataTypeList > div:nth-child(2) > div:nth-child(1) > div > label"
            )
            page.click(
                "#dataTypeList > div:nth-child(2) > div:nth-child(1) > div > label"
            )
        page.wait_for_load_state(state="networkidle")
        page.wait_for_selector("#selectDataTypeNavSection > div.next.active > button")
        page.click("#selectDataTypeNavSection > div.next.active > button")

        # Select your data
        print("- Check all")
        page.wait_for_selector(
            "#collapsedata > div.col-xs-12.col-sm-12.selectall.checkbox > label"
        )
        page.click("#collapsedata > div.col-xs-12.col-sm-12.selectall.checkbox > label")
        page.wait_for_load_state(state="networkidle")
        if types == "index":
            page.wait_for_selector(
                "#collapsedata > div.intro > div.col-xs-12.col-sm-6 > div:nth-child(3) > label"
            )
            page.click(
                "#collapsedata > div.intro > div.col-xs-12.col-sm-6 > div:nth-child(3) > label"
            )
            page.wait_for_selector(
                "#selectYourConsolidatedDataNavSection > div.next.active > button"
            )
            page.click(
                "#selectYourConsolidatedDataNavSection > div.next.active > button"
            )
        else:
            page.wait_for_selector(
                "#selectYourCritDataNavSection > div.next.active > button"
            )
            page.click("#selectYourCritDataNavSection > div.next.active > button")

        # Select Country
        print("- Check all")
        page.wait_for_selector(
            "#collapsecountry > div.col-xs-12.col-sm-12.selectall.checkbox > label"
        )
        page.click(
            "#collapsecountry > div.col-xs-12.col-sm-12.selectall.checkbox > label"
        )
        page.wait_for_load_state(state="networkidle")
        page.wait_for_selector(
            "#selectYourCountryNavSection > div.next.active > button"
        )
        page.click("#selectYourCountryNavSection > div.next.active > button")

        # Select time frame
        # print("- Check last 5 year")
        # page.wait_for_selector(
        #     "#collapsetimeframe > div.items > div:nth-child(2) > label"
        # )
        # page.click("#collapsetimeframe > div.items > div:nth-child(2) > label")
        print("- Check All Year")
        page.wait_for_selector(
            "#collapsetimeframe > div.items > div:nth-child(1) > label"
        )
        page.click("#collapsetimeframe > div.items > div:nth-child(1) > label")

        page.wait_for_load_state(state="networkidle")
        page.wait_for_selector(
            "#selectYourTimeFrameNavSection > div.next.active > button"
        )
        page.click("#selectYourTimeFrameNavSection > div.next.active > button")

        page.wait_for_load_state(state="networkidle")

        # Download page
        print("- Wait for download page")
        if types == "index":
            page.wait_for_selector(".button.blueButton.conso-result-download-options")
            page.click(".button.blueButton.conso-result-download-options")
        else:
            page.wait_for_selector(".button.blueButton.crit-result-download-options")
            page.click(".button.blueButton.crit-result-download-options")
        page.wait_for_selector(
            "#downloadOptionsContent > div.col-xs-12.col-sm-4.row.download-section > div:nth-child(2) > div:nth-child(1) > label"
        )
        page.click(
            "#downloadOptionsContent > div.col-xs-12.col-sm-4.row.download-section > div:nth-child(2) > div:nth-child(1) > label"
        )

        page.click(
            "#downloadOptionsContent > div.col-xs-12.col-sm-6.download-section > div:nth-child(2) > div:nth-child(3) > label"
        )
        page.wait_for_load_state(state="networkidle")
        with page.expect_download(timeout=0) as download_info:
            filename = "WDCR_Index.xlsx"
            if types != "index":
                filename = "WDCR_Indicator.xlsx"
                page.click("#btnCriteriaExcelDownload")
            else:
                page.click("#btnConsolidatedExcelDownload")
            download = download_info.value
            _ = download.path()
            page.wait_for_load_state(state="networkidle")

            print("- Save as {}".format(os.path.join(outputRawDir, filename)))
            download.save_as(os.path.join(outputRawDir, filename))
            print("- End download")
        browser.close()


def ingress_data():
    print("- Start Download")
    print("- Start Playwright")
    print("- Start Download Index - Sub-Index")
    Download("index")
    print("- Start Download Indicator")
    Download("indicator")
    print("- End.")
    pass


####################################################################################


def prepare(df: pd.DataFrame):
    df = df.drop(["Country Key"], axis=1).melt(
        id_vars=["Country", "Year"], var_name="Name", value_name="Score"
    )
    df["Year"] = df["Year"].astype(int)
    df["Rank"] = df.groupby(by=["Year", "Name"])["Score"].rank("dense", ascending=False)
    return df


def extract_transform():
    mi = "WDCR"
    index = "World Digital Competitiveness Ranking"
    org = "International Institute for Management Development"
    now = datetime.now(tz=tzInfo)
    etl = now.strftime("%d/%m/%Y %H:%M")

    df1: pd.DataFrame = pd.read_excel(
        os.path.join(outputRawDir, index_filename), header=0
    ).replace("-", NaN)[:-2]
    df2: pd.DataFrame = pd.read_excel(
        os.path.join(outputRawDir, indicator_filename), header=0
    ).replace("-", NaN)[:-2]

    df1 = prepare(df1)

    df2 = prepare(df2)

    prelist = []
    for idx in df1.index:
        item = df1.loc[idx]
        name = item["Name"]
        country = item["Country"]
        year = item["Year"]
        rank = item["Rank"]
        score = item["Score"]
        # mt = meta[name]
        si = None
        p = None
        sp = None
        ssp = None
        ind = None
        sind = None
        o = None

        if not name == "Overall (DIGITAL)":
            mt = meta[name]
            p = mt["pillar"]
            sp = mt["sub-pillar"]
            pass
        prelist.append(
            [
                country,
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
                "Rank",
                rank,
                etl,
            ]
        )
        prelist.append(
            [
                country,
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
                "Score",
                score,
                etl,
            ]
        )

    for idx in df2.index:
        item = df2.loc[idx]
        name = item["Name"]
        country = item["Country"]
        year = item["Year"]
        rank = item["Rank"]
        score = item["Score"]
        mt = meta[name]
        si = None
        p = mt["pillar"]
        sp = mt["sub-pillar"]
        ssp = None
        ind = mt["indicator"]
        sind = None
        o = None

        prelist.append(
            [
                country,
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
                "Rank",
                rank,
                etl,
            ]
        )
        prelist.append(
            [
                country,
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
                "Score",
                score,
                etl,
            ]
        )
    dm = pd.DataFrame(prelist, columns=columns)
    years = dm["year"].unique()
    for year in years:
        print(f"- year {year} filter")
        save_filename = f"{mi}_{year}_{now.strftime('%Y%m%d%H%M%S')}"
        tm = dm[dm["year"] == year]
        tm.reindex()
        tm.index += 1
        print(f"- year {year} save file")
        tm.to_csv(os.path.join(outputDir, f"{save_filename}.csv"), index_label="id")
        print(f"- year {year} save file end")


####################################################################################

default_args = {
    'owner': 'ETDA',
    'start_date': datetime(2022, 3, 31),
    'schedule_interval': None,
}

dag = DAG('WDCR',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)


def store_to_hdfs(**kwargs):
    hdfs = PyWebHdfsClient(host=Variable.get("hdfs_host"),
                           port=Variable.get("hdfs_port"), user_name=Variable.get("hdfs_username"))
    my_dir = kwargs['directory']
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    path = "/opt/airflow/dags/output/wdcr/data"

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
    index_name = "World Digital Competitiveness Ranking (WDCR)"
    smtp_server = Variable.get("smtp_host")
    port = Variable.get("smtp_port")
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
        op_kwargs={'directory': '/data/raw/index_dashboard/Global/WDCR'},
    )

    load_to_hdfs_processed_zone = PythonOperator(
        task_id='load_to_hdfs_processed_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/data/processed/index_dashboard/Global/WDCR'},
    )

    clean_up_output = BashOperator(
        task_id='clean_up_output',
        bash_command='rm -f /opt/airflow/dags/output/wdcr/data/*.csv && rm -f /opt/airflow/dags/output/wdcr/raw/*.csv',
    )

    send_email = PythonOperator(
        task_id='send_email',
        python_callable=send_mail,
    )

load_data_source >> extract_transform >> load_to_hdfs_raw_zone >> load_to_hdfs_processed_zone >> clean_up_output >> send_email
