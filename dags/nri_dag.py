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
from airflow.models import Variable

outputRawDir = "/opt/airflow/dags/output/nri/raw"
outputDir = "/opt/airflow/dags/output/nri/data"

if not os.path.exists(outputDir):
    os.makedirs(outputDir, exist_ok=True)
if not os.path.exists(outputRawDir):
    os.makedirs(outputRawDir, exist_ok=True)


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

filenames = ["nri_2020.xlsx", "nri_2021.xlsx"]
pillar = {
    "NRI rank": {"Pillar": "", "Sub-Pillar": "", "Indicator": ""},
    "NRI score": {"Pillar": "", "Sub-Pillar": "", "Indicator": ""},
    "Technology": {"Pillar": "Technology", "Sub-Pillar": "", "Indicator": ""},
    "People": {"Pillar": "People", "Sub-Pillar": "", "Indicator": ""},
    "Governance": {"Pillar": "Governance", "Sub-Pillar": "", "Indicator": ""},
    "Impact": {"Pillar": "Impact", "Sub-Pillar": "", "Indicator": ""},
    "Access": {"Pillar": "Technology", "Sub-Pillar": "Access", "Indicator": ""},
    "Content": {"Pillar": "Technology", "Sub-Pillar": "Content", "Indicator": ""},
    "Future Technologies": {
        "Pillar": "Technology",
        "Sub-Pillar": "Future Technologies",
        "Indicator": "",
    },
    "Individuals": {"Pillar": "People", "Sub-Pillar": "Individuals", "Indicator": ""},
    "Businesses": {"Pillar": "People", "Sub-Pillar": "Businesses", "Indicator": ""},
    "Governments": {"Pillar": "People", "Sub-Pillar": "Governments", "Indicator": ""},
    "Trust": {"Pillar": "Governments", "Sub-Pillar": "Trust", "Indicator": ""},
    "Regulation": {
        "Pillar": "Governments",
        "Sub-Pillar": "Regulation",
        "Indicator": "",
    },
    "Inclusion": {"Pillar": "Governments", "Sub-Pillar": "Inclusion", "Indicator": ""},
    "Economy": {"Pillar": "Impact", "Sub-Pillar": "Economy", "Indicator": ""},
    "Quality of Life": {
        "Pillar": "Impact",
        "Sub-Pillar": "Quality of Life",
        "Indicator": "",
    },
    "SDG Contribution": {
        "Pillar": "Impact",
        "Sub-Pillar": "SDG Contribution",
        "Indicator": "",
    },
}

meta = {
    "1": {"pillar": "TECHNOLOGY"},
    "2": {"pillar": "PEOPLE"},
    "3": {"pillar": "GOVERNANCE"},
    "4": {"pillar": "IMPACT"},
    "1.1": {"pillar": "TECHNOLOGY", "sub-pillar": "Access"},
    "1.1.1": {
        "pillar": "TECHNOLOGY",
        "sub-pillar": "Access",
        "indicator": "Mobile tariffs",
    },
    "1.1.2": {
        "pillar": "TECHNOLOGY",
        "sub-pillar": "Access",
        "indicator": "Handset prices",
    },
    "1.1.3": {
        "pillar": "TECHNOLOGY",
        "sub-pillar": "Access",
        "indicator": "Households with internet access",
    },
    "1.1.4": {
        "pillar": "TECHNOLOGY",
        "sub-pillar": "Access",
        "indicator": "SMS sent by population 15-69",
    },
    "1.1.5": {
        "pillar": "TECHNOLOGY",
        "sub-pillar": "Access",
        "indicator": "Population covered by at least a 3G mobile network",
    },
    "1.1.6": {
        "pillar": "TECHNOLOGY",
        "sub-pillar": "Access",
        "indicator": "International Internet bandwidth",
    },
    "1.1.7": {
        "pillar": "TECHNOLOGY",
        "sub-pillar": "Access",
        "indicator": "Internet access in schools",
    },
    "1.2": {"pillar": "TECHNOLOGY", "sub-pillar": "Content"},
    "1.2.1": {
        "pillar": "TECHNOLOGY",
        "sub-pillar": "Content",
        "indicator": "GitHub commits",
    },
    "1.2.2": {
        "pillar": "TECHNOLOGY",
        "sub-pillar": "Content",
        "indicator": "Wikipedia edits",
    },
    "1.2.3": {
        "pillar": "TECHNOLOGY",
        "sub-pillar": "Content",
        "indicator": "Internet domain registrations",
    },
    "1.2.4": {
        "pillar": "TECHNOLOGY",
        "sub-pillar": "Content",
        "indicator": "Mobile apps development",
    },
    "1.2.5": {
        "pillar": "TECHNOLOGY",
        "sub-pillar": "Content",
        "indicator": "AI scientific publications",
    },
    "1.3": {"pillar": "TECHNOLOGY", "sub-pillar": "Future Technologies"},
    "1.3.1": {
        "pillar": "TECHNOLOGY",
        "sub-pillar": "Future Technologies",
        "indicator": "Adoption of emerging technologies",
    },
    "1.3.2": {
        "pillar": "TECHNOLOGY",
        "sub-pillar": "Future Technologies",
        "indicator": "Investment in emerging technologies",
    },
    "1.3.3": {
        "pillar": "TECHNOLOGY",
        "sub-pillar": "Future Technologies",
        "indicator": "Robot density",
    },
    "1.3.4": {
        "pillar": "TECHNOLOGY",
        "sub-pillar": "Future Technologies",
        "indicator": "Computer software spending",
    },
    "1.3.5": {
        "pillar": "TECHNOLOGY",
        "sub-pillar": "Future Technologies",
        "indicator": "Robot density",
    },
    "2.1": {"pillar": "PEOPLE", "sub-pillar": "Individuals"},
    "2.1.1": {
        "pillar": "PEOPLE",
        "sub-pillar": "Individuals",
        "indicator": "Active mobile broadband subscriptions",
    },
    "2.1.2": {
        "pillar": "PEOPLE",
        "sub-pillar": "Individuals",
        "indicator": "ICT skills",
    },
    "2.1.3": {
        "pillar": "PEOPLE",
        "sub-pillar": "Individuals",
        "indicator": "Use of virtual social networks ",
    },
    "2.1.4": {
        "pillar": "PEOPLE",
        "sub-pillar": "Individuals",
        "indicator": "Tertiary enrollment",
    },
    "2.1.5": {
        "pillar": "PEOPLE",
        "sub-pillar": "Individuals",
        "indicator": "Adult literacy rate",
    },
    "2.1.6": {
        "pillar": "PEOPLE",
        "sub-pillar": "Individuals",
        "indicator": "ICT skills",
    },
    "2.2": {"pillar": "PEOPLE", "sub-pillar": "Businesses"},
    "2.2.1": {
        "pillar": "PEOPLE",
        "sub-pillar": "Businesses",
        "indicator": "Firms with website",
    },
    "2.2.2": {
        "pillar": "PEOPLE",
        "sub-pillar": "Businesses",
        "indicator": "GERD financed by business enterprise",
    },
    "2.2.3": {
        "pillar": "PEOPLE",
        "sub-pillar": "Businesses",
        "indicator": "Professionals",
    },
    "2.2.4": {
        "pillar": "PEOPLE",
        "sub-pillar": "Businesses",
        "indicator": "Technicians and associate professionals",
    },
    "2.2.5": {
        "pillar": "PEOPLE",
        "sub-pillar": "Businesses",
        "indicator": "Annual investment in telecommunication services",
    },
    "2.2.6": {
        "pillar": "PEOPLE",
        "sub-pillar": "Businesses",
        "indicator": "GERD performed by business enterprise",
    },
    "2.3": {"pillar": "PEOPLE", "sub-pillar": "Governments"},
    "2.3.1": {
        "pillar": "PEOPLE",
        "sub-pillar": "Governments",
        "indicator": "Government online services",
    },
    "2.3.2": {
        "pillar": "PEOPLE",
        "sub-pillar": "Governments",
        "indicator": "Publication and use of open data",
    },
    "2.3.3": {
        "pillar": "PEOPLE",
        "sub-pillar": "Governments",
        "indicator": "Government promotion of investment in emerging technologies",
    },
    "2.3.4": {
        "pillar": "PEOPLE",
        "sub-pillar": "Governments",
        "indicator": "R&D expenditure by governments and higher education",
    },
    "3.1": {"pillar": "GOVERNANCE", "sub-pillar": "Trust"},
    "3.1.1": {
        "pillar": "GOVERNANCE",
        "sub-pillar": "Trust",
        "indicator": "Secure Internet servers",
    },
    "3.1.2": {
        "pillar": "GOVERNANCE",
        "sub-pillar": "Trust",
        "indicator": "Cybersecurity",
    },
    "3.1.3": {
        "pillar": "GOVERNANCE",
        "sub-pillar": "Trust",
        "indicator": "Online access to financial account",
    },
    "3.1.4": {
        "pillar": "GOVERNANCE",
        "sub-pillar": "Trust",
        "indicator": "Internet shopping",
    },
    "3.2": {"pillar": "GOVERNANCE", "sub-pillar": "Regulation"},
    "3.2.1": {
        "pillar": "GOVERNANCE",
        "sub-pillar": "Regulation",
        "indicator": "Regulatory quality",
    },
    "3.2.2": {
        "pillar": "GOVERNANCE",
        "sub-pillar": "Regulation",
        "indicator": "ICT regulatory environment",
    },
    "3.2.3": {
        "pillar": "GOVERNANCE",
        "sub-pillar": "Regulation",
        "indicator": "Legal framework's adaptability to emerging technologies",
    },
    "3.2.4": {
        "pillar": "GOVERNANCE",
        "sub-pillar": "Regulation",
        "indicator": "E-commerce legislation",
    },
    "3.2.5": {
        "pillar": "GOVERNANCE",
        "sub-pillar": "Regulation",
        "indicator": "Privacy protection by law content",
    },
    "3.2.6": {
        "pillar": "GOVERNANCE",
        "sub-pillar": "Regulation",
        "indicator": "ICT regulatory environment",
    },
    "3.3": {"pillar": "GOVERNANCE", "sub-pillar": "Inclusion"},
    "3.3.1": {
        "pillar": "GOVERNANCE",
        "sub-pillar": "Inclusion",
        "indicator": "E-Participation",
    },
    "3.3.2": {
        "pillar": "GOVERNANCE",
        "sub-pillar": "Inclusion",
        "indicator": "Socioeconomic gap in use of digital payments",
    },
    "3.3.3": {
        "pillar": "GOVERNANCE",
        "sub-pillar": "Inclusion",
        "indicator": "Availability of local online content",
    },
    "3.3.4": {
        "pillar": "GOVERNANCE",
        "sub-pillar": "Inclusion",
        "indicator": "Gender gap in Internet use",
    },
    "3.3.5": {
        "pillar": "GOVERNANCE",
        "sub-pillar": "Inclusion",
        "indicator": "Rural gap in use of digital payments",
    },
    "4.1": {"pillar": "IMPACT", "sub-pillar": "Economy"},
    "4.1.1": {
        "pillar": "IMPACT",
        "sub-pillar": "Economy",
        "indicator": "High-tech and medium-high-tech manufacturing",
    },
    "4.1.2": {
        "pillar": "IMPACT",
        "sub-pillar": "Economy",
        "indicator": "High-tech exports",
    },
    "4.1.3": {
        "pillar": "IMPACT",
        "sub-pillar": "Economy",
        "indicator": "PCT patent applications",
    },
    "4.1.4": {
        "pillar": "IMPACT",
        "sub-pillar": "Economy",
        "indicator": "Growth rate of GDP per person engaged",
    },
    "4.1.5": {
        "pillar": "IMPACT",
        "sub-pillar": "Economy",
        "indicator": "Prevalence of gig economy",
    },
    "4.1.6": {
        "pillar": "IMPACT",
        "sub-pillar": "Economy",
        "indicator": "ICT services exports",
    },
    "4.2": {"pillar": "IMPACT", "sub-pillar": "Quality of Life"},
    "4.2.1": {
        "pillar": "IMPACT",
        "sub-pillar": "Quality of Life",
        "indicator": "Happiness ",
    },
    "4.2.2": {
        "pillar": "IMPACT",
        "sub-pillar": "Quality of Life",
        "indicator": "Freedom to make life choices",
    },
    "4.2.3": {
        "pillar": "IMPACT",
        "sub-pillar": "Quality of Life",
        "indicator": "Income inequality",
    },
    "4.2.4": {
        "pillar": "IMPACT",
        "sub-pillar": "Quality of Life",
        "indicator": "Healthy life expectancy at birth",
    },
    "4.3": {"pillar": "IMPACT", "sub-pillar": "SDG Contribution"},
    "4.3.1": {
        "pillar": "IMPACT",
        "sub-pillar": "SDG Contribution",
        "indicator": "SDG 3: Good Health and Well-Being",
    },
    "4.3.2": {
        "pillar": "IMPACT",
        "sub-pillar": "SDG Contribution",
        "indicator": "SDG 4: Quality Education",
    },
    "4.3.3": {
        "pillar": "IMPACT",
        "sub-pillar": "SDG Contribution",
        "indicator": "Females employed with advanced degrees",
    },
    "4.3.4": {
        "pillar": "IMPACT",
        "sub-pillar": "SDG Contribution",
        "indicator": "SDG 7: Affordable and Clean Energy",
    },
    "4.3.5": {
        "pillar": "IMPACT",
        "sub-pillar": "SDG Contribution",
        "indicator": "SDG 11: Sustainable Cities and Communities",
    },
}


####################################################################################


def ingress_data():
    def load_from_hdfs(filename):
        hdfs = PyWebHdfsClient(host='10.121.101.101',
                               port='50070', user_name='hdfs')
        source_file_byte = '/raw/index_dashboard/File_Upload/NRI/{}'.format(
            filename)

        data_source = hdfs.read_file(source_file_byte)

        with open('/opt/airflow/dags/output/nri/raw/{}'.format(filename), 'wb') as file_out:
            file_out.write(data_source)
            file_out.close()

        pprint("Ingested!")

    for filename in filenames:
        load_from_hdfs(filename)


####################################################################################


def extract_transform():
    mi = "NRI"
    index = "Network Readiness Index"
    org = "World Economic Forum"
    now = datetime.now()
    etl = now.strftime("%d/%m/%Y %H:%M")

    files_list = os.listdir(outputRawDir)
    files_list.sort()

    for filename in files_list:
        if not filename.endswith(".xlsx"):
            return

        year = filename[4:8]
        loadfile = os.path.join(outputRawDir, filename)
        df1 = pd.read_excel(loadfile, header=1)
        df2 = pd.read_excel(
            loadfile, header=2, sheet_name=1).iloc[1:]

        df_rank_score = df1.filter(["Country", "NRI rank", "NRI score"])
        df1 = df1.drop(["Code", "NRI rank", "NRI score"], axis=1).melt(
            id_vars=["Country"], var_name="Name", value_name="Score"
        )
        df1["Score"] = df1["Score"].astype(float)
        df1["Rank"] = df1.groupby(by=["Name"])["Score"].rank(
            "dense", ascending=False)

        df2 = df2.drop("Unnamed: 0", axis=1).melt(
            id_vars=["Unnamed: 1"], var_name="Name", value_name="Score"
        )
        df2["Score"] = df2["Score"].astype(float)
        df2["Rank"] = df2.groupby(by=["Name"])["Score"].rank(
            "dense", ascending=False)

        prelist = []
        for idx in df_rank_score.index:
            item = df_rank_score.loc[idx]
            country = item["Country"]
            rank = item["NRI rank"]
            score = item["NRI score"]
            prelist.append(
                [
                    country,
                    year,
                    mi,
                    org,
                    index,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
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
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    "Score",
                    score,
                    etl,
                ]
            )
        for idx in df1.index:
            item = df1.loc[idx]
            country = item["Country"]
            name = item["Name"]
            rank = item["Rank"]
            score = item["Score"]
            pn = pillar[name]
            si = None
            p = pn["Pillar"]
            sp = pn["Sub-Pillar"]
            ssp = None
            ind = pn["Indicator"]
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
        for idx in df2.index:
            item = df2.loc[idx]
            country = item["Unnamed: 1"]
            name = item["Name"]
            rank = item["Rank"]
            score = item["Score"]
            pn = meta[name]
            si = None
            p = pn["pillar"]
            sp = pn["sub-pillar"]
            ssp = None
            ind = pn["indicator"]
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
        print(f"- Step : year {year} save file")
        save_filename = f"{mi}_{year}_{now.strftime('%Y%m%d%H%M%S')}"
        dm = pd.DataFrame(prelist, columns=columns)
        dm.index += 1
        dm.to_csv(os.path.join(
            outputDir, f"{save_filename}.csv"), index_label="id")

        print(f"- Step : year {year} end")


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


dag = DAG("NRI", default_args=default_args, catchup=False)

def store_to_hdfs(**kwargs):
    hdfs = PyWebHdfsClient(host='10.121.101.101',
                           port='50070', user_name='hdfs')
    my_dir = kwargs['directory']
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    path = "/opt/airflow/dags/output/nri/data"

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
    index_name = "Networked Readiness Index (NRI)"
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
    load_data_source = PythonOperator(
        task_id="load_data_source", 
        python_callable=ingress_data
    )

    extract_transform = PythonOperator(
        task_id="extract_transform",
        python_callable=extract_transform,
    )

    load_to_hdfs_raw_zone = PythonOperator(
        task_id='load_to_hdfs_raw_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/raw/index_dashboard/Global/NRI'},
    )

    load_to_hdfs_processed_zone = PythonOperator(
        task_id='load_to_hdfs_processed_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/processed/index_dashboard/Global/NRI'},
    )

    clean_up_output = BashOperator(
        task_id='clean_up_output',
        bash_command='rm -f /opt/airflow/dags/output/nri/data/*.csv && rm -f /opt/airflow/dags/output/nri/raw/*.csv',
    )

    send_email = PythonOperator(
        task_id='send_email',
        python_callable=send_mail,
    )

load_data_source >> extract_transform >> load_to_hdfs_raw_zone >> load_to_hdfs_processed_zone >> clean_up_output >> send_email
