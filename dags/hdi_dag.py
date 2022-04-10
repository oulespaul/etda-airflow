from playwright.sync_api import sync_playwright
from pprint import pprint
from pywebhdfs.webhdfs import PyWebHdfsClient
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.models import Variable
import smtplib
import ssl
import pytz
import requests
import json
import pandas as pd
import os

downloadLink = "http://hdr.undp.org/sites/all/themes/hdr_theme/js/bars.json"
outputRawDir = "/opt/airflow/dags/output/hdi/raw"
outputDir = "/opt/airflow/dags/output/hdi/data"

if not os.path.exists(outputDir):
    os.makedirs(outputDir, exist_ok=True)
if not os.path.exists(outputRawDir):
    os.makedirs(outputRawDir, exist_ok=True)


####################################################################################

ids = [
    ["hdi", 137506, ""],
    ["life_at_birth", 69206, "1.1.1"],
    ["expect_years_school", 69706, "2.1.1"],
    ["mean_years_school", 103006, "2.1.2"],
    ["gross_nation", 195706, "3.1.1"],
]

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
    "1": {"pillar": "Long and healthy life"},
    "2": {"pillar": "Knowledge"},
    "3": {"pillar": "A decent standard of living"},
    "1.1": {"pillar": "Long and healthy life", "sub-pillar": "Life expectancy index"},
    "1.1.1": {
        "pillar": "Long and healthy life",
        "sub-pillar": "Life expectancy index",
        "indicator": "Life expectancy at birth",
    },
    "2.1": {"pillar": "Knowledge", "sub-pillar": "Education index"},
    "2.1.1": {
        "pillar": "Knowledge",
        "sub-pillar": "Education index",
        "indicator": "Expected years of schooling",
    },
    "2.1.2": {
        "pillar": "Knowledge",
        "sub-pillar": "Education index",
        "indicator": "Mean years of schooling",
    },
    "3.1": {"pillar": "A decent standard of living", "sub-pillar": "GNI index"},
    "3.1.1": {
        "pillar": "A decent standard of living",
        "sub-pillar": "GNI index",
        "indicator": "GNI per capita (PPP$)",
    },
}


####################################################################################


def ingress_data():
    print("- Start Download")
    filename = os.path.join(outputRawDir, "raw.json")
    print("- Try look in local")
    isExist = os.path.isfile(filename)
    if isExist:
        print("- File is exist")
    else:
        print("- File not find")
        print("- Start download file")
        rawJson = requests.get(downloadLink).json()
        with open(filename, "w+", encoding="utf8") as f:
            json.dump(rawJson, f)
            print("- Save file success")


####################################################################################


def pivotData(raw: pd.DataFrame, id: int) -> pd.DataFrame:
    return raw[raw["id"] == id].fillna(0)


def get_by_id(id: int):
    for i in ids:
        if i[1] == id:
            return i[2]


def extract_transform():
    print("- Start transform")
    print("- Load raw file")
    print("- Step 1 : prepare")
    filename = os.path.join(outputRawDir, "raw.json")
    fp = open(filename, "r", encoding="utf8")
    raw = json.load(fp)
    fp.close()
    raw_df = pd.DataFrame(raw)
    temp = pd.DataFrame()
    for [name, id, _] in ids:
        print(f"- Start {name}")
        data = pivotData(raw_df, id)
        temp = pd.concat([temp, data])
    temp = temp.drop(columns=["interval"])
    temp = temp.sort_values(by=["year", "country", "indicator"])
    temp["rank"] = temp.groupby(by=["year", "id"])["value"].rank(
        ascending=False, method="max"
    )

    mi = "HDI"
    org = "UNITED NATIONS DEVELOPMENT PROGRAMME"
    index = "Human Development Index"
    now = datetime.now()
    etl = now.strftime("%d/%m/%Y %H:%M")
    prelist = []
    for tmp in temp.values.tolist():
        [_, _, country, year, value, id, rank] = tmp
        dd = get_by_id(id)
        d = meta[dd] if dd in meta else {}
        si = d["sub-index"] if "sub-index" in d else None
        p = d["pillar"] if "pillar" in d else None
        sp = d["sub-pillar"] if "sub-pillar" in d else None
        ssp = None
        ind = d["indicator"] if "indicator" in d else None
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
                value,
                etl,
            ]
        )

    dm = pd.DataFrame(prelist, columns=columns)
    years = dm["year"].unique()

    for year in years:
        print(f"- Step 2 : year {year} filter")
        save_filename = f"{mi}_{year}_{now.strftime('%Y%m%d%H%M%S')}"
        tm = dm[dm["year"] == year]
        tm.reindex()
        tm.index += 1
        print(f"- Step 2 : year {year} save file")
        tm.to_csv(os.path.join(
            outputDir, f"{save_filename}.csv"), index_label="id")

        print(f"- Step 2 : year {year} save file end")


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


dag = DAG("hdi", default_args=default_args, catchup=False)


def store_to_hdfs(**kwargs):
    hdfs = PyWebHdfsClient(host='vm002namenode.aml.etda.local',
                           port='50070', user_name='hdfs')
    my_dir = kwargs['directory']
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    path = "/opt/airflow/dags/output/hdi/data"

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
    index_name = "Human Development Index (HDI)"
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
        task_id="load_data_source", python_callable=ingress_data
    )

    extract_transform = PythonOperator(
        task_id="extract_transform",
        python_callable=extract_transform,
    )

    load_to_hdfs_raw_zone = PythonOperator(
        task_id='load_to_hdfs_raw_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/raw/index_dashboard/Global/HDI'},
    )

    load_to_hdfs_processed_zone = PythonOperator(
        task_id='load_to_hdfs_processed_zone',
        python_callable=store_to_hdfs,
        op_kwargs={'directory': '/processed/index_dashboard/Global/HDI'},
    )

    clean_up_output = BashOperator(
        task_id='clean_up_output',
        bash_command='rm -f /opt/airflow/dags/output/hdi/data/*.csv && rm -f /opt/airflow/dags/output/hdi/raw/*.csv',
    )

    send_email = PythonOperator(
        task_id='send_email',
        python_callable=send_mail,
    )

load_data_source >> extract_transform >> load_to_hdfs_raw_zone >> load_to_hdfs_processed_zone >> clean_up_output >> send_email
