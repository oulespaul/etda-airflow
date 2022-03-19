from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import smtplib
import ssl
import pytz

index_name = "ICT Development Index (IDI)"


def send_mail():
    smtp_server = "203.154.120.150"
    port = 25
    email_to = "dac-visualization@etda.or.th"
    email_from = "noreply-data@etda.or.th"
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
        server.sendmail(email_from, email_to, email_string)
        print("sent!")
    except Exception as e:
        print(e)
    finally:
        server.quit()


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

dag = DAG('mail', default_args=default_args, catchup=False)

with dag:
    send_mail = PythonOperator(
        task_id='send_mail',
        python_callable=send_mail,
    )
