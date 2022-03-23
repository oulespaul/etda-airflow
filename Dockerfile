FROM apache/airflow:2.1.0-python3.7
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --user -r /tmp/requirements.txt
RUN python -m playwright install

USER root

RUN apt-get update && apt-get -y install libnss3 libatk-bridge2.0-0 libdrm-dev libxkbcommon-dev libgbm-dev libasound-dev libatspi2.0-0 libxshmfence-dev
RUN apt-get install -y poppler-utils

# #=========
# # Firefox
# #=========
ARG FIREFOX_VERSION=94.0
RUN apt-get update                             \
   && apt-get install -y --no-install-recommends \
   ca-certificates curl firefox-esr           \
   && rm -fr /var/lib/apt/lists/*                \
   && curl -L https://github.com/mozilla/geckodriver/releases/download/v0.30.0/geckodriver-v0.30.0-linux64.tar.gz | tar xz -C /usr/local/bin

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow
