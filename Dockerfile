FROM apache/airflow:2.1.0
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --user -r /tmp/requirements.txt

USER root

# #=========
# # Firefox
# #=========
ARG FIREFOX_VERSION=94.0
RUN apt-get update                             \
   && apt-get install -y --no-install-recommends \
   ca-certificates curl firefox-esr wget           \
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
