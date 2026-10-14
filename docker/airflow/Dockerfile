FROM apache/airflow:2.10.3-python3.11

# PySpark needs a JVM; batch jobs run in Spark local mode inside the Airflow container.
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/jre17
ENV JAVA_HOME=/usr/lib/jvm/jre17

USER airflow
RUN pip install --no-cache-dir pyspark==3.5.3 psycopg2-binary==2.9.10
