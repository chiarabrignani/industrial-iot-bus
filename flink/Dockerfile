FROM flink:1.20.1

# Installa Python
RUN apt-get update \
    && apt-get install -y python3 python3-pip \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# Installa PyFlink e il driver PostgreSQL
RUN pip3 install apache-flink==1.20.1 psycopg2-binary

# Aggiunge il Kafka Connector
RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar