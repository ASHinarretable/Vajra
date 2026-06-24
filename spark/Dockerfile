FROM apache/spark:3.5.3

USER root

# Postgres JDBC driver — fetched via ADD so we don't depend on curl/wget being
# present in the base image.
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
    /opt/spark/jars/postgresql-42.7.4.jar
RUN chmod 644 /opt/spark/jars/postgresql-42.7.4.jar

COPY jobs /opt/spark/jobs

USER spark
WORKDIR /opt/spark
