FROM apache/spark:3.5.3-python3

USER root
RUN pip3 install --no-cache-dir psycopg2-binary==2.9.10 \
 && mkdir -p /checkpoints /ivy /app \
 && chown -R spark:spark /checkpoints /ivy /app
COPY --chown=spark:spark wiki_stats_job.py /app/wiki_stats_job.py

USER spark
WORKDIR /app

ENTRYPOINT ["/opt/spark/bin/spark-submit", \
  "--master", "local[2]", \
  "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3", \
  "--conf", "spark.jars.ivy=/ivy", \
  "--conf", "spark.sql.shuffle.partitions=4", \
  "/app/wiki_stats_job.py"]