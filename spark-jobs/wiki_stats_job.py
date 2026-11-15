import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:19092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "wikimedia.recentchange")
START_OFFSETS = os.environ.get("START_OFFSETS", "latest")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/checkpoints/wiki_stats_1m")

PG = dict(
    host=os.environ.get("PG_HOST", "postgres"),
    dbname=os.environ.get("PG_DB", "analytics"),
    user=os.environ.get("PG_USER", "analytics"),
    password=os.environ.get("PG_PASSWORD", "analytics"),
    options="-c timezone=UTC",
)

DDL = """
CREATE TABLE IF NOT EXISTS wiki_stats_1m (
    window_start  TIMESTAMPTZ NOT NULL,
    wiki          TEXT        NOT NULL,
    event_type    TEXT        NOT NULL,
    is_bot        BOOLEAN     NOT NULL,
    event_count   BIGINT      NOT NULL,
    bytes_changed BIGINT      NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, wiki, event_type, is_bot)
)
"""

UPSERT = """
INSERT INTO wiki_stats_1m
    (window_start, wiki, event_type, is_bot, event_count, bytes_changed, updated_at)
VALUES %s
ON CONFLICT (window_start, wiki, event_type, is_bot)
DO UPDATE SET event_count   = EXCLUDED.event_count,
              bytes_changed = EXCLUDED.bytes_changed,
              updated_at    = now()
"""

EVENT_SCHEMA = StructType([
    StructField("meta", StructType([
        StructField("id", StringType()),
        StructField("dt", StringType()),
    ])),
    StructField("wiki", StringType()),
    StructField("type", StringType()),
    StructField("bot", BooleanType()),
    StructField("length", StructType([
        StructField("old", LongType()),
        StructField("new", LongType()),
    ])),
])


def parse_events(raw: DataFrame) -> DataFrame:
    """Kafka value (JSON string) -> typed columns. Rows missing required fields are dropped."""
    return (
        raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select(
            F.col("e.meta.dt").cast("timestamp").alias("event_time"),
            F.col("e.wiki").alias("wiki"),
            F.col("e.type").alias("event_type"),
            F.coalesce(F.col("e.bot"), F.lit(False)).alias("is_bot"),
            (F.col("e.length.new") - F.col("e.length.old")).alias("bytes_changed"),
        )
        .where(
            F.col("event_time").isNotNull()
            & F.col("wiki").isNotNull()
            & F.col("event_type").isNotNull()
        )
    )


def aggregate_windows(events: DataFrame) -> DataFrame:
    """1-minute tumbling windows per wiki / event type / bot flag."""
    return (
        events.withWatermark("event_time", "2 minutes")
        .groupBy(F.window("event_time", "1 minute"), "wiki", "event_type", "is_bot")
        .agg(
            F.count("*").alias("event_count"),
            F.coalesce(F.sum("bytes_changed"), F.lit(0)).alias("bytes_changed"),
        )
    )


def write_batch(batch_df: DataFrame, batch_id: int) -> None:
    rows = [
        (r.window.start, r.wiki, r.event_type, r.is_bot, r.event_count, r.bytes_changed)
        for r in batch_df.collect()
    ]
    if not rows:
        return
    conn = psycopg2.connect(**PG)
    try:
        with conn, conn.cursor() as cur:  # `with conn` commits on success
            execute_values(cur, UPSERT, rows, template="(%s,%s,%s,%s,%s,%s,now())")
    finally:
        conn.close()
    print(f"batch {batch_id}: upserted {len(rows)} window rows", flush=True)


def main() -> None:
    conn = psycopg2.connect(**PG)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(DDL)
    finally:
        conn.close()

    spark = (
        SparkSession.builder.appName("wiki-stats-1m")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", START_OFFSETS)
        .option("maxOffsetsPerTrigger", 20000)
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        aggregate_windows(parse_events(raw))
        .writeStream.outputMode("update")
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()