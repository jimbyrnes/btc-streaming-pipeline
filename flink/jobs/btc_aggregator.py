import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS_INTERNAL", "kafka:29092")
SOURCE_TOPIC = os.environ.get("KAFKA_TOPIC_RAW", "btc_trades_raw")
SINK_TOPIC = os.environ.get("KAFKA_TOPIC_ALERTS", "btc_alerts")

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
t_env = StreamTableEnvironment.create(env)

t_env.execute_sql(f"""
    CREATE TABLE btc_trades (
        exchange  STRING,
        symbol    STRING,
        price     DOUBLE,
        quantity  DOUBLE,
        `timestamp` STRING,
        proc_time AS PROCTIME()
    ) WITH (
        'connector'                     = 'kafka',
        'topic'                         = '{SOURCE_TOPIC}',
        'properties.bootstrap.servers'  = '{KAFKA_SERVERS}',
        'properties.group.id'           = 'flink-btc-source',
        'scan.startup.mode'             = 'latest-offset',
        'format'                        = 'json'
    )
""")

t_env.execute_sql(f"""
    CREATE TABLE btc_alerts (
        window_start STRING,
        window_end   STRING,
        trade_count  BIGINT,
        avg_price    DOUBLE,
        total_volume DOUBLE
    ) WITH (
        'connector'                     = 'kafka',
        'topic'                         = '{SINK_TOPIC}',
        'properties.bootstrap.servers'  = '{KAFKA_SERVERS}',
        'format'                        = 'json'
    )
""")

print(f"Flink job started — windowing '{SOURCE_TOPIC}' → '{SINK_TOPIC}' ({KAFKA_SERVERS})")

t_env.execute_sql("""
    INSERT INTO btc_alerts
    SELECT
        CAST(window_start AS STRING) AS window_start,
        CAST(window_end   AS STRING) AS window_end,
        COUNT(*)           AS trade_count,
        ROUND(AVG(price),     2) AS avg_price,
        ROUND(SUM(quantity),  6) AS total_volume
    FROM TABLE(
        TUMBLE(TABLE btc_trades, DESCRIPTOR(proc_time), INTERVAL '10' SECONDS)
    )
    GROUP BY window_start, window_end
""").wait()
