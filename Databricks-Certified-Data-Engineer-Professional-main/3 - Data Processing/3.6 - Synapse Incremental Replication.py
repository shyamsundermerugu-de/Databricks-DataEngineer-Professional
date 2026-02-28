# Databricks notebook source
# MAGIC %md
# MAGIC ## Incremental replication from Synapse `admin.jobStatus`
# MAGIC
# MAGIC - Reads only `jobStage in ('preStage', 'landing')`
# MAGIC - Loads incrementally based on a watermark column
# MAGIC - Deduplicates by business key + latest modification timestamp
# MAGIC - Upserts into a Delta target and exposes the replicated data as a dataframe

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# -----------------------------
# Configuration
# -----------------------------
SYNAPSE_HOST = "<synapse-server>.database.windows.net"
SYNAPSE_PORT = 1433
SYNAPSE_DATABASE = "<synapse-db>"
SYNAPSE_SCHEMA = "admin"
SYNAPSE_TABLE = "jobStatus"
SYNAPSE_USER = dbutils.secrets.get("synapse-scope", "username")
SYNAPSE_PASSWORD = dbutils.secrets.get("synapse-scope", "password")

# Incremental processing configuration
WATERMARK_COL = "lastUpdatedTs"   # Must exist in source table
PRIMARY_KEYS = ["jobId"]          # Replace with the true business key(s)
JOB_STAGE_COL = "jobStage"
JOB_STAGE_FILTER = ["preStage", "landing"]

# Delta targets (persisted state + replicated data)
STATE_TABLE = "ops.jobstatus_incremental_state"
TARGET_TABLE = "ops.jobstatus_replica"

# -----------------------------
# Utility functions
# -----------------------------

def sql_string_list(values: list[str]) -> str:
    """Safely quote string values for SQL IN clause."""
    escaped = [v.replace("'", "''") for v in values]
    return ", ".join([f"'{v}'" for v in escaped])


def get_last_watermark(state_table: str) -> str:
    """Read last successful watermark from state table (or epoch if first run)."""
    if not spark.catalog.tableExists(state_table):
        return "1900-01-01T00:00:00"

    state_df = spark.table(state_table).where(F.col("pipeline_name") == F.lit("jobstatus_replication"))
    if state_df.rdd.isEmpty():
        return "1900-01-01T00:00:00"

    return state_df.agg(F.max("last_watermark").alias("wm")).collect()[0]["wm"]


def persist_watermark(state_table: str, new_watermark: str) -> None:
    """Persist watermark only after successful load."""
    state_update_df = spark.createDataFrame(
        [("jobstatus_replication", new_watermark)],
        ["pipeline_name", "last_watermark"],
    ).withColumn("updated_at", F.current_timestamp())

    if not spark.catalog.tableExists(state_table):
        (
            state_update_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(state_table)
        )
        return

    state_delta = DeltaTable.forName(spark, state_table)
    (
        state_delta.alias("t")
        .merge(state_update_df.alias("s"), "t.pipeline_name = s.pipeline_name")
        .whenMatchedUpdate(set={
            "last_watermark": "s.last_watermark",
            "updated_at": "s.updated_at",
        })
        .whenNotMatchedInsert(values={
            "pipeline_name": "s.pipeline_name",
            "last_watermark": "s.last_watermark",
            "updated_at": "s.updated_at",
        })
        .execute()
    )


# COMMAND ----------

# -----------------------------
# 1) Read source incrementally from Synapse
# -----------------------------
last_watermark = get_last_watermark(STATE_TABLE)
stage_values_sql = sql_string_list(JOB_STAGE_FILTER)

jdbc_url = (
    f"jdbc:sqlserver://{SYNAPSE_HOST}:{SYNAPSE_PORT};"
    f"database={SYNAPSE_DATABASE};encrypt=true;trustServerCertificate=false;loginTimeout=30;"
)

incremental_query = f"""
(
    SELECT *
    FROM {SYNAPSE_SCHEMA}.{SYNAPSE_TABLE}
    WHERE {JOB_STAGE_COL} IN ({stage_values_sql})
      AND {WATERMARK_COL} > '{last_watermark}'
) src
"""

source_incremental_df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("query", incremental_query)
    .option("user", SYNAPSE_USER)
    .option("password", SYNAPSE_PASSWORD)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
)

# COMMAND ----------

# -----------------------------
# 2) Deduplicate incoming batch
#    Keep latest record per business key
# -----------------------------
if source_incremental_df.rdd.isEmpty():
    print("No new rows found for the configured filter and watermark.")
else:
    dedup_window = Window.partitionBy(*PRIMARY_KEYS).orderBy(F.col(WATERMARK_COL).desc())

    source_dedup_df = (
        source_incremental_df
        .withColumn("_rn", F.row_number().over(dedup_window))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

    # -----------------------------
    # 3) Merge into Delta target (idempotent upsert, no duplicates)
    # -----------------------------
    if not spark.catalog.tableExists(TARGET_TABLE):
        (
            source_dedup_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(TARGET_TABLE)
        )
    else:
        target_delta = DeltaTable.forName(spark, TARGET_TABLE)
        merge_condition = " AND ".join([f"t.{c} = s.{c}" for c in PRIMARY_KEYS])

        update_map = {c: f"s.{c}" for c in source_dedup_df.columns}
        insert_map = {c: f"s.{c}" for c in source_dedup_df.columns}

        (
            target_delta.alias("t")
            .merge(source_dedup_df.alias("s"), merge_condition)
            .whenMatchedUpdate(
                condition=f"s.{WATERMARK_COL} >= t.{WATERMARK_COL}",
                set=update_map,
            )
            .whenNotMatchedInsert(values=insert_map)
            .execute()
        )

    # -----------------------------
    # 4) Advance watermark after successful merge
    # -----------------------------
    next_watermark = source_incremental_df.agg(F.max(WATERMARK_COL).alias("wm")).collect()[0]["wm"]
    persist_watermark(STATE_TABLE, str(next_watermark))

# COMMAND ----------

# Replicated dataframe for downstream notebook logic
jobstatus_replica_df = spark.table(TARGET_TABLE)
display(jobstatus_replica_df)
