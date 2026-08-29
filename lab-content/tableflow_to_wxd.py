"""
LineageBridge Demo - Confluent Tableflow -> Native Iceberg (watsonx.data)
=========================================================================
Reads the 3 Confluent Tableflow tables (via Iceberg REST catalog) and
writes them as NATIVE Iceberg tables into watsonx.data's IBM COS catalog,
so Presto / watsonx BI can query them.

    Confluent Tableflow (Iceberg REST, Confluent-managed S3)
        --> [THIS JOB] -->
    iceberg_catalog.lineage.*  (native tables on IBM COS)
        --> Presto --> watsonx BI

Tables bridged:
    lineage_bridge.orders_v2       -> orders_v2
    lineage_bridge.customers_v2    -> customers_v2
    lineage_bridge.order_stats     -> order_stats

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LAB STEP — fill in your values before uploading to COS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Get values from your terraform output:

  cd terraform
  terraform output -json

  ENV_ID       <- environment_id
  CLUSTER_ID   <- kafka_cluster_id
  TABLEFLOW_APIKEY  <- tableflow_api_key_id
  TABLEFLOW_SECRET  <- tableflow_api_key_secret

  ORG_ID is always: efabb4b6-83b0-4d06-943e-e7127050d10e
  REGION is always: us-east-1

Fill in the 4 constants marked FILL_IN below, then upload this file to COS
and submit via the watsonx.data Spark engine.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

RUN (watsonx.data console):
  1. Fill in the FILL_IN values below and save this file.
  2. Upload to COS: s3a://bucket-lab-2973/spark/tableflow_to_wxd.py
  3. Infrastructure manager -> Spark engine -> Applications -> Create application
       Application type : Python
       Application path : s3a://bucket-lab-2973/spark/tableflow_to_wxd.py
       Spark version    : 3.5
       Spark configuration properties:
         spark.hadoop.wxd.apiKey             = Basic <base64 of ibmlhapikey_<userid>:<apikey>>
         spark.hadoop.fs.s3a.endpoint.region = us-east-1
         spark.gluten.sql.columnar.batchscan = false
         spark.gluten.sql.columnar.filescan  = false
  4. Submit. On success: iceberg_catalog.lineage.* is queryable in Presto / watsonx BI.

Notes learned the hard way:
  - Gluten/Velox's native S3 reader can't use Tableflow's remote-signed
    (vended) credentials -> Iceberg scan must fall back to the JVM reader
    (the two gluten configs above).
  - The native S3 reader needs the region set explicitly, or you get HTTP 301.
  - Tableflow table names include the namespace prefix (lineage_bridge.orders_v2)
    as a single identifier inside the cluster namespace backtick.
"""
from pyspark.sql import SparkSession

# --- Confluent Tableflow (source) -------------------------------------------
# ┌─ FILL IN your values from: cd terraform && terraform output -json ────────┐
REGION           = "us-east-1"                              # unchanged
ORG_ID           = "efabb4b6-83b0-4d06-943e-e7127050d10e"  # unchanged
ENV_ID           = "FILL_IN"   # terraform output: environment_id
CLUSTER_ID       = "FILL_IN"   # terraform output: kafka_cluster_id
TABLEFLOW_APIKEY = "FILL_IN"   # terraform output: tableflow_api_key_id
TABLEFLOW_SECRET = "FILL_IN"   # terraform output: tableflow_api_key_secret
# └───────────────────────────────────────────────────────────────────────────┘

# --- watsonx.data native catalog (destination) ------------------------------
DEST_CATALOG = "iceberg_catalog"     # your Iceberg catalog (Spark + Presto engines)
DEST_SCHEMA  = "lineage"             # created if missing
DEST_BUCKET  = "bucket-lab-2973"     # COS bucket behind DEST_CATALOG

# Tableflow source tables -> columns to keep (drops internal metadata cols).
# Source path: tableflow.`<CLUSTER_ID>`.`lineage_bridge.<table>`
# Dest  path:  iceberg_catalog.lineage.<table>
TABLES = {
    "lineage_bridge.orders_v2": [
        "order_id", "customer_id", "product_name",
        "quantity", "price", "order_status", "created_at",
    ],
    "lineage_bridge.customers_v2": [
        "customer_id", "name", "email", "country", "signup_date",
    ],
    "lineage_bridge.order_stats": [
        "order_status", "order_count", "total_quantity",
        "window_start", "window_end",
    ],
}

REST_URI = (
    f"https://tableflow.{REGION}.aws.confluent.cloud/iceberg/catalog/"
    f"organizations/{ORG_ID}/environments/{ENV_ID}"
)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("tableflow-to-wxd-lineage")
        .enableHiveSupport()
        # Source: Confluent Tableflow Iceberg REST catalog, aliased 'tableflow'.
        .config("spark.sql.catalog.tableflow", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.tableflow.type", "rest")
        .config("spark.sql.catalog.tableflow.uri", REST_URI)
        .config("spark.sql.catalog.tableflow.credential", f"{TABLEFLOW_APIKEY}:{TABLEFLOW_SECRET}")
        .config("spark.sql.catalog.tableflow.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.tableflow.rest-metrics-reporting-enabled", "false")
        .config("spark.sql.catalog.tableflow.s3.remote-signing-enabled", "true")
        # Explicitly request the table-scoped signing token on every loadTable
        # call. Without this header Confluent may not return a per-table
        # token, and the signer then rejects requests made with the
        # catalog-level OAuth token ("not authorized to sign the request").
        .config("spark.sql.catalog.tableflow.header.X-Iceberg-Access-Delegation", "remote-signing")
        .config("spark.sql.catalog.tableflow.client.region", REGION)
        .config("spark.sql.catalog.tableflow.s3.region", REGION)
        # Force the regional S3 hostname (bucket.s3.<region>.amazonaws.com).
        # Without this the Iceberg AWS S3 client builds requests against the
        # global bucket.s3.amazonaws.com endpoint, which is the URL Confluent's
        # signer actually rejects with "not authorized to sign the request" --
        # diagnose-tableflow-read.py's working requests always used the
        # regional hostname explicitly.
        .config("spark.sql.catalog.tableflow.s3.endpoint", f"https://s3.{REGION}.amazonaws.com")
        # Native S3 reader region hints (avoid HTTP 301).
        .config("spark.hadoop.fs.s3a.endpoint.region", REGION)
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{REGION}.amazonaws.com")
        # Force Iceberg scan onto the JVM reader (Velox can't remote-sign).
        .config("spark.gluten.sql.columnar.batchscan", "false")
        .config("spark.gluten.sql.columnar.filescan", "false")
        .getOrCreate()
    )


def bridge_table(spark: SparkSession, table: str, cols: list[str]) -> None:
    # table is e.g. "lineage_bridge.orders_v2" — backtick the whole name
    src = f"tableflow.`{CLUSTER_ID}`.`{table}`"
    # dest uses only the part after the dot as the table name
    dest_table = table.split(".")[-1]
    dst = f"{DEST_CATALOG}.{DEST_SCHEMA}.{dest_table}"
    col_list = ", ".join(cols)

    print(f">>> Bridging {src} -> {dst}")
    df = spark.sql(f"SELECT {col_list} FROM {src}")

    # Full overwrite each run keeps the native table in sync with Tableflow.
    df.writeTo(dst).using("iceberg").createOrReplace()

    count = spark.sql(f"SELECT COUNT(*) AS c FROM {dst}").collect()[0]["c"]
    print(f"    wrote {count} rows to {dst}")


def diagnose(spark: SparkSession) -> bool:
    """
    Fast ACL/connectivity check before attempting any writes.

    For each Tableflow table:
      1. Runs SELECT COUNT(*) — tests catalog visibility + S3 read ACLs.
      2. Prints the count on success or the exact error on failure.

    Returns True only if ALL tables pass. If any fail, check:
      - Confluent Cloud console → cluster → API keys / RBAC
      - Service account behind TABLEFLOW_APIKEY needs DeveloperRead
        (or equivalent ACL Read) scoped to each topic, not just cluster-wide.
    """
    print("=" * 60)
    print("DIAGNOSTIC: testing Tableflow read access per table")
    print("=" * 60)
    all_ok = True
    for table in TABLES:
        src = f"tableflow.`{CLUSTER_ID}`.`{table}`"
        try:
            count = spark.sql(f"SELECT COUNT(*) AS c FROM {src}").collect()[0]["c"]
            print(f"  OK  {src}  ({count} rows)")
        except Exception as e:
            print(f"  FAIL {src}")
            print(f"       {type(e).__name__}: {e}")
            print(f"       -> Check DeveloperRead ACL on topic '{table}' for this service account")
            all_ok = False
    print("=" * 60)
    return all_ok


def main():
    spark = build_spark()

    if not diagnose(spark):
        print("ABORTING: fix ACLs above before running the full bridge job.")
        spark.stop()
        return

    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {DEST_CATALOG}.{DEST_SCHEMA} "
        f"LOCATION 's3a://{DEST_BUCKET}/{DEST_SCHEMA}/'"
    )

    for table, cols in TABLES.items():
        bridge_table(spark, table, cols)

    print(">>> Done. Query the native tables in Presto / watsonx BI:")
    for table in TABLES:
        dest_table = table.split(".")[-1]
        print(f"      SELECT * FROM {DEST_CATALOG}.{DEST_SCHEMA}.{dest_table} LIMIT 20;")

    spark.stop()


if __name__ == "__main__":
    main()
