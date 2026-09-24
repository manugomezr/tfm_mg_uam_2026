from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from src.pipeline import run_pipeline, run_baseline_pipeline
import logging
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

logger = logging.getLogger(__name__)

logger.info("Inicializando pipeline de data contracts")
builder = SparkSession.builder \
    .appName("TFM_mg_uam") \
    .master("local[2]") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.python.worker.faulthandler.enabled", "true")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

logger.info(f"SparkSession creada")

# Caso 0: registros nulos o inválidos y reglas de calidad
logger.info(f"----------- Pipeline de registros nulos -----------")
run_pipeline(
    spark,
    "data/landing/2021-09/yellow_tripdata_2021-09_nulls.csv",
    "contracts/drafts/draft_contract_yellow_tripdata_2021-09.yaml",
    "batch_2021_09_nulls"
)

# Caso 1: reglas de calidad (max_null_rate)
logger.info(f"----------- Pipeline de reglas de calidad -----------")
run_pipeline(
    spark,
    "data/landing/2021-09/yellow_tripdata_2021-09_nulls_quality_rule.csv",
    "contracts/drafts/draft_contract_yellow_tripdata_2021-09.yaml",
    "batch_2021_09_max_null_rate"
)


# Caso 2: incumplimiento de reglas de tabla o lote (min_row_count)
logger.info(f"----------- Pipeline de violacion de reglas de tabla/lote -----------")
run_pipeline(
    spark,
    "data/landing/2023-09/yellow_tripdata_2023-09.csv",
    "contracts/drafts/draft_contract_yellow_tripdata_2021-09.yaml",
    "batch_2023_09_min_row_count"
)

# Caso 3: schema drift, enforcement level = 3
logger.info(f"----------- Pipeline de schema drift Pipeline de enforcement level = 3, descarte de lote 2025, contrato 2021 -----------")
run_pipeline(
   spark,
    "data/landing/2025-09/yellow_tripdata_2025-09.csv",
    "contracts/drafts/draft_contract_yellow_tripdata_2021-09_enf_lvl_3.yaml",
    "batch_2025-09_schema_drift",
    infer_schema=True
)

## Caso 5:  Baseline sin contrato ni validacion
logger.info(f"----------- Pipeline base (sin contrato) -----------")
run_baseline_pipeline(
    spark,
    "data/landing/2025-09/yellow_tripdata_2025-09.csv",
    batch_id="baseline_2025-09"
)

# Caso 6: nuevo contrato con drifted schema

logger.info(f"----------- Pipeline nuevo contrato con schema modificado -----------")
run_pipeline(
    spark,
    "data/landing/2025-09/yellow_tripdata_2025-09.csv",
    "contracts/drafts/draft_contract_yellow_tripdata_2025-09.yaml",
    "batch_2025_09_new_contract",
    cert_path="data/certified_2025-09/",
    quarantine_path="data/quarantine_2025-09/"
)

logger.info(f"Pipeline finished")

logger.info(f"Consulta de tabla Delta de observability")
df = spark.read.format("delta").load("data/observability")
df.coalesce(1).write.format("parquet").mode("overwrite").save("data/observability_parquet/")
df.show(20)

logger.info(f"Consulta de tabla Delta de quarantine")
df = spark.read.format("delta").load("data/quarantine")
df.coalesce(1).write.format("parquet").mode("overwrite").save("data/quarantine_parquet/")
df.show()

logger.info(f"Consulta de tabla Delta certified")
df = spark.read.format("delta").load("data/certified")
df.show()


logger.info("END")