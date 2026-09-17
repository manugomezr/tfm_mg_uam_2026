from src.onboarding import profile_and_draft_contract
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

logger.info("Inicializando generacion de data contracts")
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

# Proceso de onboarding o generacion de contrato con datos de referencia, se ejecuta una vez

profile_and_draft_contract(
    spark,
    "data/landing/2021-09/yellow_tripdata_2021-09.parquet",
    "contracts/drafts/"
)
logger.info(f"Contrato 2019 generado")

profile_and_draft_contract(
    spark,
    "data/landing/2025-09/yellow_tripdata_2025-09.parquet",
    "contracts/drafts/"
)
logger.info(f"Contratos 2025 generado")
