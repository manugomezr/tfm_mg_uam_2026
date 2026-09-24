from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, ArrayType, LongType
)
import logging
from datetime import datetime
from src.enforcement import validate_batch, load_contract

logger = logging.getLogger(__name__)

SPARK_TYPE_MAP = {
    "string": StringType(),
    "integer": IntegerType(),
    "long": LongType(),
    "double": DoubleType()
}

def build_spark_schema(contract: dict, include_diagnostic_fields: bool = False) -> StructType:
    fields = [
        StructField(f["name"], SPARK_TYPE_MAP[f["type"]], True if include_diagnostic_fields else f["nullable"] )
        for f in contract["schema"]
    ]
    if include_diagnostic_fields:
        fields.append(StructField("_failed_rule", StringType(), True))
        fields.append(StructField("_contract_name", StringType(), True))
    return StructType(fields)



def build_observability_schema() -> StructType:
    return StructType([
        StructField("batch_id", StringType(), False),
        StructField("contract_version", StringType(), True),
        StructField("enforcement_level", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("total_records", IntegerType(), True),
        StructField("passed_records", IntegerType(), True),
        StructField("failed_records", IntegerType(), True),
        StructField("rejected_batch", BooleanType(), True),
        StructField("errors", ArrayType(StringType()), True),
        StructField("table_violations", StringType(), True),
        StructField("batch_violations", StringType(), True),
    ])

def run_pipeline(
        spark,
        file_path: str,
        contract_path: str,
        batch_id: str,
        cert_path : str = "data/certified/",
        quarantine_path : str = "data/quarantine/",
        infer_schema: bool = False
):

    contract = load_contract(contract_path)
    enforcement_level = contract["contract"]["enforcement_level"]

    read_schema = build_spark_schema(contract)
    if infer_schema:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        for f in contract["schema"]:
            if f["name"] in df.columns:
                df = df.withColumn(f["name"], col(f["name"]).cast(SPARK_TYPE_MAP[f["type"]]))

    else:
        df = spark.read.csv(file_path, header=True, schema=read_schema)
    df = df.na.replace(["Null", "null", "None", "NA", ""], None)
    rows = [row.asDict() for row in df.collect()]

    total = len(rows)

    # Validar
    result = validate_batch(rows, contract)
    valid_rows = result["valid"]
    quarantine_rows = result["quarantine"]

    valid_schema = build_spark_schema(contract, include_diagnostic_fields = False)
    quarantine_schema = build_spark_schema(contract,include_diagnostic_fields = True)

    # Escribir zonas
    if valid_rows:
        spark.createDataFrame(valid_rows,schema=valid_schema).write.format("delta").mode("append").save(cert_path)

    if quarantine_rows and enforcement_level >= 2:
        spark.createDataFrame(quarantine_rows,schema=quarantine_schema).write.format("delta").mode("append").save(quarantine_path)

    obs = [{
        "batch_id": batch_id,
        "contract_version": contract["contract"]["version"],
        "enforcement_level": enforcement_level,
        "timestamp": datetime.now().isoformat(),
        "total_records": total,
        "passed_records": len(valid_rows),
        "failed_records": len(quarantine_rows),
        "rejected_batch": result["rejected_batch"],
        "errors": result["errors"],
        "table_violations": result["table_violations"],
        "batch_violations": result["batch_violations"]
    }]
    spark.createDataFrame(obs, schema=build_observability_schema()).write.format("delta").mode("append").save("data/observability/")

    logger.info(
        f"Batch {batch_id}: {len(valid_rows)} válidos, {len(quarantine_rows)} en cuarentena, rejected_batch={result['rejected_batch']}")

def run_baseline_pipeline(spark, file_path: str, batch_id: str):

    df = spark.read.csv(file_path, header=True, inferSchema=True)
    df.write.format("delta").mode("append").save("data/baseline_certified/")

    logger.info(f"Baseline {batch_id}: {df.count()} registros escritos sin validacion")