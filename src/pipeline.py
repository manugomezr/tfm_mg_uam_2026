from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, TimestampType, ArrayType
)
import logging
from datetime import datetime
from src.enforcement import validate_batch, load_contract

logger = logging.getLogger(__name__)

SPARK_TYPE_MAP = {
    "string": StringType(),
    "integer": IntegerType(),
    "double": DoubleType()
}

def build_spark_schema(contract: dict, include_diagnostic_fields: bool = False) -> StructType:
    fields = [
        StructField(f["name"], SPARK_TYPE_MAP[f["type"]], f["nullable"])
        for f in contract["schema"]
    ]
    if include_diagnostic_fields:
        fields.append(StructField("_failed_rule", StringType(), True))
        fields.append(StructField("_contract_version", StringType(), True))
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
    ])

def run_pipeline(spark, file_path: str, contract_path: str, batch_id: str):

    contract = load_contract(contract_path)
    enforcement_level = contract["contract"]["enforcement_level"]

    read_schema = build_spark_schema(contract)
    df = spark.read.csv(file_path, header=True, schema=read_schema)
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
        spark.createDataFrame(valid_rows,schema=valid_schema).write.format("delta").mode("append").save("data/certified/")

    if quarantine_rows and enforcement_level >= 2:
        spark.createDataFrame(quarantine_rows,schema=quarantine_schema).write.format("delta").mode("append").save("data/quarantine/")

    obs = [{
        "batch_id": batch_id,
        "contract_version": contract["contract"]["version"],
        "enforcement_level": enforcement_level,
        "timestamp": datetime.now().isoformat(),
        "total_records": total,
        "passed_records": len(valid_rows),
        "failed_records": len(quarantine_rows),
        "rejected_batch": result["rejected_batch"],
        "errors": result["errors"]
    }]
    spark.createDataFrame(obs, schema=build_observability_schema()).write.format("delta").mode("append").save("data/observability/")

    logger.info(
        f"Batch {batch_id}: {len(valid_rows)} válidos, {len(quarantine_rows)} en cuarentena, rejected_batch={result['rejected_batch']}")

def run_baseline_pipeline(spark, file_path: str, batch_id: str):

    df = spark.read.csv(file_path, header=True, inferSchema=True)
    df.write.format("delta").mode("append").save("data/baseline_certified/")

    logger.info(f"Baseline {batch_id}: {df.count()} registros escritos sin validacion")