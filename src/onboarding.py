import yaml
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def profile_and_draft_contract(spark, parquet_filepath: str, output_path: str):

    df = spark.read.parquet(parquet_filepath)

    type_map_to_contract = {
        "StringType": "string",
        "IntegerType": "integer",
        "LongType": "long",
        "DoubleType": "double"
    }

    schema_fields = []
    quality_rules = []
    table_quality_rules = []
    total = df.count()
    draft_contract_name = f"draft_contract_{Path(parquet_filepath).stem}.yaml"

    for field in df.schema.fields:
        spark_type = type(field.dataType).__name__
        contract_type = type_map_to_contract.get(spark_type)

        logger.info(f"type {spark_type} mapeado a {contract_type} para el campo {field.name}")

        schema_fields.append({
            "name": field.name,
            "type": contract_type,
            "nullable": field.nullable
        })

        # Creación de reglas de calidad por campo
        # Tasa max de valores nulos
        if contract_type in ("double", "integer"):
            nulls = df.filter(df[field.name].isNull()).count()
            null_rate = round(nulls / total, 4)
            if null_rate > 0:
                quality_rules.append({
                    "field": field.name,
                    "rule": "max_null_rate",
                    "value": min(null_rate, 0.01)  # arbitrario
                })

    # Creacion de reglas de calidad por tabla o contrato
    # Minimo de registros en la tabla
    table_quality_rules.append({
        "rule": "min_row_count",
        "value": 495 # arbitrario
    })

    draft = {
        "contract": {
            "name": str(draft_contract_name),
            "version": "1.0",
            "owner": "tfm_mg_uam",
            "enforcement_level": 2 # 1 sin enforcement, 2 registro sin accion, 3 registro y accion (rechazo o aprobacion de batch)
        },
        "schema": schema_fields,
        "quality": quality_rules,
        "table_quality": table_quality_rules
    }
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(Path(output_path) / draft_contract_name, "w") as f:
        yaml.dump(draft, f, default_flow_style=False, sort_keys=False)

    logger.info(f"Contrato borrador generado en {str(draft_contract_name)}")
