from pydantic import create_model, ValidationError, ConfigDict
from typing import Optional, Literal
import yaml
import logging

logger = logging.getLogger(__name__)

TYPE_MAP_CONTRACT_TO_PYTHON = {
    "string": str,
    "integer": int,
    "double": float
}

def load_contract(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)

def build_dynamic_model(contract: dict):
    """Genera la clase Pydantic en tiempo de ejecución a partir del schema del contrato."""
    fields = {}
    for f in contract["schema"]:
        py_type = TYPE_MAP_CONTRACT_TO_PYTHON[f["type"]]
        if f["nullable"]:
            fields[f["name"]] = (Optional[py_type], None)
        else:
            fields[f["name"]] = (py_type, ...)

    # create_model construye el modelo automaticamente
    extra_mode : Literal["forbid","allow"] = "forbid" if contract["contract"]["enforcement_level"] == 3 else "allow"
    return create_model(
        "ContractModel",
        __config__ = ConfigDict(extra=extra_mode),
        **fields
    )

# Validacion de reglas de contrato definidas en el onboarding
def check_max_null_rate_rules(rows: list, contract: dict) -> dict:
    """Reglas agregadas sobre el batch completo, no por registro."""
    total = len(rows)
    violations = {}
    for rule in contract.get("quality_rules", []):
        if rule["rule"] == "max_null_rate":
            field = rule["field"]
            nulls = sum(1 for r in rows if r.get(field) is None)
            observed_rate = nulls / total if total else 0
            if observed_rate > rule["value"]:
                violations[field] = observed_rate
    return violations

def check_min_row_count_rule(rows: list, contract: dict) -> dict:
    """Regla a nivel de tabla: verifica que el batch no llegó vacío o truncado."""
    total = len(rows)
    violations = {}
    for rule in contract.get("table_quality_rules", []):
        if rule["rule"] == "min_row_count":
            if total < rule["value"]:
                violations["min_row_count"] = {
                    "expected_min": rule["value"],
                    "observed": total
                }
    return violations


def validate_batch(rows: list, contract: dict) -> dict:
    model = build_dynamic_model(contract)
    enforcement_level = contract["contract"]["enforcement_level"]

    valid = []
    quarantine = []
    errors = []
    # Validacion de reglas de calidad de registros
    for i, row in enumerate(rows):

        try:
            model(**row)
            valid.append(row)
        except ValidationError as e:
            errors.extend(f"row: {i}, err: {err['msg']}" for err in e.errors())
            row_errors = [err["msg"] for err in e.errors()]
            row["_failed_rule"] = "; ".join(row_errors)
            row["_contract_version"] = contract["contract"]["version"]
            if enforcement_level >= 2:
                quarantine.append(row)
            else :
                valid.append(row)

    # Validación de reglas de calidad de tabla y contrato
    batch_violations = check_max_null_rate_rules(rows, contract)
    if batch_violations:
        print(f"ALERTA - max_null_rate superado: {batch_violations}")

    table_violations = check_min_row_count_rule(rows, contract)
    rejected_batch = bool(table_violations) and enforcement_level == 3

    if rejected_batch:
        logger.info(f"BLOQUEO - Batch completo rechazado: {table_violations}")
        valid = []

    return {
        "valid": valid,
        "quarantine": quarantine,
        "errors": errors,
        "table_violations": table_violations,
        "rejected_batch": rejected_batch
    }