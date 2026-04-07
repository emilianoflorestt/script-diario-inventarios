import os
import re
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv()


# ============================================================
# Logging
# ============================================================

def setup_logger() -> logging.Logger:
    logger = logging.getLogger("tree-observation-daily-fix")
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(getattr(logging, log_level, logging.INFO))

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(getattr(logging, log_level, logging.INFO))
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


logger = setup_logger()


# ============================================================
# Config
# ============================================================

def get_env_config() -> Dict[str, Any]:
    run_mode = os.getenv("RUN_MODE", "dry-run").strip().lower()
    if run_mode not in {"dry-run", "apply"}:
        raise RuntimeError("RUN_MODE debe ser 'dry-run' o 'apply'")

    tz_name = os.getenv("TZ", "America/Mexico_City").strip()
    timezone = ZoneInfo(tz_name)

    project_id = (os.getenv("PROJECT_ID") or "").strip() or None

    limit_raw = (os.getenv("LIMIT") or "").strip()
    limit = int(limit_raw) if limit_raw else None

    max_updates_raw = (os.getenv("MAX_UPDATES") or "1000").strip()
    max_updates = int(max_updates_raw)

    return {
        "run_mode": run_mode,
        "apply": run_mode == "apply",
        "timezone": timezone,
        "timezone_name": tz_name,
        "project_id": project_id,
        "limit": limit,
        "max_updates": max_updates,
    }


def get_processing_window(timezone: ZoneInfo) -> Tuple[datetime, datetime]:
    """
    Ventana cerrada del último día completo:
      start_window = ayer 00:00:00
      end_window   = hoy  00:00:00

    Si el job corre a las 00:00, esto toma exactamente el día anterior.
    """
    now_local = datetime.now(timezone)
    today_midnight = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    start_window = today_midnight - timedelta(days=1)
    end_window = today_midnight
    return start_window, end_window


# ============================================================
# DB
# ============================================================

def get_mysql_engine() -> Engine:
    host = os.getenv("MYSQL_HOST")
    port = os.getenv("MYSQL_PORT", "3306")
    user = os.getenv("MYSQL_USER")
    raw_password = os.getenv("MYSQL_PASS")
    database = os.getenv("MYSQL_DB")

    if not all([host, user, raw_password, database]):
        raise RuntimeError("Faltan variables de entorno MYSQL_HOST, MYSQL_USER, MYSQL_PASS o MYSQL_DB")

    password = quote_plus(raw_password)
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"
    return create_engine(url, pool_pre_ping=True, future=True)


# ============================================================
# Helpers
# ============================================================

def normalize_initials(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip().upper()
    cleaned = cleaned.replace(".", "")
    cleaned = re.sub(r"\s+", "", cleaned)
    return cleaned or None


def derive_size_class(tree_number: Optional[str]) -> Optional[str]:
    if tree_number is None:
        return None

    value = tree_number.strip()
    if not value:
        return None

    if re.fullmatch(r"\d+", value):
        return "Árboles"

    if re.fullmatch(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]+", value):
        return "Arbolitos"

    return None


def has_value(v: Any) -> bool:
    return v is not None and str(v).strip() != ""


def is_baseline(period_type: Optional[str]) -> bool:
    if not period_type:
        return False
    return period_type.strip().lower() == "línea base".lower()


def pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


# ============================================================
# Fetch candidatos
# ============================================================

def fetch_candidates(
    engine: Engine,
    start_window: datetime,
    end_window: datetime,
    project_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    where_clauses = [
        """
        (
            (o.created IS NOT NULL AND o.created >= :start_window AND o.created < :end_window)
            OR
            (o.updated IS NOT NULL AND o.updated >= :start_window AND o.updated < :end_window)
        )
        """
    ]
    params: Dict[str, Any] = {
        "start_window": start_window,
        "end_window": end_window,
    }

    if project_id:
        where_clauses.append("o.project_id = :project_id")
        params["project_id"] = project_id

    limit_clause = ""
    if limit:
        limit_clause = " LIMIT :limit"
        params["limit"] = limit

    sql = text(f"""
        SELECT
            -- tree_observations
            o.id AS observation_id,
            o.tree_id,
            o.project_id,
            o.period_id,
            o.site_visit_id,
            o.observed_species_id,
            o.species_differs,
            o.species_notes,
            o.species_reference,
            o.defect_low,
            o.defect_mid,
            o.defect_top,
            o.center_reference_distance_m AS obs_center_distance,
            o.center_reference_azimuth_deg AS obs_center_azimuth,
            o.visit_date,
            o.created AS observation_created,
            o.updated AS observation_updated,

            -- trees
            t.id AS tree_pk,
            t.tree_number,
            t.size_class AS tree_size_class,
            t.initial_species_id,
            t.is_center_reference,
            t.center_reference_distance_m AS tree_center_distance,
            t.center_reference_azimuth_deg AS tree_center_azimuth,

            -- site_visits
            sv.id AS sv_id,
            sv.leader_initials,
            sv.technician_1_initials,
            sv.technician_2_initials,
            sv.technician_3_initials,
            sv.technician_4_initials,
            sv.technician_5_initials,

            -- measurement_periods
            mp.id AS mp_id,
            mp.type AS measurement_period_type

        FROM tree_observations o
        INNER JOIN trees t
            ON t.id = o.tree_id
        LEFT JOIN site_visits sv
            ON sv.id = o.site_visit_id
        LEFT JOIN measurement_periods mp
            ON mp.id = o.period_id
        WHERE {" AND ".join(where_clauses)}
        ORDER BY COALESCE(o.updated, o.created) DESC
        {limit_clause}
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql, params).mappings().all()

    return [dict(r) for r in rows]


# ============================================================
# Reglas de negocio
# ============================================================

def build_tree_observation_updates(row: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    updates: Dict[str, Any] = {}
    reasons: List[str] = []

    for col in ("defect_low", "defect_mid", "defect_top"):
        if row.get(col) is None:
            updates[col] = 0
            reasons.append(f"{col}: NULL -> 0")

    return updates, reasons


def build_tree_updates(row: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    updates: Dict[str, Any] = {}
    reasons: List[str] = []

    obs_dist = row.get("obs_center_distance")
    obs_az = row.get("obs_center_azimuth")
    tree_dist = row.get("tree_center_distance")
    tree_az = row.get("tree_center_azimuth")
    tree_is_center = int(row.get("is_center_reference") or 0)
    tree_number = row.get("tree_number")
    current_size_class = row.get("tree_size_class")
    current_initial_species_id = row.get("initial_species_id")
    observed_species_id = row.get("observed_species_id")

    # Siempre: copiar center reference si observación trae valor
    if has_value(obs_dist) and obs_dist != tree_dist:
        updates["center_reference_distance_m"] = obs_dist
        reasons.append(f"center_reference_distance_m: {tree_dist} -> {obs_dist}")

    if has_value(obs_az) and obs_az != tree_az:
        updates["center_reference_azimuth_deg"] = obs_az
        reasons.append(f"center_reference_azimuth_deg: {tree_az} -> {obs_az}")

    # Si existen ambas referencias, marcar centro
    final_dist = updates.get("center_reference_distance_m", tree_dist)
    final_az = updates.get("center_reference_azimuth_deg", tree_az)
    if has_value(final_dist) and has_value(final_az) and tree_is_center != 1:
        updates["is_center_reference"] = 1
        reasons.append("is_center_reference: 0 -> 1")

    # Reglas solo línea base
    if is_baseline(row.get("measurement_period_type")):
        derived_size = derive_size_class(tree_number)
        if derived_size and derived_size != current_size_class:
            updates["size_class"] = derived_size
            reasons.append(f"size_class: {current_size_class} -> {derived_size}")

        # Si es línea base, pasar species observada a initial_species_id
        if has_value(observed_species_id) and observed_species_id != current_initial_species_id:
            updates["initial_species_id"] = observed_species_id
            reasons.append(f"initial_species_id: {current_initial_species_id} -> {observed_species_id}")

    return updates, reasons


def build_site_visit_updates(row: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    updates: Dict[str, Any] = {}
    reasons: List[str] = []

    initials_cols = [
        "leader_initials",
        "technician_1_initials",
        "technician_2_initials",
        "technician_3_initials",
        "technician_4_initials",
        "technician_5_initials",
    ]

    for col in initials_cols:
        original = row.get(col)
        normalized = normalize_initials(original)

        if original is not None and normalized is not None and original != normalized:
            updates[col] = normalized
            reasons.append(f"{col}: '{original}' -> '{normalized}'")

    return updates, reasons


# ============================================================
# Consolidación
# ============================================================

def merge_payloads(
    target: Dict[str, Dict[str, Any]],
    row_id: str,
    new_payload: Dict[str, Any],
) -> None:
    if row_id not in target:
        target[row_id] = {}

    for k, v in new_payload.items():
        target[row_id][k] = v


# ============================================================
# Persistencia
# ============================================================

def update_row(conn, table: str, row_id_field: str, row_id_value: str, payload: Dict[str, Any]) -> None:
    set_clause = ", ".join([f"`{col}` = :{col}" for col in payload.keys()])
    sql = text(f"""
        UPDATE `{table}`
        SET {set_clause}
        WHERE `{row_id_field}` = :row_id
    """)
    params = dict(payload)
    params["row_id"] = row_id_value
    conn.execute(sql, params)


def apply_changes(
    engine: Engine,
    obs_updates: Dict[str, Dict[str, Any]],
    tree_updates: Dict[str, Dict[str, Any]],
    sv_updates: Dict[str, Dict[str, Any]],
    apply: bool,
) -> None:
    if not apply:
        logger.info("RUN_MODE=dry-run: no se persiste ningún cambio.")
        return

    logger.info("RUN_MODE=apply: persistiendo cambios...")
    with engine.begin() as conn:
        for obs_id, payload in obs_updates.items():
            if payload:
                update_row(conn, "tree_observations", "id", obs_id, payload)

        for tree_id, payload in tree_updates.items():
            if payload:
                update_row(conn, "trees", "id", tree_id, payload)

        for sv_id, payload in sv_updates.items():
            if payload:
                update_row(conn, "site_visits", "id", sv_id, payload)


# ============================================================
# Main
# ============================================================

def main():
    config = get_env_config()
    engine = get_mysql_engine()

    start_window, end_window = get_processing_window(config["timezone"])

    logger.info("Iniciando proceso diario")
    logger.info("run_mode=%s", config["run_mode"])
    logger.info("timezone=%s", config["timezone_name"])
    logger.info("start_window=%s", start_window.isoformat())
    logger.info("end_window=%s", end_window.isoformat())
    logger.info("project_id=%s", config["project_id"])
    logger.info("limit=%s", config["limit"])
    logger.info("max_updates=%s", config["max_updates"])

    rows = fetch_candidates(
        engine=engine,
        start_window=start_window,
        end_window=end_window,
        project_id=config["project_id"],
        limit=config["limit"],
    )

    logger.info("Registros candidatos encontrados: %s", len(rows))

    obs_updates: Dict[str, Dict[str, Any]] = {}
    tree_updates: Dict[str, Dict[str, Any]] = {}
    sv_updates: Dict[str, Dict[str, Any]] = {}

    counters = {
        "obs_candidates": 0,
        "tree_candidates": 0,
        "sv_candidates": 0,
    }

    for row in rows:
        observation_id = row["observation_id"]
        tree_id = row["tree_id"]
        sv_id = row.get("sv_id")

        # tree_observations
        obs_payload, obs_reasons = build_tree_observation_updates(row)
        if obs_payload:
            merge_payloads(obs_updates, observation_id, obs_payload)
            counters["obs_candidates"] += 1
            logger.info(
                "[PLAN][tree_observations] id=%s tree_id=%s changes=%s",
                observation_id,
                tree_id,
                pretty_json(obs_reasons),
            )

        # trees
        tree_payload, tree_reasons = build_tree_updates(row)
        if tree_payload:
            merge_payloads(tree_updates, tree_id, tree_payload)
            counters["tree_candidates"] += 1
            logger.info(
                "[PLAN][trees] id=%s source_observation=%s baseline=%s changes=%s",
                tree_id,
                observation_id,
                is_baseline(row.get("measurement_period_type")),
                pretty_json(tree_reasons),
            )

        # site_visits
        if sv_id:
            sv_payload, sv_reasons = build_site_visit_updates(row)
            if sv_payload:
                merge_payloads(sv_updates, sv_id, sv_payload)
                counters["sv_candidates"] += 1
                logger.info(
                    "[PLAN][site_visits] id=%s source_observation=%s changes=%s",
                    sv_id,
                    observation_id,
                    pretty_json(sv_reasons),
                )

    total_unique_updates = len(obs_updates) + len(tree_updates) + len(sv_updates)

    logger.info("Resumen de cambios planeados:")
    logger.info(" - tree_observations únicas a actualizar: %s", len(obs_updates))
    logger.info(" - trees únicas a actualizar: %s", len(tree_updates))
    logger.info(" - site_visits únicas a actualizar: %s", len(sv_updates))
    logger.info(" - total_unique_updates: %s", total_unique_updates)

    if config["apply"] and total_unique_updates > config["max_updates"]:
        raise RuntimeError(
            f"Abortado: total_unique_updates={total_unique_updates} supera MAX_UPDATES={config['max_updates']}"
        )

    apply_changes(
        engine=engine,
        obs_updates=obs_updates,
        tree_updates=tree_updates,
        sv_updates=sv_updates,
        apply=config["apply"],
    )

    logger.info("Proceso finalizado correctamente")


if __name__ == "__main__":
    main()