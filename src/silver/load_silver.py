"""
Load bronze tables into silver tables.
"""

import uuid
import logging
from psycopg2 import sql, extensions
from dataclasses import dataclass

from db import get_db_connection, health_check
from .transform_silver import TRANSFORMS, resolve_effective_snapshot
from .config import SILVER_TABLE_SOURCES
from .quality_silver import run_quality_checks, run_cross_table_checks, persist_quality_results

logger = logging.getLogger(__name__)

@dataclass
class SilverLoadSummary:
    run_id: str
    snapshot_id: str
    tables_loaded: int
    tables_failed: int
    effective_snapshot_id: dict[str, str]


def _register_run(conn:extensions.connection, run_id:str, snapshot_id:str):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion.runs (run_id, snapshot_id, layer, status) 
            VALUES (%s, %s, 'silver', 'started')
            """, (run_id, snapshot_id),
        )
    conn.commit()

def _complete_run(conn:extensions.connection, run_id:str, status:str, error_message:str=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ingestion.runs
            SET status = %s, end_time = NOW(), error_message = %s
            WHERE run_id = %s
            """, (status, error_message, run_id),
        )
    conn.commit()

def _record_lineage(conn: extensions.connection, run_id:str,
    silver_table:str, effective_snapshots: dict[str, str]):

    bronze_sources = SILVER_TABLE_SOURCES[silver_table]
    with conn.cursor() as cur:
        for bronze_table in bronze_sources: 
            cur.execute(
                """
                INSERT INTO ingestion.silver_lineage (run_id, silver_table, bronze_table, effective_snapshot_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (run_id, silver_table, bronze_table) DO UPDATE
                SET effective_snapshot_id = EXCLUDED.effective_snapshot_id
                """, 
                (run_id, silver_table, bronze_table, effective_snapshots[bronze_table]),
            )
    conn.commit()


def _build_query_params(target_snapshot_id:str, run_id:str, 
    silver_table:str, effective_snapshots: dict[str, str]) -> dict:
    """
    Builds a dict of query parameters shared by all the SQL template.
    Includes the target snapshot_id, run_id, and the effective snapshot_id for each bronze table.
    """
    params = {'target_snapshot_id': target_snapshot_id, 'run_id': run_id}
    for bronze_table in SILVER_TABLE_SOURCES[silver_table]:
        params[f"eff_{bronze_table}"] = effective_snapshots[bronze_table]
    return params


### Main load function 

def load(snapshot_id: str = None, run_id: str = None) -> SilverLoadSummary:
    """
    Load all silver tables from bronze for the given snapshot_id.
    """

    if snapshot_id is None:
        from bronze.config import latest_manifest_path
        import json
        path = latest_manifest_path()
        if path is None:
            raise FileNotFoundError('No manifest found, try running bronze pipeline first.')
        manifest = json.loads(path.read_text())
        snapshot_id = manifest['snapshot_id']

    run_id = run_id or str(uuid.uuid4())

    status = health_check()
    if status['status'] != 'healthy':
        raise RuntimeError(f"database is unhealthy: {status}")

    with get_db_connection() as conn:
        # resolve which bronze snapshot each source table should read from
        effective = resolve_effective_snapshot(conn, snapshot_id)
        _register_run(conn, run_id, snapshot_id)

        loaded = []
        failed = []

        for silver_table, transform_sql in TRANSFORMS.items():
            try: 
                params = _build_query_params(snapshot_id, run_id, silver_table, effective)

                # idempotency: clear previous data for this snapshot
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("DELETE FROM {} WHERE _snapshot_id = %s").format(
                            sql.Identifier('silver', silver_table)
                        ), (snapshot_id,),
                    )
                    cur.execute(transform_sql, params)
                    rows = cur.rowcount
                logger.info('silver_table_loaded', extra={'table': silver_table, 'rows': rows})

                # record lineage and run quality checks
                _record_lineage(conn, run_id, silver_table, effective)
                eff_snap = effective[SILVER_TABLE_SOURCES[silver_table][0]]
                dq_results = run_quality_checks(conn, silver_table, snapshot_id, eff_snap)
                persist_quality_results(conn, run_id, dq_results)

                # log failed quality checks
                failed_checks = [r for r in dq_results if not r.passed and r.severity == 'error']
                if failed_checks:
                    logger.warning('silver_dq_failed', extra = {
                        'table': silver_table,
                        'checks': [r.check_name for r in failed_checks],
                    })
                loaded.append(silver_table)
                conn.commit()

            except Exception as e:
                conn.rollback()
                failed.append(silver_table)
                logger.error('silver_table_failed', extra = {'table': silver_table, 'error': str(e)}, exc_info=True)

        if loaded:
            cross_results = run_cross_table_checks(conn, snapshot_id)
            persist_quality_results(conn, run_id, cross_results)

            cross_failed = [r for r in cross_results if not r.passed]
            if cross_failed:
                logger.warning('silver_cross_table_dq_failed', extra = {
                    'checks': [(r.table, r.check_name) for r in cross_failed],
                })

        run_status = 'failed' if failed else 'success'
        error_msg = f"failed tables: {failed}" if failed else None
        # mark run complete
        _complete_run(conn, run_id, run_status, error_msg)

    return SilverLoadSummary(
        run_id = run_id,
        snapshot_id = snapshot_id,
        tables_loaded = len(loaded),
        tables_failed = len(failed),
        effective_snapshot_id = effective,
    )