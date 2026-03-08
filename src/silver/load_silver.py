import uuid
import logging
from psycopg2 import sql as psql, extensions

from db import get_db_connection
from .transform_silver import TRANSFORMS, resolve_effective_snapshot
from .config import SILVER_TABLE_SOURCES

logger = logging.getLogger(__name__)

def load(target_snapshot_id: str, run_id: str = None):
    run_id = run_id or str(uuid.uuid4())

    with get_db_connection() as conn:
        # register run
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingestion.runs (run_id, snapshot_id, status) VALUES (%s, %s, 'started')
                """,
                (run_id, target_snapshot_id),
            )
        conn.commit()

        # resolve which bronze snapshot each source table should read from
        effective = resolve_effective_snapshot(conn, target_snapshot_id)

        # build param dict shared by all SQL templates
        params = {
            'target_snapshot_id': target_snapshot_id,
            'run_id': run_id,
        }
        for bronze_table, snap in effective.items():
            params[f"eff_{bronze_table}"] = snap

        for table_name, source_tables in SILVER_TABLE_SOURCES.items():
            try:
                # idempotency: clear previous data for this snapshot
                with conn.cursor() as cur:
                    cur.execute(
                        psql.SQL("DELETE FROM silver.{} WHERE _snapshot_id = %s").format(
                            psql.Identifier(table_name)
                        ),
                        (target_snapshot_id,),
                    )
                    # execute the transform
                    cur.execute(TRANSFORMS[table_name], params)
                    rows = cur.rowcount

                # record lineage
                with conn.cursor() as cur:
                    for src in source_tables:
                        cur.execute(
                            """
                            INSERT INTO ingestion.silver_linage
                            (run_id, silver_table, bronze_table, effective_snapshot_id)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
                            """,
                            (run_id, table_name, src, effective[src]),
                        )
                conn.commit()
                logger.info('silver_table_loaded', extra={'table': table_name, 'rows': rows})

            except Exception:
                conn.rollback()
                logger.error('silver_table_failed', extra={'table': table_name}, exc_info=True)
                raise

        # mark run complete
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ingestion.runs SET status = 'success', end_time = NOW() WHERE run_id = %s
                """,
                (run_id,),
            )
        conn.commit()
        logger.info('silver_load_complete', extra={'run_id': run_id, 'snapshot': target_snapshot_id})