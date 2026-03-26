"""
Tests for load_silver.py: _build_query_params and _get_completed_tables,
the helper functions that wire SQL parameters and support the resume feature.

WHY TEST THESE: They are pure-ish functions with clear inputs/outputs.
  - _build_query_params is a pure function (no DB access) -> no mock needed at all.
  - _get_completed_tables does a single SELECT -> mock_conn with fetchall.
"""

from silver.load_silver import _build_query_params, _get_completed_tables


### query parameter builder

class TestBuildQueryParams:
    """
    _build_query_params builds the dict passed to cursor.execute(sql, params).
    Each SQL template uses %(target_snapshot_id)s, %(run_id)s, and one
    %(eff_<bronze_table>)s key per source table.
    """

    def test_single_source_table(self):
        # 'orders' reads from one bronze table -> expect one eff_ key
        effective = {'orders': 'eff_snap_orders'}
        result = _build_query_params('snap1', 'run-1', 'orders', effective)
        assert result == {
            'target_snapshot_id': 'snap1',
            'run_id': 'run-1',
            'eff_orders': 'eff_snap_orders',
        }

    def test_multi_source_table(self):
        # 'products' reads from TWO bronze tables:
        # products + translation -> expect two eff_ keys in the output dict
        effective = {
            'products': 'eff_snap_prod',
            'product_category_name_translation': 'eff_snap_cat',
        }
        result = _build_query_params('snap1', 'run-1', 'products', effective)
        assert result['target_snapshot_id'] == 'snap1'
        assert result['run_id'] == 'run-1'
        assert result['eff_products'] == 'eff_snap_prod'
        assert result['eff_product_category_name_translation'] == 'eff_snap_cat'
        # 2 fixed keys + 2 eff_ keys = 4 total
        assert len(result) == 4


### resume feature helper

class TestGetCompletedTables:
    """
    _get_completed_tables queries silver_table_loads for tables with status = 'loaded'.
    It powers the resume feature: on retry, already-loaded tables are skipped.
    """

    def test_returns_loaded_tables(self, mock_conn):
        # Simulate: fetchall returns two rows -> two loaded tables
        conn, _ = mock_conn(fetchall=[('orders',), ('customers',)])
        result = _get_completed_tables(conn, 'run-1')
        assert result == {'orders', 'customers'}

    def test_returns_empty_set_when_none_loaded(self, mock_conn):
        # No rows -> no tables have been loaded yet
        conn, _ = mock_conn(fetchall=[])
        result = _get_completed_tables(conn, 'run-1')
        assert result == set()
