"""Checks that the DAGs load and keep the settings the single-writer rule depends on. Needs apache-airflow installed.

Run from the repository root: python3 -m unittest discover -s airflow/tests
"""
import os
import unittest
from pathlib import Path

os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

from airflow.models import DagBag  # noqa: E402  (after the environment is set)
from airflow.timetables.simple import NullTimetable  # noqa: E402

DAGS = Path(__file__).resolve().parents[1] / "dags"


class DagTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bag = DagBag(dag_folder=str(DAGS), include_examples=False)

    def test_both_dags_load_without_errors(self):
        self.assertEqual(self.bag.import_errors, {})
        self.assertEqual(set(self.bag.dag_ids), {"lakehouse_batch", "lakehouse_backfill"})

    def test_batch_runs_bronze_then_silver_then_gold(self):
        dag = self.bag.dags["lakehouse_batch"]
        tasks = {t.task_id: t for t in dag.tasks}
        self.assertEqual(set(tasks), {"bronze", "silver", "gold"})
        self.assertEqual(tasks["bronze"].downstream_task_ids, {"silver"})
        self.assertEqual(tasks["silver"].downstream_task_ids, {"gold"})
        self.assertEqual(tasks["gold"].downstream_task_ids, set())

    def test_batch_runs_one_at_a_time_without_catch_up(self):
        dag = self.bag.dags["lakehouse_batch"]
        self.assertEqual(dag.max_active_runs, 1)
        self.assertFalse(dag.catchup)

    def test_every_task_shares_the_one_slot_writer_pool(self):
        for dag_id in ("lakehouse_batch", "lakehouse_backfill"):
            for task in self.bag.dags[dag_id].tasks:
                self.assertEqual(task.pool, "lake_writer", f"{dag_id}.{task.task_id}")

    def test_backfill_is_on_demand_and_one_at_a_time(self):
        dag = self.bag.dags["lakehouse_backfill"]
        self.assertIsInstance(dag.timetable, NullTimetable)
        self.assertEqual(dag.max_active_runs, 1)

    def test_backfill_days_parameter_accepts_dates_and_nothing_else(self):
        param = self.bag.dags["lakehouse_backfill"].params.get_param("days")
        for good in ("2026-01-01", "2026-01-01,2026-01-02"):
            self.assertEqual(param.resolve(good), good)
        for bad in ("", "2026-01-01;", "2026-01-01; rm -rf /", "$(id)", "2026-01-01,", "x"):
            with self.assertRaises(Exception, msg=bad):
                param.resolve(bad)


if __name__ == "__main__":
    unittest.main()
