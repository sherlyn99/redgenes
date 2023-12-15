# -----------------------------------------------------------------------------
# Copyright (c) 2014--, The Qiita Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

from unittest import TestCase, main
from os import remove, close
from os.path import exists
from tempfile import mkstemp

from contextlib import contextmanager
from sqlite3 import connect, Connection
from redgenes_db.redgenes_settings import redgenes_config
import redgenes_db.sql_connection as sql_connection


DB_CREATE_TEST_TABLE = """CREATE TABLE test_table (
    str_column      varchar DEFAULT 'foo' NOT NULL,
    bool_column     bool DEFAULT True NOT NULL,
    int_column      bigint NOT NULL);"""

DB_DROP_TEST_TABLE = """DROP TABLE IF EXISTS test_table;"""


@contextmanager
def get_cursor(conn):
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()


# @qiita_test_checker() # make sure we are in test not prod
class TestBase(TestCase):
    def setUp(self):
        # Add the test table to the database, so we can use it in the tests
        with connect(
            redgenes_config.dbpath,
        ) as self.con:
            with get_cursor(self.con) as cur:
                cur.execute(DB_DROP_TEST_TABLE)
                cur.execute(DB_CREATE_TEST_TABLE)
        self.con.commit()
        self._files_to_remove = []

    def tearDown(self):
        for fp in self._files_to_remove:
            if exists(fp):
                remove(fp)

        with get_cursor(self.con) as cur:
            cur.execute(DB_DROP_TEST_TABLE)
        self.con.commit()

    def _populate_test_table(self):
        """Aux function that populates the test table"""
        sql = """INSERT INTO test_table
                    (str_column, bool_column, int_column)
                 VALUES (?, ?, ?)"""
        sql_args = [
            ("test1", True, 1),
            ("test2", True, 2),
            ("test3", False, 3),
            ("test4", False, 4),
        ]
        with self.con.cursor() as cur:
            cur.executemany(sql, sql_args)
        self.con.commit()

    def _assert_sql_equal(self, exp):
        """Aux function for testing"""
        with connect(
            redgenes_config.dbpath,
        ) as con:
            with get_cursor(con) as cur:
                cur.execute("SELECT * FROM test_table")
                obs = cur.fetchall()
            con.commit()

        self.assertEqual(obs, exp)


class TestTransaction(TestBase):
    def test_init(self):
        obs = sql_connection.Transaction()
        self.assertEqual(obs._queries, [])
        self.assertEqual(obs._results, [])
        self.assertEqual(obs._connection, None)
        self.assertEqual(obs._contexts_entered, 0)
        with obs:
            pass
        self.assertTrue(isinstance(obs._connection, Connection))

    def test_add(self):
        with sql_connection.TRN:
            self.assertEqual(sql_connection.TRN._queries, [])

            sql1 = "INSERT INTO test_table (bool_column) VALUES (?)"
            args1 = [True]
            sql_connection.TRN.add(sql1, args1)
            sql2 = "INSERT INTO test_table (int_column) VALUES (1)"
            sql_connection.TRN.add(sql2)
            args3 = (False,)
            sql_connection.TRN.add(sql1, args3)
            sql3 = "INSERT INTO test_table (int_column) VALEUS (%(foo)s)"
            args4 = {"foo": 1}
            sql_connection.TRN.add(sql3, args4)

            exp = [(sql1, args1), (sql2, None), (sql1, args3), (sql3, args4)]
            self.assertEqual(sql_connection.TRN._queries, exp)

            # Remove queries so __exit__ doesn't try to execute it
            sql_connection.TRN._queries = []

    def test_add_many(self):
        with sql_connection.TRN:
            self.assertEqual(sql_connection.TRN._queries, [])

            sql = "INSERT INTO test_table (int_column) VALUES (?)"
            args = [[1], [2], [3]]
            sql_connection.TRN.add(sql, args, many=True)

            exp = [(sql, [1]), (sql, [2]), (sql, [3])]
            self.assertEqual(sql_connection.TRN._queries, exp)

    def test_add_error(self):
        with sql_connection.TRN:
            with self.assertRaises(TypeError):
                sql_connection.TRN.add("SELECT 42", 1)

            with self.assertRaises(TypeError):
                sql_connection.TRN.add("SELECT 42", {"foo": "bar"}, many=True)

            with self.assertRaises(TypeError):
                sql_connection.TRN.add("SELECT 42", [1, 1], many=True)

    def test_execute(self):
        with sql_connection.TRN:
            sql = """INSERT INTO test_table (str_column, int_column)
                     VALUES (?, ?)"""
            sql_connection.TRN.add(sql, ["test_insert", 2])
            sql = """UPDATE test_table
                     SET int_column = ?, bool_column = ?
                     WHERE str_column = ?"""
            sql_connection.TRN.add(sql, [20, False, "test_insert"])
            obs = sql_connection.TRN.execute()
            self.assertEqual(obs, [[], []])
            self._assert_sql_equal([])

        self._assert_sql_equal([("test_insert", False, 20)])

    def test_execute_many(self):
        with sql_connection.TRN:
            sql = """INSERT INTO test_table (str_column, int_column)
                     VALUES (?, ?)"""
            args = [["insert1", 1], ["insert2", 2], ["insert3", 3]]
            sql_connection.TRN.add(sql, args, many=True)
            sql = """UPDATE test_table
                     SET int_column = ?, bool_column = ?
                     WHERE str_column = ?"""
            sql_connection.TRN.add(sql, [20, False, "insert2"])
            obs = sql_connection.TRN.execute()
            self.assertEqual(obs, [[], [], [], []])

            self._assert_sql_equal([])

        self._assert_sql_equal(
            [("insert1", True, 1), ("insert2", False, 20), ("insert3", True, 3)]
        )

    def test_execute_return(self):
        with sql_connection.TRN:
            sql = """INSERT INTO test_table (str_column, int_column)
                     VALUES (?, ?) RETURNING str_column, int_column"""
            sql_connection.TRN.add(sql, ["test_insert", 2])
            sql = """UPDATE test_table SET bool_column = ?
                     WHERE str_column = ? RETURNING int_column"""
            sql_connection.TRN.add(sql, [False, "test_insert"])
            obs = sql_connection.TRN.execute()
            self.assertEqual(obs, [[["test_insert", 2]], [[2]]])

    def test_execute_return_many(self):
        with sql_connection.TRN:
            sql = """INSERT INTO test_table (str_column, int_column)
                     VALUES (?, ?) RETURNING str_column, int_column"""
            args = [["insert1", 1], ["insert2", 2], ["insert3", 3]]
            sql_connection.TRN.add(sql, args, many=True)
            sql = """UPDATE test_table SET bool_column = ?
                     WHERE str_column = ?"""
            sql_connection.TRN.add(sql, [False, "insert2"])
            sql = "SELECT * FROM test_table"
            sql_connection.TRN.add(sql)
            obs = sql_connection.TRN.execute()
            exp = [
                [["insert1", 1]],  # First query of the many query
                [["insert2", 2]],  # Second query of the many query
                [["insert3", 3]],  # Third query of the many query
                [],  # Update query
                [
                    ["insert1", True, 1],  # First result select
                    ["insert2", False, 2],
                    ["insert3", True, 3],  # Second result select
                ],
            ]  # Third result select
            self.assertEqual(obs, exp)

    def test_execute_huge_transaction(self):
        with sql_connection.TRN:
            # Add a lot of inserts to the transaction
            sql = "INSERT INTO test_table (int_column) VALUES (?)"
            for i in range(1000):
                sql_connection.TRN.add(sql, [i])
            # Add some updates to the transaction
            sql = """UPDATE test_table SET bool_column = ?
                     WHERE int_column = ?"""
            for i in range(500):
                sql_connection.TRN.add(sql, [False, i])
            # Make the transaction fail with the last insert
            sql = """INSERT INTO table_to_make (the_trans_to_fail)
                     VALUES (1)"""
            sql_connection.TRN.add(sql)

            with self.assertRaises(ValueError):
                sql_connection.TRN.execute()

            # make sure rollback correctly
            self._assert_sql_equal([])

    def test_execute_commit_false(self):
        with sql_connection.TRN:
            sql = """INSERT INTO test_table (str_column, int_column)
                     VALUES (?, ?) RETURNING str_column, int_column"""
            args = [["insert1", 1], ["insert2", 2], ["insert3", 3]]
            sql_connection.TRN.add(sql, args, many=True)

            obs = sql_connection.TRN.execute()
            exp = [[["insert1", 1]], [["insert2", 2]], [["insert3", 3]]]
            self.assertEqual(obs, exp)

            self._assert_sql_equal([])

            sql_connection.TRN.commit()

            self._assert_sql_equal(
                [("insert1", True, 1), ("insert2", True, 2), ("insert3", True, 3)]
            )

    def test_execute_commit_false_rollback(self):
        with sql_connection.TRN:
            sql = """INSERT INTO test_table (str_column, int_column)
                     VALUES (?, ?) RETURNING str_column, int_column"""
            args = [["insert1", 1], ["insert2", 2], ["insert3", 3]]
            sql_connection.TRN.add(sql, args, many=True)

            obs = sql_connection.TRN.execute()
            exp = [[["insert1", 1]], [["insert2", 2]], [["insert3", 3]]]
            self.assertEqual(obs, exp)

            self._assert_sql_equal([])

            sql_connection.TRN.rollback()

            self._assert_sql_equal([])

    def test_execute_commit_false_wipe_queries(self):
        with sql_connection.TRN:
            sql = """INSERT INTO test_table (str_column, int_column)
                     VALUES (?, ?) RETURNING str_column, int_column"""
            args = [["insert1", 1], ["insert2", 2], ["insert3", 3]]
            sql_connection.TRN.add(sql, args, many=True)

            obs = sql_connection.TRN.execute()
            exp = [[["insert1", 1]], [["insert2", 2]], [["insert3", 3]]]
            self.assertEqual(obs, exp)

            self._assert_sql_equal([])

            sql = """UPDATE test_table SET bool_column = ?
                     WHERE str_column = ?"""
            args = [False, "insert2"]
            sql_connection.TRN.add(sql, args)
            self.assertEqual(sql_connection.TRN._queries, [(sql, args)])

            sql_connection.TRN.execute()
            self._assert_sql_equal([])

        self._assert_sql_equal(
            [("insert1", True, 1), ("insert2", False, 2), ("insert3", True, 3)]
        )

    def test_execute_fetchlast(self):
        with sql_connection.TRN:
            sql = """INSERT INTO test_table (str_column, int_column)
                     VALUES (?, ?) RETURNING str_column, int_column"""
            args = [["insert1", 1], ["insert2", 2], ["insert3", 3]]
            sql_connection.TRN.add(sql, args, many=True)

            sql = """SELECT EXISTS(
                        SELECT * FROM test_table WHERE int_column=?)"""
            sql_connection.TRN.add(sql, [2])
            self.assertTrue(sql_connection.TRN.execute_fetchlast())

    def test_execute_fetchindex(self):
        with sql_connection.TRN:
            sql = """INSERT INTO test_table (str_column, int_column)
                     VALUES (?, ?) RETURNING str_column, int_column"""
            args = [["insert1", 1], ["insert2", 2], ["insert3", 3]]
            sql_connection.TRN.add(sql, args, many=True)
            self.assertEqual(sql_connection.TRN.execute_fetchindex(), [["insert3", 3]])

            sql = """INSERT INTO test_table (str_column, int_column)
                     VALUES (?, ?) RETURNING str_column, int_column"""
            args = [["insert4", 4], ["insert5", 5], ["insert6", 6]]
            sql_connection.TRN.add(sql, args, many=True)
            self.assertEqual(sql_connection.TRN.execute_fetchindex(3), [["insert4", 4]])

    def test_execute_fetchflatten(self):
        with sql_connection.TRN:
            sql = """INSERT INTO test_table (str_column, int_column)
                     VALUES (?, ?)"""
            args = [["insert1", 1], ["insert2", 2], ["insert3", 3]]
            sql_connection.TRN.add(sql, args, many=True)

            sql = "SELECT str_column, int_column FROM test_table"
            sql_connection.TRN.add(sql)

            sql = "SELECT int_column FROM test_table"
            sql_connection.TRN.add(sql)

            obs = sql_connection.TRN.execute_fetchflatten()
            self.assertEqual(obs, [1, 2, 3])

            sql = "SELECT 42"
            sql_connection.TRN.add(sql)
            obs = sql_connection.TRN.execute_fetchflatten(3)
            self.assertEqual(obs, ["insert1", 1, "insert2", 2, "insert3", 3])

    def test_context_manager_rollback(self):
        try:
            with sql_connection.TRN:
                sql = """INSERT INTO test_table (str_column, int_column)
                     VALUES (?, ?) RETURNING str_column, int_column"""
                args = [["insert1", 1], ["insert2", 2], ["insert3", 3]]
                sql_connection.TRN.add(sql, args, many=True)

                sql_connection.TRN.execute()
                raise ValueError("Force exiting the context manager")
        except ValueError:
            pass
        self._assert_sql_equal([])
        self.assertEqual(
            sql_connection.TRN._connection.in_transaction,
            False,
        )

    def test_context_manager_execute(self):
        with sql_connection.TRN:
            sql = """INSERT INTO test_table (str_column, int_column)
                 VALUES (?, ?) RETURNING str_column, int_column"""
            args = [["insert1", 1], ["insert2", 2], ["insert3", 3]]
            sql_connection.TRN.add(sql, args, many=True)
            self._assert_sql_equal([])

        self._assert_sql_equal(
            [("insert1", True, 1), ("insert2", True, 2), ("insert3", True, 3)]
        )
        self.assertEqual(
            sql_connection.TRN._connection.in_transaction,
            False,
        )

    def test_context_manager_no_commit(self):
        with sql_connection.TRN:
            sql = """INSERT INTO test_table (str_column, int_column)
                 VALUES (?, ?) RETURNING str_column, int_column"""
            args = [["insert1", 1], ["insert2", 2], ["insert3", 3]]
            sql_connection.TRN.add(sql, args, many=True)

            sql_connection.TRN.execute()
            self._assert_sql_equal([])

        self._assert_sql_equal(
            [("insert1", True, 1), ("insert2", True, 2), ("insert3", True, 3)]
        )
        self.assertEqual(
            sql_connection.TRN._connection.in_transaction,
            False,
        )

    def test_context_manager_multiple(self):
        self.assertEqual(sql_connection.TRN._contexts_entered, 0)

        with sql_connection.TRN:
            self.assertEqual(sql_connection.TRN._contexts_entered, 1)

            sql_connection.TRN.add("SELECT 42")
            with sql_connection.TRN:
                self.assertEqual(sql_connection.TRN._contexts_entered, 2)
                sql = """INSERT INTO test_table (str_column, int_column)
                         VALUES (?, ?) RETURNING str_column, int_column"""
                args = [["insert1", 1], ["insert2", 2], ["insert3", 3]]
                sql_connection.TRN.add(sql, args, many=True)

            # We exited the second context, nothing should have been executed
            self.assertEqual(sql_connection.TRN._contexts_entered, 1)
            self.assertEqual(
                sql_connection.TRN._connection.in_transaction,
                False,
            )
            self._assert_sql_equal([])

        # We have exited the first context, everything should have been
        # executed and committed
        self.assertEqual(sql_connection.TRN._contexts_entered, 0)
        self._assert_sql_equal(
            [("insert1", True, 1), ("insert2", True, 2), ("insert3", True, 3)]
        )
        self.assertEqual(
            sql_connection.TRN._connection.in_transaction,
            False,
        )

    def test_context_manager_multiple_2(self):
        self.assertEqual(sql_connection.TRN._contexts_entered, 0)

        def tester():
            self.assertEqual(sql_connection.TRN._contexts_entered, 1)
            with sql_connection.TRN:
                self.assertEqual(sql_connection.TRN._contexts_entered, 2)
                sql = """SELECT EXISTS(
                        SELECT * FROM test_table WHERE int_column=?)"""
                sql_connection.TRN.add(sql, [2])
                self.assertTrue(sql_connection.TRN.execute_fetchlast())
            self.assertEqual(sql_connection.TRN._contexts_entered, 1)

        with sql_connection.TRN:
            self.assertEqual(sql_connection.TRN._contexts_entered, 1)
            sql = """INSERT INTO test_table (str_column, int_column)
                         VALUES (?, ?) RETURNING str_column, int_column"""
            args = [["insert1", 1], ["insert2", 2], ["insert3", 3]]
            sql_connection.TRN.add(sql, args, many=True)
            tester()
            self.assertEqual(sql_connection.TRN._contexts_entered, 1)
            self._assert_sql_equal([])

        self.assertEqual(sql_connection.TRN._contexts_entered, 0)
        self._assert_sql_equal(
            [("insert1", True, 1), ("insert2", True, 2), ("insert3", True, 3)]
        )
        self.assertEqual(
            sql_connection.TRN._connection.in_transaction,
            False,
        )

    def test_post_commit_funcs(self):
        fd, fp = mkstemp()
        close(fd)
        self._files_to_remove.append(fp)

        def func(fp):
            with open(fp, "w") as f:
                f.write("\n")

        with sql_connection.TRN:
            sql_connection.TRN.add("SELECT 42")
            sql_connection.TRN.add_post_commit_func(func, fp)

        self.assertTrue(exists(fp))

    def test_post_commit_funcs_error(self):
        def func():
            raise ValueError()

        with self.assertRaises(RuntimeError):
            with sql_connection.TRN:
                sql_connection.TRN.add("SELECT 42")
                sql_connection.TRN.add_post_commit_func(func)

    def test_post_rollback_funcs(self):
        fd, fp = mkstemp()
        close(fd)
        self._files_to_remove.append(fp)

        def func(fp):
            with open(fp, "w") as f:
                f.write("\n")

        with sql_connection.TRN:
            sql_connection.TRN.add("SELECT 42")
            sql_connection.TRN.add_post_rollback_func(func, fp)
            sql_connection.TRN.rollback()

        self.assertTrue(exists(fp))

    def test_post_rollback_funcs_error(self):
        def func():
            raise ValueError()

        with self.assertRaises(RuntimeError):
            with sql_connection.TRN:
                sql_connection.TRN.add("SELECT 42")
                sql_connection.TRN.add_post_rollback_func(func)
                sql_connection.TRN.rollback()

    def test_context_manager_checker(self):
        with self.assertRaises(RuntimeError):
            sql_connection.TRN.add("SELECT 42")

        with self.assertRaises(RuntimeError):
            sql_connection.TRN.execute()

        with self.assertRaises(RuntimeError):
            sql_connection.TRN.commit()

        with self.assertRaises(RuntimeError):
            sql_connection.TRN.rollback()

        with sql_connection.TRN:
            sql_connection.TRN.add("SELECT 42")

        with self.assertRaises(RuntimeError):
            sql_connection.TRN.execute()

    def test_index(self):
        with sql_connection.TRN:
            self.assertEqual(sql_connection.TRN.index, 0)

            sql_connection.TRN.add("SELECT 42")
            self.assertEqual(sql_connection.TRN.index, 1)

            sql = "INSERT INTO test_table (int_column) VALUES (?)"
            args = [[1], [2], [3]]
            sql_connection.TRN.add(sql, args, many=True)
            self.assertEqual(sql_connection.TRN.index, 4)

            sql_connection.TRN.execute()
            self.assertEqual(sql_connection.TRN.index, 4)

            sql_connection.TRN.add(sql, args, many=True)
            self.assertEqual(sql_connection.TRN.index, 7)

        self.assertEqual(sql_connection.TRN.index, 0)


if __name__ == "__main__":
    main()
