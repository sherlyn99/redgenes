from contextlib import contextmanager
from itertools import chain
from functools import wraps
import sqlite3
from redgenes_db.redgenes_settings import redgenes_config


def _checker(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self._contexts_entered == 0:
            raise RuntimeError(
                "Operation not permitted. Transaction methods can only be invoked within the context manager"
            )
        return func(self, *args, *kwargs)

    return wrapper


def _check_conn(con):
    try:
        con.cursor()
        return True
    except Exception as ex:
        return False


@contextmanager
def get_cursor(conn):
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()


class Transaction(object):
    def __init__(self, admin=False):
        self._queries = []
        self._results = []
        self._contexts_entered = 0
        self._connection = None
        self._post_commit_funcs = []
        self._post_rollback_funcs = []
        self.admin = admin

    def _open_connection(self):
        if self._connection is not None and _check_conn(self._connection) == True:
            return

        try:
            self._connection = sqlite3.connect(redgenes_config.dbpath)
        except sqlite3.OperationalError as e:
            raise RuntimeError("Cannot connect to database: %s" % e)

    def close(self):
        if self._connection is not None:  # double check
            self._connection.close()

    @contextmanager
    def _get_cursor(self):
        self._open_connection()
        self._connection.row_factory = sqlite3.Row
        # res = [dict(row) for row in c.fetchall()]

        try:
            with get_cursor(self._connection) as cur:
                yield cur
        except sqlite3.Error as e:
            raise RuntimeError("Cannot get sqlite3 cursor: %s" % e)

    def __enter__(self):
        self._open_connection()
        self._contexts_entered += 1
        return self

    def _clean_up(self, exc_type):  # undone
        if exc_type is not None:  # what is exc_type?
            self.rollback()
        elif self._queries:
            self.execute()
            self.commit()
        elif self._connection.in_transaction:
            self.commit()

    def __exit__(self, exc_type, exc_value, traceback):
        if self._contexts_entered == 1:
            try:
                self._clean_up(exc_type)
            finally:
                self._contexts_entered -= 1
        else:
            self._contexts_entered -= 1

    def _raise_execution_error(self, sql, sql_args, error):
        self.rollback()  # sqlite3: roll back until the last commit
        raise ValueError("Error running SQL query: %s" % error)

    @_checker
    def add(self, sql, sql_args=None, many=False):
        # many indicates whether this method should prepare to execute multiple sql queries or just a single query
        if not many:
            # if many is false, wraps sql_args in a list (of length 1)
            sql_args = [sql_args]

        for args in sql_args:
            if args:
                if not isinstance(args, (list, tuple, dict)):
                    raise TypeError(
                        "sql_args should be a list, tuple, or dict. Found %s"
                        % type(args)
                    )
            self._queries.append((sql, args))

    def _execute(self):
        with self._get_cursor() as cur:
            for sql, sql_args in self._queries:
                try:
                    if sql_args:
                        cur.execute(sql, sql_args)
                    else:
                        cur.execute(sql)
                except Exception as e:
                    self._raise_execution_error(sql, sql_args, e)

                try:
                    tmp = cur.fetchall()
                    res = [list(dict(row).values()) for row in tmp]
                    # how sqlite3 rowdict works
                # sqlite3 v2.6.0 does not report error when cur.fetchall() has no returning results
                # except sqlite3.ProgrammingError:
                #     # do not rollback when the error is caused by us running
                #     # a sql query with no results (e.g. insert, update .etc)
                #     res = None
                except sqlite3.Error as e:
                    self._raise_execution_error(sql, sql_args, e)

                self._results.append(res)

        self._queries = []
        return self._results

    @_checker
    def execute(self):
        try:
            return self._execute()
        except Exception:
            self.rollback()
            raise

    @_checker
    def execute_fetchlast(self):
        """Executes the transaction and returns the last result"""
        return self.execute()[-1][0][0]

    @_checker
    def execute_fetchindex(self, idx=-1):  
        return self.execute()[idx]

    @_checker
    def execute_fetchflatten(self, idx=-1):  # this is wrong
        """Executes the transcation and returns the flattened results of the
        `idx` query"""
        return list(chain.from_iterable(self.execute()[idx]))

    def _funcs_executor(self, funcs, func_str):
        error_msg = []
        for f, args, kwargs in funcs:
            try:
                f(*args, **kwargs)
            except Exception as e:
                error_msg.append(str(e))

        self._post_commit_funcs = []
        self._post_rollback_funcs = []
        if error_msg:
            raise RuntimeError(
                "An error ocurred during the post %s commands:\n%s"
                % (func_str, "\n".join(error_msg))
            )

    @_checker
    def commit(self):
        # reset queries, results and the index
        self._queries = []
        self._results = []
        try:
            self._connection.commit()
        except Exception:
            self._connection.close()
            raise
        # execute the post commit functions
        self._funcs_executor(self._post_commit_funcs, "commit")

    @_checker
    def rollback(self):
        """Rollbacks the transaction and reset the queries"""
        self._queries = []
        self._results = []

        if self._connection is not None and _check_conn(self._connection) == True:
            try:
                self._connection.rollback()
            except Exception:
                self._connection.close()  # why close at exceptions?
                raise
        # execute the post rollback functions
        self._funcs_executor(self._post_rollback_funcs, "rollback")

    @property
    def index(self):
        return len(self._queries) + len(self._results)

    @_checker
    def add_post_commit_func(self, func, *args, **kwargs):
        self._post_commit_funcs.append((func, args, kwargs))

    @_checker
    def add_post_rollback_func(self, func, *args, **kwargs):
        self._post_rollback_funcs.append((func, args, kwargs))


# Singleton pattern, create the transaction for the entire system
TRN = Transaction()
TRNADMIN = Transaction(admin=True)
