import sqlite3
from itertools import chain
from functools import wraps
from contextlib import contextmanager
from redgenes_settings import redgenes_config

def _checker(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self._contexts_entered <= 0:
            raise RuntimeError("Transaction must be used within a context manager.")
        return func(self, *args, **kwargs)
    return wrapper

@contextmanager
def get_cursor(connection):
    cursor = connection.cursor()
    try:
        yield cursor
    finally:
        cursor.close()

class Transaction:
    def __init__(self, admin=False):
        self._queries = []
        self._contexts_entered = 0
        self._connection = None
        self._admin = admin
        self._post_commit_funcs = []
        self._post_rollback_funcs = []

    def _open_connection(self):
        if not self._connection:
            self._connection = sqlite3.connect(redgenes_config.dbpath)
            self._connection.row_factory = sqlite3.Row

    def __enter__(self):
        if self._contexts_entered == 0:
            self._open_connection()
        self._contexts_entered += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._clean_up(exc_type)
        self._contexts_entered -= 1
        if self._contexts_entered == 0:
            self._connection.close()
            self._connection = None

    def _clean_up(self, exc_type):
        if exc_type:
            self.rollback()
        else:
            if self._queries:
                self.execute()
            self.commit()

    @_checker
    def add(self, sql, sql_args=None, many=False):
        if many:
            self._queries.extend([(sql, args) for args in sql_args])
        else:
            self._queries.append((sql, sql_args or []))

    def _execute(self):
        results = []
        with get_cursor(self._connection) as cursor:
            for sql, sql_args in self._queries:
                cursor.execute(sql, sql_args or [])
                results.append(cursor.fetchall())
        self._queries = []
        return results

    @_checker
    def execute(self):
        try:
            return self._execute()
        except sqlite3.Error as e:
            self.rollback()
            raise RuntimeError(f"Database execution error: {e}") from e

    @_checker
    def execute_fetchdicts(self, idx=-1):
        """Executes the transaction and returns the results as a list of dictionaries."""
        all_results = self.execute()
        if idx >= len(all_results) or -idx > len(all_results):
            # If idx is out of bounds, return an empty list or handle the error as needed
            return []
        
        selected_results = all_results[idx] if all_results else []
        return [dict(row) for row in selected_results]

    @_checker
    def execute_fetchlast(self):
        """Fetches the last result of the last executed query."""
        return self.execute()[-1][-1] if self._queries else None

    @_checker
    def execute_fetchindex(self, idx=-1):
        """Fetches results by index from the executed queries."""
        return self.execute()[idx] if self._queries else None

    @_checker
    def execute_fetchflatten(self, idx=-1):
        """Flattens and fetches results of the indexed query."""
        return list(chain.from_iterable(self.execute()[idx])) if self._queries else None

    @_checker
    def execute_fetchiter(self):
        """Generates an iterator for the results of the queries."""
        if not self._queries:
            return iter([])
        self._open_connection()
        results = self._execute()
        return iter(chain.from_iterable(results))

    def close(self):
        if self._connection:
            self._connection.close()
            self._connection = None

    @_checker
    def commit(self):
        self._connection.commit()
        for func, args, kwargs in self._post_commit_funcs:
            func(*args, **kwargs)
        self._post_commit_funcs = []

    @_checker
    def rollback(self):
        if self._connection:
            self._connection.rollback()
        for func, args, kwargs in self._post_rollback_funcs:
            func(*args, **kwargs)
        self._post_rollback_funcs = []

    @property
    def index(self):
        return len(self._queries) + len(self._results)

    @_checker
    def add_post_commit_func(self, func, *args, **kwargs):
        self._post_commit_funcs.append((func, args, kwargs))

    @_checker
    def add_post_rollback_func(self, func, *args, **kwargs):
        self._post_rollback_funcs.append((func, args, kwargs))

    @_checker
    def executescript(self, sql_script):
        """Executes an SQL script as part of the transaction."""
        if self._connection:
            with get_cursor(self._connection) as cursor:
                cursor.executescript(sql_script)

# Singleton pattern, create the transaction for the entire system
TRN = Transaction()
TRNADMIN = Transaction(admin=True)


def perform_as_transaction(sql, parameters=None):
    """Opens, adds and executes sql as a single transaction

    Parameters
    ----------
    sql : str
        The SQL to execute
    parameters: object, optional
        The object of parameters to pass to the TRN.add command
    """
    with TRN:
        if parameters:
            TRN.add(sql, parameters)
        else:
            TRN.add(sql)
        TRN.execute()


def create_new_transaction():
    """Creates a new global transaction

    This is needed when using multiprocessing
    """
    global TRN
    TRN = Transaction()
