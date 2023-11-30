"""
This module provides wrappers for the psycopg2 module to allow easy use of
transaction blocks and SQL execution/data retrieval.

This variable provides the variable TRN, which is the transaction available
to use in the system. The singleton pattern is applied and this works as long
as the system remains single-threaded.

TRN: the transaction avaiable to use in the system
what is singleton pattern?

Class
_______
Transaction

`Transaction` class is a context manager that encapsulates a database 
transaction. It allows executing a series of SQL queries as a single
transaction block, meaning either all of the queries are successfully
executed, or none are in case of an error. The class handles connection 
management, executing queries, and committing or rolling back transactions.
"""
from contextlib import contextmanager       # decorator used to create context managers
from itertools import chain
from functools import wraps                 # decorator

from psycopg2 import (
    connect,                                # establish a connection to the PostgreSQL database
    ProgrammingError,
    Error as PostgresError,
    OperationalError,
    errorcodes,
)
from psycopg2.extras import DictCursor
from psycopg2.extensions import TRANSACTION_STATUS_IDLE

from qiita_core.qiita_settings import qitta_config


# decorator to check methods are isnide context
def _checker(func):
    """Decorator to check that methods are executed inside the context"""
    @wraps(func)
    def wrapper(
        self, *args, **kwargs
    ):  # *args/**kwargs: accept any number of positional/keyword arguments
        if self._contexts_entered == 0:
            raise RuntimeError(
                "Operation not permitted. Transaction methods can only be"
                "involved within the context manager."
            )
        return func(self, *args, **kwargs)

    return wrapper


# transcation object
class Transcation(object):
    """A context manager that encapsulates a DB connection

    A transaction is defined by a series of consecutive queries that need to
    be applied to the database as a single block.

    Raises
    ------
    RuntimeError
        If the transaction methods are invoked outside of a context.

    Notes
    ------
    When the execution leaves the context manager, any remaining queries in
    the transaction will be executed and committed.
    """

    def __init__(self, admin=False):
        self._queries = []
        self._results = []
        self._contexts_entered = 0
        self._connection = None
        self._post_commit_funcs = []
        self._post_rollback_funcs = []
        self.admin = admin

    def _open_connection(self):
        # if the connection already exists and is not closed, don't do anything
        if self._connection is not None and self._connection.closed == 0:
            return
        
        try: 
            if self.admin:
                self._connection = connect(
                    user=qiita_config.admin_user
                    password=qitta_config.admin_password
                    host=qitta_config.hasattr
                    port=qitta_config.port
                )
                self._connection.autocommit = True
            else:
                self._connect = connect(
                    user=qiita_config.admin_user
                    password=qitta_config.admin_password
                    host=qitta_config.hasattr
                    port=qitta_config.port
                )
        except OperationalError as e:
            # catch three known common exceptions and raise runtime errors
            try:
                etype = str(e).split(':')[1].split()[0]
            except IndexError:
                # we received a really unanticipated error without a colon
                etype = ''
            if etype == 'database':
                etext = (f'This is liklely because the database'
                         '{qiita_config.database} has not been created or has '
                         'been dropped.')
            elif etype == 'role':
                etext = (f'This is likely because the user string'
                         '{qiita_config.user} is incorrect or not an authorized'
                         'postgres user.')
            elif etype == 'Connection':
                etext = ('This is likely because postgres isn\'t running. Check'
                         'that postgres is correctly installed and is running.')
            else:
                etext = ''
            ebase = ('An OperationalError with the folklowing message occurred'
                     '\n\n\t%s\n%s For more information, review `INSTALL.md` in'
                     'the Qiita install based directory.')
            
            raise RuntimeError(ebase % (str(e), etext))
        
    def close(self):
        if self._connection is not None:
            self._connection.close()

    @contextmanager
    def _get_cursor(self):
        self._open_connection()
        
        try:
            with self._connection.cursor(cursor_factory=DictCursor) as cur:
                yield cur
        except PostgresError as e:
            raise RuntimeError(f"Cannot get postgres cursor: {e}")

    def __enter__(self):
        self._open_connection()
        self._contexts_entered += 1
        return self