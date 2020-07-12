
# python >= 3.7.4

from collections import namedtuple
import functools
import pathlib
import sqlite3
import time


Path = namedtuple('Path', ['path', 'date'])


def timer(func):
    """Print the runtime of the decorated function"""
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        print(f"Finished {func.__name__!r} in {run_time:.4f} secs")
        return value
    return wrapper_timer


def row_to_dict(row):
    d = {}
    for column in row.keys():
        d[column] = row[column]
    return d


def get_db_conn():
    conn = sqlite3.connect('stage/covid19.db')
    conn.row_factory = sqlite3.Row
    return conn


def touch_file(path):
    pathlib.Path(path).touch()
