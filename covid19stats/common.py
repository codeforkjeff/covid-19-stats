
# python >= 3.7.4

from collections import namedtuple
import functools
import hashlib
import os
import os.path
import pathlib
import sqlite3
import time
import urllib.request


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


def blanks_to_none(row):
    return [(val if val != '' else None) for val in row]


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


# 3 hours
UPDATE_THRESHOLD = 60 * 60 * 3


def download_and_update(url, path, threshold=UPDATE_THRESHOLD):

    hash = None
    size = None
    exists = os.path.exists(path)

    path_latest = "/tmp/download_latest_" + str(time.time())
    hash_latest = None
    size_latest = None

    downloaded = False

    if (not exists) or time.time() - os.path.getmtime(path) > threshold:
        with urllib.request.urlopen(url) as f:
            data = f.read()
            hash_latest = hashlib.md5(data)
            with open(path_latest, 'wb') as output:
                output.write(data)
            size_latest = os.path.getsize(path_latest)
        downloaded = True

    replace = False

    if downloaded:
        if exists:
            size = os.path.getsize(path)
            if size == size_latest:
                with open(path, 'rb') as f:
                    data = f.read()
                    hash = hashlib.md5(data)
                if hash != hash_latest:
                    replace = True
            else:
                replace = True
        else:
            replace = True

    if downloaded:
        if replace:
            print("Downloaded file is different, replacing file")
            os.rename(path_latest, path)
        else:
            os.remove(path_latest)

    return replace

