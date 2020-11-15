
# python >= 3.7.4

import codecs
from collections import namedtuple
import csv
import datetime
import functools
import hashlib
import io
import os
import os.path
import pathlib
import pytz
import re
import shlex
import shutil
import sqlite3
import subprocess
import time
import urllib.request
import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from google.cloud import bigquery
from google.cloud import storage

Path = namedtuple('Path', ['path', 'date'])

utc=pytz.UTC

bq_path = "/opt/google-cloud-sdk/bin/bq"

bq_global_opts = "--project_id covid-19-stats-294405"

sources_bucket = 'codeforkjeff-covid-19-sources'

public_bucket = 'codeforkjeff-covid-19-public'

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


def none_to_blanks(row):
    return [(val if val is not None else '') for val in row]


def row_to_dict(row):
    d = {}
    for column in row.keys():
        d[column] = row[column]
    return d


def get_db_conn():
    path = os.path.expanduser("~/.dbt/profiles.yml")
    if os.path.exists(path):
        profiles = yaml.load(open(path).read(), Loader=Loader)
        dev = profiles['covid19']['outputs']['dev']
        host = dev['host']
        user = dev['user']
        password = dev['pass']
    else:
        raise Exception(f"{path} doesn't exist, can't get db connection params")

    conn = psycopg2.connect(database="covid19", host=host, user=user, password=password, port=5432)
    #conn = sqlite3.connect('stage/covid19.db')
    #conn.row_factory = sqlite3.Row
    return conn


def fast_bulk_insert(conn, rows, table):
    buf = io.StringIO()
    for row in rows:
        buf.write("\t".join(none_to_blanks(row)))
        buf.write("\n")
    buf.seek(0)

    c = conn.cursor()
    c.copy_from(buf, table, sep="\t", null='',size=200000)


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
            #os.rename(path_latest, path)
            shutil.move(path_latest, path)
        else:
            os.remove(path_latest)

    return replace


def clean_column_name(name):
    return re.sub(r"[^a-zA-Z0-9]", "_", name.strip())


def load_flat_file(path, conn, table_name, delimiter=",", encoding='utf-8'):

    with codecs.open(path, encoding=encoding) as f:
        reader = csv.reader(f, delimiter=delimiter)
        rows = [blanks_to_none(row) for row in reader]
        column_names = [clean_column_name(name) for name in rows[0]]
        rows = rows[1:]

    load_table(conn, table_name, column_names, rows)


def load_table(conn, table_name, column_names, rows, drop_if_exists=True):

    c = conn.cursor()

    if drop_if_exists:
        c.execute(f"DROP TABLE IF EXISTS {table_name};")

    # Create table
    c.execute(f"CREATE TABLE {table_name} (" + ",".join([col + " text" for col in column_names]) + ")")

    fast_bulk_insert(conn, rows, table_name)

    conn.commit()


def bq_load_flat_file(path, table_name, delimiter=",", encoding='utf-8'):

    cmd = f"{bq_path} {bq_global_opts} rm -f {table_name}"
    print(f"Runnning: {cmd}", flush=True)
    subprocess.run(shlex.split(cmd), check=True)

    with codecs.open(path, encoding=encoding) as f:
        headers = f.readline().strip()
        if delimiter != ',':
            headers = ",".join(clean_column_name(name) for name in headers.split(delimiter))

    cmd = f"{bq_path} {bq_global_opts} mk {table_name} {headers}"
    print(f"Runnning: {cmd}", flush=True)
    subprocess.run(shlex.split(cmd), check=True)

    load_opts = ''
    if delimiter != ',':
        load_opts = '-F ' + ('tab' if delimiter == "\t" else delimiter)

    cmd = f"{bq_path} {bq_global_opts} load --skip_leading_rows=1 {load_opts} {table_name} {path}"
    print(f"Runnning: {cmd}", flush=True)
    subprocess.run(shlex.split(cmd), check=True)


@functools.lru_cache
def get_dbt_profile():
    path = os.path.expanduser("~/.dbt/profiles.yml")
    if os.path.exists(path):
        profiles = yaml.load(open(path).read(), Loader=Loader)
        dev = profiles['covid19_bigquery']['outputs']['dev']
        return dev
    else:
        raise Exception(f"{path} doesn't exist, can't get db connection params")


def get_bq_project_id():
    profile = get_dbt_profile()
    return profile['project']


def get_bq_dataset():
    profile = get_dbt_profile()
    return profile['dataset']


def get_bq_keyfile():
    profile = get_dbt_profile()
    return profile['keyfile']


def sync_to_bucket(local_path, bucket_uri):
    project_id = get_bq_project_id()

    client = storage.Client.from_service_account_json(
        get_bq_keyfile(),
        project=project_id)

    (bucket_name, bucket_path) = parse_gs_uri(bucket_uri)
    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob(bucket_path)

    if blob.exists():
        blob.reload()
        blob_last_updated = blob.updated
    else:
        blob_last_updated = utc.localize(datetime.datetime.utcfromtimestamp(0))

    file_timestamp = utc.localize(datetime.datetime.utcfromtimestamp(os.path.getmtime(local_path)))

    if not blob.exists() or file_timestamp >= blob_last_updated:

        # needed to prevent connection timeouts when upstream is slow
        #
        # https://github.com/googleapis/python-storage/issues/74
        blob.chunk_size = 5 * 1024 * 1024 # Set 5 MB blob size

        blob.upload_from_filename(filename=local_path)
        print(f"Uploaded {local_path} to {bucket_uri}")
        return True
    else:
        print(f"File {local_path} not modified since last sync to bucket, doing nothing.")

    return False


def get_utc_now():
    # returns tz-aware datetime object for current time
    return utc.localize(datetime.datetime.utcnow())


def parse_gs_uri(uri):
    stripped = uri.replace("gs://", "")
    (bucket, path) = stripped.split("/", 1)
    return (bucket, path)


def bq_load(local_path, bucket_uri, table, delimiter=",", encoding="utf8"):
    """
    Sync a local file up to a storage bucket location and reload the
    source table in BigQuery.
    """
    synced = sync_to_bucket(local_path, bucket_uri)

    if synced:
        print(f"Reloading table {table}")

        column_names = []

        with codecs.open(local_path, encoding=encoding) as f:
            headers = f.readline().strip()
            column_names = [clean_column_name(name) for name in headers.split(delimiter)]

        bq_load_from_bucket(bucket_uri, table, column_names, delimiter)


def bq_load_from_bucket(bucket_uri, table, column_names, delimiter):
    """
    table should be "dataset.table"
    """

    client = get_bq_client()

    project_id = get_bq_project_id()
    table_id = project_id + "." + table

    job_config = bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField(column_name, "STRING") for column_name in column_names],
        skip_leading_rows=1,
        write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=delimiter
    )

    load_job = client.load_table_from_uri(
        bucket_uri, table_id, job_config=job_config
    )

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))


def get_bq_client():
    """
    factory method for client objects for BigQuery
    """
    query_job_config = bigquery.job.QueryJobConfig(
        default_dataset=get_bq_project_id() + "." + get_bq_dataset())

    client = bigquery.Client.from_service_account_json(
        get_bq_keyfile(),
        project=get_bq_project_id(),
        default_query_job_config=query_job_config)

    return client
