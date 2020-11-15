
import codecs
import csv
import datetime
import os
import os.path
import pytz
import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from google.cloud import bigquery
from google.cloud import storage


def parse_gs_uri(uri):
    stripped = uri.replace("gs://", "")
    (bucket, path) = stripped.split("/", 1)
    return (bucket, path)


def get_utc_now():
    # returns tz-aware datetime object for current time
    return utc.localize(datetime.datetime.utcnow())

def get_bq_project_id():
    path = os.path.expanduser("~/.dbt/profiles.yml")
    if os.path.exists(path):
        profiles = yaml.load(open(path).read(), Loader=Loader)
        dev = profiles['covid19_bigquery']['outputs']['dev']
        return dev['project']
    else:
        raise Exception(f"{path} doesn't exist, can't get db connection params")


class Loader:

    def __init__(self):
        pass


    def load(self):
        pass


class BigQueryLoader(Loader):
    """
    Loads files into source tables in BigQuery, by way of Google Cloud Storage.
    """

    def __init__(self, project_id, local_path, bucket_uri, table, delimiter=",", encoding="utf8"):
        """
        table should be "dataset.table"
        """
        self.project_id = project_id
        self.local_path = local_path
        self.bucket_uri = bucket_uri
        self.table = table
        self.delimiter = delimiter
        self.encoding = encoding


    def needs_load(self):
        """
        Compares timestamps of local files to blobs on GCS to determine
        whether we need to load.
        """

        client = storage.Client(project=self.project_id)

        (bucket_name, bucket_path) = parse_gs_uri(self.bucket_uri)
        bucket = client.get_bucket(bucket_name)

        blob = bucket.blob(bucket_path)

        if blob.exists():
            blob.reload()
            blob_last_updated = blob.updated
        else:
            blob_last_updated = utc.localize(datetime.datetime.utcfromtimestamp(0))

        file_timestamp = utc.localize(datetime.datetime.utcfromtimestamp(os.path.getmtime(self.local_path)))

        return not blob.exists() or file_timestamp >= blob_last_updated


    def copy_to_bucket(self):

        print(f"Uploading {self.local_path} to {self.bucket_uri}")
        blob.upload_from_filename(filename=self.local_path)


    def load(self):
        """
        Sync a local file up to a storage bucket location and reload the
        source table in BigQuery.
        """
        if self.needs_load():

            self.copy_to_bucket()

            column_names = []

            with codecs.open(self.local_path, encoding=self.encoding) as f:
                headers = f.readline().strip()
                column_names = [clean_column_name(name) for name in headers.split(self.delimiter)]

            self.load_from_bucket()

        else:
            print(f"File {self.local_path} not modified since last sync to {self.bucket_uri}, doing nothing.")


    def load_from_bucket(self):

        print(f"(Re)loading table {self.table} from {self.bucket_uri}")

        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField(column_name, "STRING") for column_name in column_names],
            skip_leading_rows=1,
            write_disposition=bigquery.job.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter=self.delimiter
        )

        table_id = self.project_id + "." + self.table

        load_job = client.load_table_from_uri(
            self.bucket_uri, table_id, job_config=job_config
        )

        load_job.result()  # Waits for the job to complete.

        destination_table = client.get_table(table_id)

        print("Loaded {} rows.".format(destination_table.num_rows))

