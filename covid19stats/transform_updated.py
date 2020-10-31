
import glob
import os
import os.path
import pathlib
import shlex
import subprocess
import sys

update_file = "stage/transforms.updated"

def transform_updated():
    """
    figures out which source tables have changed since last transform.
    prints to stdout a space-separated list of dbt node selection expressions,
    which should get passed as args to 'dbt run --models'
    """
    last_transformed = 0

    if os.path.exists(update_file):
        last_transformed = os.stat(update_file).st_mtime

    models = []

    for path in glob.glob("stage/*.loaded"):
        base = os.path.basename(path)
        tag = base[:base.rindex(".")]
        last_loaded = os.stat(path).st_mtime
        if last_loaded > last_transformed:
            models.append(tag)

    if len(models)> 0:
        cmd = "dbt run --models " + " ".join(["@tag:" + model for model in models])
        print(f"Runnning: {cmd}", flush=True)
        subprocess.run(shlex.split(cmd), check=True)

        pathlib.Path(update_file).touch()
    else:
        print("No sources were updated since last transform; nothing to do.")


if __name__ == "__main__":
    transform_updated()
