
import hashlib
import os
import os.path
import time
import urllib.request


def update_covidtracking_states():

    path = "input/covidtracking_states.csv"
    hash = None

    path_latest = "input/covidtracking_states_latest.csv"
    hash_latest = None

    # seconds
    THRESHOLD = 60 * 60 * 3

    if os.path.exists(path):
        with open(path, 'rb') as f:
            data = f.read()
            hash = hashlib.md5(data)

    if (not os.path.exists(path)) or time.time() - os.path.getmtime(path) > THRESHOLD:
        with urllib.request.urlopen("https://covidtracking.com/api/v1/states/daily.csv") as f:
            data = f.read()
            hash_latest = hashlib.md5(data)
            with open(path_latest, 'wb') as output:
                output.write(data)

    if hash_latest:
        if hash != hash_latest:
            print("Replacing covid states file")
            os.rename(path_latest, path)
        else:
            os.remove(path_latest)


if __name__ == "__main__":
    update_covidtracking_states()

