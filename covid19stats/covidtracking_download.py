
from .common import download_and_update


def update_covidtracking_states():

    download_and_update("https://covidtracking.com/api/v1/states/daily.csv", "input/covidtracking_states.csv")


if __name__ == "__main__":

    update_covidtracking_states()
