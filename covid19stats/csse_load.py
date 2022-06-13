
import codecs
import csv
import glob
import io
import multiprocessing
import os
import os.path
import time

from .common import timer, touch_file, Path, bq_load, sources_bucket


ordered_fields = [
    "FIPS",
    "Admin2",
    "Province_State",
    "Country_Region",
    "Last_Update",
    "Lat",
    "Long_",
    "Confirmed",
    "Deaths",
    "Recovered",
    "Active",
    "Combined_Key",
]


def get_sortable_date(path):
    basename = os.path.basename(path)
    return basename[6:10] + basename[0:2] + basename[3:5]


def get_rows_from_csse_file(path):
    result = []
    print(f"Loading from {path.path}")
    with codecs.open(path.path, encoding='utf8') as f:
        reader = csv.reader(f)
        field_order_in_file = []
        first_row = True
        for row in reader:
            if first_row:
                field_order_in_file = row
                # strip out BOM
                if field_order_in_file[0].encode().startswith(codecs.BOM_UTF8):
                    # this is ridiculous
                    field_order_in_file[0] = field_order_in_file[0].encode()[len(codecs.BOM_UTF8):].decode()
            else:
                reordered = []
                for field in ordered_fields:
                    reordered.append(row[field_order_in_file.index(field)])

                row = [path.date] + reordered

                #row = [None if val == '' else val for val in row]

                result.append(row)

            first_row = False
    return result


@timer
def load_csse():

    # TODO: should probably use path relative to this .py file
    spec = os.path.join('..', 'COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/*.csv')
    print(f"Looking for files: {spec}")
    paths = [Path(path, get_sortable_date(path)) for path in glob.glob(spec)]

    filtered_paths = [p for p in paths if p.date >= '20200322']

    start_time = time.perf_counter()

    num_rows = 0

    with codecs.open("stage/raw_csse.txt", "w", encoding='utf-8') as f:
        f.write("\t".join(['Date']+ordered_fields))
        f.write("\n")

        p = multiprocessing.Pool(4)
        for result in p.map(get_rows_from_csse_file, filtered_paths):
            print(f"Writing {len(result)} rows to file")
            num_rows += len(result)
            for row in result:
                f.write("\t".join(row))
                f.write("\n")
        p.close()

    bq_load("stage/raw_csse.txt", f"gs://{sources_bucket}/raw_csse.txt", 'source_tables.raw_csse', delimiter="\t")

    end_time = time.perf_counter()
    run_time = end_time - start_time
    rate = num_rows / run_time

    print(f"load_csse took {run_time:.4f} secs ({rate:.4f} rows/s)")

    # -- find rows that should havbe a fips code but doesn't.
    # -- I think these are actually okay to let by.
    #
    # select distinct combined_key
    # from raw_csse
    # where
    #     shouldhavefips = 1
    #     and (fips is null or length(fips) <> 5 or fips = '00000')

    touch_file('stage/csse.loaded')


if __name__ == "__main__":
    load_csse()

