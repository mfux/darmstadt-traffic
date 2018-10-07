import requests
import datetime as dt
import json
import threading
import random
import time


###############
#   CONFIG    #
###############

# set start-time of dataset
YEAR = 2017
MONTH = 10
DAY = 8
HOUR = 23
MINUTE = 00

# set length of dataset in minutes
LENGTH = 40

# set location to store data
PATH = './data/'  # with trailing slash


################
#   UTILITIES  #
################


def data_exists(fname):
    # check if file already exists
    try:
        with open(PATH + fname, 'r') as f:
            _ = f.readline()
        return True
    except FileNotFoundError:
        # file does not yet exist
        return False


def write_json(payload, fname):
    with open(PATH + fname, 'w') as f:
        f.write(json.dumps(payload))


def make_datetime():
    return dt.datetime(year=YEAR, month=MONTH, day=DAY, hour=HOUR, minute=MINUTE)


def make_url(time):
    url_prefix = 'https://darmstadt.ui-traffic.de/resources/CSVExport?from='
    time_str = time.strftime('%d/%m/%y+%I:%M+%p')
    url = url_prefix + time_str + '&to=' + time_str
    return url


def get_raw_data(time):
    response = requests.get(make_url(time)).text
    if len(response.split('\n')) <= 1:
        # no data available for this time
        print('No data available at', time.strftime('%d/%m/%y %H:%M'))
        return None
    return response


def parse_data(raw):
    data_lines = raw.split('\n')
    key_line = data_lines[0].rstrip().split(';')
    data_lines = [line.rstrip().replace(' ', '') for line in data_lines if line][1:]
    traffic_data = [dict(zip(key_line, data_line.split(';'))) for data_line in data_lines]
    return traffic_data


def write_data(time_chunk):
    for current_time in time_chunk:
        time.sleep(abs(random.gauss(mu=0, sigma=0.5)))
        filename = current_time.strftime('%d-%m-%y_%H:%M')
        if data_exists(filename):
            print(filename, 'already exists')
            continue
        raw_data = get_raw_data(current_time)
        if not raw_data:
            print(filename, 'is missing')
            continue
        write_json(parse_data(raw_data), filename)


def chunks(l, n):
    """Split list into n parts."""
    return [l[i::n] for i in range(n)]


################
#     MAIN     #
################
start_time = make_datetime()
times = []

for offset in range(LENGTH):
    times.append(start_time + dt.timedelta(minutes=offset))

num_threads = 4
threads = [threading.Thread(target=write_data, args=(times_chunk,)) for times_chunk in chunks(times, num_threads)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
