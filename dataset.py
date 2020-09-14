"""
Crowdquake data download library
"""

"""Entry level raw data query script

This script shows how query raw Acc. data from DB,

For reduce I/O overhead, DB scheme is designed for one-day batch usage,

So you may want additional scripting for your usage based on this script.
"""
import os
from io import BytesIO
import zlib
from typing import List, Tuple, Callable, Optional
import datetime

import json
import logging

import obspy
from obspy.core.stream import Stream

from cassandra.cluster import Cluster

logger = logging.getLogger('crowdquake.casconnector')
logger.setLevel(logging.INFO)

# Constants
# Count-to-G constants, multiply it to each trace data
EQMS_COUNT_TO_G = (2.5 / 2 ** 15)
# Count-to-m/s^2 constants, multiply it to each trace data
EQMS_COUNT_TO_MS = EQMS_COUNT_TO_G * 9.80665  # scipy.constants.g

# Connect to cassandra cluster
cluster = Cluster(['155.230.118.229'])
session = cluster.connect()

session.execute(f'USE raw_data')


def _do_select_internal(dev_id: str, start_time: datetime.datetime, end_time: datetime.datetime,
                        do_merge: bool = True) -> Stream:
    """
    Retrieve stream from database

    Constraint:

    """
    # start_time, end_time = start_time.utcnow(), end_time.utcnow()
    st = start_time.strftime('%Y-%m-%dT%H:%M:%S')
    et = end_time.strftime('%Y-%m-%dT%H:%M:%S')
    print(st, et)
    # Build query
    qry = (
        f"SELECT time_record_start, contents from skt_sensor "
        f"WHERE sensor_id='{dev_id}' AND "
        f"time_bucket='{start_time.date()}' AND "
        f"time_record_start >= '{st}' AND "
        f"time_record_start <= '{et}'"
    )

    result_set = session.execute(qry)

    # Create empty stream
    s = Stream()

    # For each stream in result_set
    for r in result_set:
        target = zlib.decompress(r.contents)

        bio = BytesIO(target)

        s += obspy.read(bio)

    # No result in result_set
    if len(s) == 0:
        return None

    # Fix network code
    for t in s:
        t.stats.network = 'SK'

    if do_merge:
        s.merge(fill_value='latest')

    return s


def query_stream(cb: Callable[['str', Stream], None], dev_ids: List['str'],
                 target_date: datetime.datetime, periods, do_merge: bool = True) -> int:
    """
    Retrieve one day data from SK network, and do cb per each station, that contains 3 channels, x, y, z.


    """

    # Create timestamp based on date
    start_time = target_date
    # tomorrow = target_date + datetime.timedelta(days=1)
    end_time = start_time + periods

    success = 0

    for dev_id in dev_ids:
        s = _do_select_internal(dev_id, start_time, end_time, do_merge)
        if s is not None:
            success += 1
            cb(dev_id, s)

    return success


def _save_stream_to_file(base_path: str, dev_id: str, s: Stream):
    """
    Save stream to base_path
    Stream saved as {base_path}/{dev_id}.mseed

    :param base_path: Basepath for save stream
    :param dev_id: device id
    :param s: Stream object
    :return:
    """

    s.write(os.path.join(base_path, f'{dev_id}.mseed'))


def download_streams(sensor_list: List[str], start_time: datetime.datetime, periods: datetime.timedelta, base_path: str):
    """
    Due to DB scheme, You can only receive one day's worth of data at a time.

    If you want to download more then one day. you must call this function several times.

    Usage:

    >> download_stream(['01231321236'], datetime.datetime(2020, 9, 1, 10, 0), datetime.timedelta(hours=2))

    :param sensor_list:
    :param start_time:
    :param periods:
    :return:
    """
    import functools
    f = functools.partial(_save_stream_to_file, base_path=base_path)

    query_stream(f, sensor_list, start_time, periods)
