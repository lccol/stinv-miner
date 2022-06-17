import numpy as np
import datetime
import argparse
import time
import math
import pandas as pd
import pyspark as ps

from pathlib import Path
from sklearn.neighbors import BallTree
from pyspark import SparkContext, Broadcast, RDD
from pyspark.sql import Row, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from functools import partial

from stpm.sequence_analyzer import SequenceGenerator, SequenceExtractor, SequenceAbsoluteGenerator
from stpm.utils import extract_temporal_id
from typing import Union, Set, List, Tuple, Dict, Optional
from collections import defaultdict

def parse():
    parser = argparse.ArgumentParser(description='Pattern extraction on SF Bike Sharing dataset')
    parser.add_argument('-t', type=int, help='Time threshold in minutes')
    parser.add_argument('-s', type=int, help='Spatial threshold in meters')
    parser.add_argument('-nt', type=int, help='Number of temporal steps')
    parser.add_argument('-ns', type=int, help='Number of spatial steps. This parameter is ignored if neigh-type is set to "incoming" or "nearest".')
    parser.add_argument('--tag', type=str, default='', help='Tag for spark job.')
    parser.add_argument('--support', type=float, help='Minimum support used for extraction')
    parser.add_argument('--max-pattern-len', dest='max_pattern_len', type=int, default=10, help='Maximum pattern (total number of elements) length.')
    parser.add_argument('--cities', type=str, help='Cities on which the extraction is performed. If "global", a single global extraction is performed on the entire dataset; otherwise, a single string is required containing the city name on which the extraction must be performed')
    parser.add_argument('--event-types', type=str, dest='event_type', help='Type of events which must be extracted, comma-separated. Must be one or more of the following: {"increase", "decrease", "full", "almost_full", "empty", "almost_empty"}')
    parser.add_argument('--neigh-type', type=str, dest='neigh_type', help='Type of neighborhood which must be considered. Can be one of the following: {"radius", "nearest", "incoming"}. Radius means that all the events within a radius of spatial_threshold * n_spatial_steps are considered. Nearest means that top N nearest stations are considered. Incoming means that the top N stations from which bikes arrived are considered for each station. N is specified with the --top-n flag.')
    parser.add_argument('--top-n', type=int, dest='top_n', default=0, help='Number of stations to be considered in case neigh-type is either top-n or nearest. Otherwise it is ignored.')
    parser.add_argument('--ignore-set', type=str, dest='ignore_set', default='', help='Types of events that will be ignored when reprojecting the events. Must be comma-separated. Admitted values: {"increase", "decrease", "full", "almost_full", "empty", "almost_empty"}.')
    parser.add_argument('--reproj-mode', type=str, dest='reproj_mode', default='future', help='Temporal reprojection mode. Must be either "future" or "past". Default is "future".')
    parser.add_argument('-o', type=str, help='Output path for the extracted patterns on HDFS.')
    parser.add_argument('--absolute', default=False, action='store_true', help='Set absolute mode, i.e., uses station IDs instead of discretized spatial distance. Default is False.')
    return parser.parse_args()

def haversineDistance(aLat, aLng, bLat, bLng):
    #From degree to radian
    fLat = math.radians(aLat)
    fLon = math.radians(aLng)
    sLat = math.radians(bLat)
    sLon = math.radians(bLng)

#     R = 3958.7564 #mi
    R = 6_371_000.0 #meters
    #R = 6371.0 #km
    dLon = sLon - fLon
    dLat = sLat - fLat
    a = math.sin(dLat/2.0)**2 + (math.cos(fLat) * math.cos(sLat) * math.pow(math.sin(dLon/2.0), 2))
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))

    return R * c

def spatial_corr_func_radius(e1: Row,
                      e2: Row,
                      spat_thr: float,
                      max_spat_delta: int) -> int:
    max_dist = spat_thr * max_spat_delta
    dist = haversineDistance(e1['lat'], e1['long'], e2['lat'], e2['long'])
    if dist > max_dist:
        return -1
    return math.ceil(dist / spat_thr)

def spatial_corr_func_from_neigh_list(e1: Row,
                              e2: Row,
                              spat_thr: float,
                              neigh_dict: Broadcast,
                              check_reverse_events: bool=False) -> int:
    id1, id2 = e1['station'], e2['station']
    evt1, evt2 = e1['event_type'], e2['event_type']
    if not id2 in neigh_dict.value[id1]:
        return -1
    dist = haversineDistance(e1['lat'], e1['long'], e2['lat'], e2['long'])
    if check_reverse_events:
        if evt1 in {'full', 'almost_full', 'increase'} and \
            not evt2 in {'decrease', 'empty', 'almost_empty'} and \
            dist > 0:
            return -1
        elif evt1 in {'empty', 'almost_empty', 'decrease'} and \
            not evt2 in {'increase', 'full', 'almost_full'} and \
            dist > 0:
            return -1
        elif dist == 0 and \
            not ((evt1 in {'empty', 'almost_empty', 'decrease'} and \
            evt2 in {'empty', 'almost_empty', 'decrease'}) or \
                (evt1 in {'full', 'almost_full', 'increase'} and \
            evt2 in {'full', 'almost_full', 'increase'})):
            return -1
                
    return math.ceil(dist / spat_thr)

def timeslot_station_repartition(df: DataFrame, minutes: int) -> DataFrame:
    rdd = df.rdd
    time_idx_func = partial(extract_temporal_id, time_granularity=datetime.timedelta(minutes=minutes))
    station_temporal_rdd = rdd.map(lambda it: ((time_idx_func(it['time']), it['id']), it)) \
                                .groupByKey() \
                                .mapValues(list)
    return station_temporal_rdd

def station_event_generation(rdd,
                             event_set: Set[str],
                             almost_threshold: Union[float, int]=2,
                             almost_overlap_check: bool=False):
    use_relative_thr = 0 < almost_threshold < 1
    if not use_relative_thr:
        assert almost_threshold >= 1

    assert event_set.issubset({'increase', 'decrease', 'full', 'almost_full', 'empty', 'almost_empty'})
        
    def _generate(seq):
        seq.sort(key=lambda it: it['time'])
        occurred = set()
        thr = round(seq[0]['dock_count'] * almost_threshold) if use_relative_thr else almost_threshold
        last_bikes_avail = None
        events = []
        for s in seq:
            dock_avail, bikes_avail = s['docks_available'], s['bikes_available']
            if 'almost_full' in event_set and \
                not 'almost_full' in occurred:
                if (dock_avail <= thr and not 'full' in event_set) or \
                    (0 < dock_avail <= thr and 'full' in event_set):
                    occurred.add('almost_full')
                    events.append(Row(event_type='almost_full',
                                      timestamp=s['time'],
                                      station=s['station_id'],
                                      lat=s['lat'],
                                      long=s['long']))
            if 'full' in event_set and \
                not 'full' in occurred:
                if dock_avail == 0:
                    occurred.add('full')
                    events.append(Row(event_type='full',
                                      timestamp=s['time'],
                                      station=s['station_id'],
                                      lat=s['lat'],
                                      long=s['long']))
            if 'almost_empty' in event_set and \
                not 'almost_empty' in occurred:
                if (bikes_avail <= thr and not 'empty' in event_set) or \
                    (0 < bikes_avail <= thr and 'empty' in event_set):
                    occurred.add('almost_empty')
                    events.append(Row(event_type='almost_empty',
                                      timestamp=s['time'],
                                      station=s['station_id'],
                                      lat=s['lat'],
                                      long=s['long']))
            if 'empty' in event_set and \
                not 'empty' in occurred:
                if bikes_avail == 0:
                    occurred.add('empty')
                    events.append(Row(event_type='empty',
                                      timestamp=s['time'],
                                      station=s['station_id'],
                                      lat=s['lat'],
                                      long=s['long']))
            if 'increase' in event_set and \
                not 'increase' in occurred and \
                not last_bikes_avail is None:
                if bikes_avail > last_bikes_avail:
                    occurred.add('increase')
                    events.append(Row(event_type='increase',
                                      timestamp=s['time'],
                                      station=s['station_id'],
                                      lat=s['lat'],
                                      long=s['long']))
            if 'decrease' in event_set and \
                not 'decrease' in occurred and \
                not last_bikes_avail is None:
                if bikes_avail < last_bikes_avail:
                    occurred.add('decrease')
                    events.append(Row(event_type='decrease',
                                      timestamp=s['time'],
                                      station=s['station_id'],
                                      lat=s['lat'],
                                      long=s['long']))
            last_bikes_avail = bikes_avail
        if almost_overlap_check:
            if 'empty' in occurred and 'almost_empty' in occurred:
                events = list(filter(lambda t: t != 'almost_empty', events))
            if 'full' in occurred and 'almost_full' in occurred:
                events = list(filter(lambda t: t['event_type'] != 'almost_full', events))
        return events
    return rdd.flatMapValues(_generate).values()

def _filter_by_city(df: pd.DataFrame, cities: str) -> pd.DataFrame:
    if cities == 'global':
        return df
    cities = set(cities.split(','))
    df = df[df['city'].isin(cities)]
    assert df.shape[0] > 0
    
    return df

def compute_nearest_neighbors(path: Union[str, Path],
                              cities: str,
                              n_neigh: int) -> Dict[int, Set[int]]:
    res = defaultdict(set())
    df = pd.read_csv(path)
    
    n_neigh += 1 # increase it since 1 of the neighbor for each point will be the point itself
    
    if cities != 'global':
        df = _filter_by_city(df, cities)
    
    X = np.radians(df[['lat', 'long']].values)
    bt = BallTree(x, metric='haversine')
    neighbors = bt.query(X, k=n_neigh)
    for idx, neigh_list in enumerate(neighbors):
        count_same = 0
        key = df.iloc[idx]['id']
        for neigh_idx in enumerate(neigh_list):
            if neigh_idx == idx:
                count_same += 1
                continue
            value = df.iloc[neigh_idx]['id']
            res[key].add(value)
        assert count_same == 1
    return dict(res)

def compute_transaction_neighbors(trip_hdfs_path: str,
                                  local_station_path: Union[Path, str],
                                  cities: str,
                                  n_neigh: int,
                                  spark: Optional[SparkSession]=None) -> Dict[int, Set[int]]:
    res = {}
    df = pd.read_csv(local_station_path)
    
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    if cities != 'global':
        df = _filter_by_city(df, cities)
    ids = set(df['id'].unique().tolist())
    
    trip_df = spark.read.csv(trip_hdfs_path,
                             header=True,
                             sep=',',
                             inferSchema=True)
    if cities != 'global':
        cities = set(cities.split(','))
        filtered_trip_df = trip_df.filter(trip_df.start_station_id.isin(ids) & trip_df.end_station_id.isin(ids))
    else:
        filtered_trip_df = trip_df
    def _keep_top(it):
        it.sort(key=lambda x: x[1], reverse=True)
        return it[:n_neigh]
    local_res = filtered_trip_df.groupBy('start_station_id', 'end_station_id') \
                                .count() \
                                .rdd \
                                .map(lambda it: (it['end_station_id'], (it['start_station_id'], it['count']))) \
                                .groupByKey() \
                                .mapValues(list) \
                                .mapValues(_keep_top) \
                                .collect()
    for key, values in local_res:
        acc = set(map(lambda e: e[0], values))
        res[key] = acc
    return res

def compute_neighbors(trip_path: str,
                      local_station_path: Union[str, Path],
                      cities: str,
                      n_neigh: int,
                      neigh_type: str,
                      spark: Optional[SparkSession]=None) -> Dict[int, Set[int]]:
    if neigh_type == 'radius':
        return {}
    if neigh_type == 'nearest':
        return compute_nearest_neighbors(local_station_path, cities, n_neigh)
    elif neigh_type == 'incoming':
        return compute_transaction_neighbors(trip_path,
                                             local_station_path,
                                             cities,
                                             n_neigh,
                                             spark)
    else:
        raise ValueError(f'Not yet implemented the following neighbors type: {neigh_type}')
        
def print_config(args, neighbor_dict):
    print('#' * 50 + ' CONFIGURATION ' + '#' * 50)
    print(f'Tag: {args.tag}')
    print(f'Temporal threshold : {args.t}')
    print(f'Spatial threshold: {args.s}')
    print(f'Temporal steps: {args.nt}')
    print(f'Spatial steps: {args.ns}')
    print(f'Minimum support: {args.support}')
    print(f'Max pattern length: {args.max_pattern_len}')
    print(f'Cities value: {args.cities}')
    print(f'Event types: {args.event_type}')
    print(f'Neighbor types: {args.neigh_type}')
    print(f'Number of neighbors: {args.top_n}')
    print(f'Ignore set: {set(args.ignore_set.split(","))}')
    print(f'Output path: {args.o}')
    print(f'Neighbors: {neighbor_dict}')
    print(f'Reprojection mode: {args.reproj_mode}')
    print(f'Absolute mode: {args.absolute}')
    print('#' * 50 + ' END CONFIGURATION ' + '#' * 50)
    return

if __name__ == '__main__':
    # INPUT PROGRAM ARGUMENTS
    args = parse()
    TEMPORAL_THRESHOLD = args.t
    SPATIAL_THRESHOLD = args.s
    TEMPORAL_STEPS = args.nt
    SPATIAL_STEPS = args.ns
    TAG = args.tag
    MAX_PATT_LEN = args.max_pattern_len
    MINSUPP = args.support
    CITIES = args.cities
    EVENT_TYPES = args.event_type
    NEIGHBOR_TYPES = args.neigh_type
    TOP_N = args.top_n
    IGNORE_SET = args.ignore_set
    REPROJ_MODE = args.reproj_mode
    OUTPUT_PATH = args.o
    ABSOLUTE_MODE = args.absolute
    # END INPUT PROGRAM ARGUMENTS
    
    spark = SparkSession.builder \
                    .appName(f'{TAG}') \
                    .getOrCreate()
    sc = spark.sparkContext
    
    local_station_path = Path.home() / 'datasets' / 'sf_bay_area_bike_sharing' / 'station.csv'

    basepath = 'datasets/SFBayAreaBikeSharing/'
    status_path = basepath + 'status.csv'
    station_path = basepath + 'station.csv'
    trip_path = basepath + 'trip.csv'
    
    
    
    event_to_generate = set(EVENT_TYPES.split(','))
    ignore_set = set(IGNORE_SET.split(',')) if len(IGNORE_SET) > 0 else None
    
    neigh_dict = compute_neighbors(trip_path,
                                     local_station_path,
                                     CITIES,
                                     TOP_N,
                                     NEIGHBOR_TYPES,
                                     spark)
    
    print_config(args, neigh_dict)
    print(f'number of stations: {len(neigh_dict)}')
    
    if NEIGHBOR_TYPES == 'radius':
        spatial_cf = partial(spatial_corr_func_radius,
                             spat_thr=SPATIAL_THRESHOLD,
                             max_spat_delta=SPATIAL_STEPS)
    elif NEIGHBOR_TYPES == 'nearest' or NEIGHBOR_TYPES == 'incoming':
        neigh_dict_broadcast = sc.broadcast(neigh_dict)
        check_reverse = True if NEIGHBOR_TYPES == 'incoming' else False
        print(f'Check reverse type of events: {check_reverse}')
        spatial_cf = partial(spatial_corr_func_from_neigh_list,
                             spat_thr=SPATIAL_THRESHOLD,
                             neigh_dict=neigh_dict_broadcast,
                             check_reverse_events=check_reverse)
    else:
        raise ValueError(f'Invalid neighborhood type {NEIGHBOR_TYPES}')
    
    

    status_df = spark.read.csv(status_path,
                                header=True,
                                sep=',',
                                inferSchema=True)

    status_df = status_df \
                .withColumn('time',
                            F.to_timestamp(F.regexp_replace('time', '-', '/'), 'yyyy/MM/dd HH:mm:ss'))

    station_df = spark.read.csv(station_path,
                                header=True,
                                sep=',',
                                inferSchema=True) \
                        .select('id', 'lat', 'long', 'dock_count', 'city')
    if CITIES != 'global':
        cities = set(CITIES.split(','))
        station_df = station_df.filter(station_df.city.isin(cities))

    status_df.printSchema()
    station_df.printSchema()
    
    joined_df = status_df \
                    .join(station_df,
                          on=status_df.station_id == station_df.id)
    
    joined_df.printSchema()
    filtered_df = joined_df \
                    .filter('(bikes_available + docks_available) <= (dock_count + 5) AND (bikes_available + docks_available) >= (dock_count - 5)')

    print(f'before: {joined_df.count()}')
    print(f'after: {filtered_df.count()}')
    
    station_time_rdd = timeslot_station_repartition(filtered_df, minutes=TEMPORAL_THRESHOLD)
    event_df = station_event_generation(station_time_rdd,
                                        event_set=event_to_generate,
                                        almost_threshold=2,
                                        almost_overlap_check=True) \
                .toDF().persist(ps.StorageLevel.MEMORY_AND_DISK)

    if not ABSOLUTE_MODE:
        print('relative mode')
        sg = SequenceGenerator(time_thr=datetime.timedelta(minutes=TEMPORAL_THRESHOLD),
                               t_time_steps=TEMPORAL_STEPS,
                               spatial_corr_func=spatial_cf,
                               event_type_col_name='event_type',
                               timestamp_col_name='timestamp',
                               latitude_col_name='lat',
                               longitude_col_name='long',
                               ignore_event_type=ignore_set,
                               mode=REPROJ_MODE)
    else:
        print('absolute mode')
        sg = SequenceAbsoluteGenerator(time_thr=datetime.timedelta(minutes=TEMPORAL_THRESHOLD),
                               t_time_steps=TEMPORAL_STEPS,
                               spatial_corr_func=spatial_cf,
                               event_type_col_name='event_type',
                               event_id_col_name='station',
                               timestamp_col_name='timestamp',
                               latitude_col_name='lat',
                               longitude_col_name='long',
                               ignore_event_type=ignore_set,
                               mode=REPROJ_MODE)
    event_df_final = sg.generate(event_df).persist(ps.StorageLevel.MEMORY_AND_DISK)
        
    se = SequenceExtractor(min_supp=MINSUPP, max_pattern_len=MAX_PATT_LEN, maxLocalProjDBSize=10_000_000)
    patterns = se.extract(event_df_final)
    
    if not ABSOLUTE_MODE:
        s0t0_filter = spark.udf.register('s0t0filter', lambda it: any('S0_T0' in x for x in it[0]), 'boolean')

        ordered_patterns = patterns.orderBy('relFreq', ascending=False) \
                                    .filter('s0t0filter(sequence)')
    else:
        ordered_patterns = patterns.orderBy('relFreq', ascending=False)
    
    ordered_patterns.withColumn('sequence', ordered_patterns.sequence.cast('string')) \
                    .write.csv(OUTPUT_PATH)
    
    sc.stop()