import numpy as np
import datetime
import time
import math
import pandas as pd
import pyspark as ps
import argparse

from pathlib import Path
from pyspark.sql import Row, DataFrame
from pyspark.sql import functions as F
from functools import partial
from sklearn.neighbors import BallTree

from pyspark.sql import SparkSession
from pyspark import SparkContext, RDD

from stpm.sequence_analyzer import SequenceGenerator, SequenceExtractor
from typing import Tuple, Union, Set, List
from collections import defaultdict

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

def spatial_corr_func(e1: Row,
                      e2: Row,
                      spat_thr: float, # meters
                      max_spat_range: int) -> int:
    max_dist = spat_thr * max_spat_range
    type1, type2 = e1['TypeWT'], e2['TypeWT']
    if type1 == type2 and type1 == 'W':
        return -1
    if type1 == type2:
        # both are traffic incidents (T)
        dist = haversineDistance(e1['LocationLat'],
                                 e1['LocationLng'],
                                 e2['LocationLat'],
                                 e2['LocationLng'])
        if dist > max_dist:
            return -1
        return math.ceil(dist / spat_thr)
    else:
        if e1['AirportCode'] == e2['AirportCode']:
            return 0
        else:
            return -1
        
def generate_optimized_radius_sequence(events: List[Tuple[int, List[Row]]],
                                       spatial_corr_func,
                                       max_spatial_dist: float,
                                       max_temporal_delta: int) -> List[List[List[str]]]:
    linearized_events_list = [(idx, x) for idx, y in events for x in y]
    linearized_traffic_events = [x for x in linearized_events_list if x[1]['TypeWT'] == 'T']
    linearized_weather_events = [x for x in linearized_events_list if x[1]['TypeWT'] == 'W']
    
    weather_dict = defaultdict(list)
    for ev in linearized_weather_events:
        weather_dict[ev[1]['AirportCode']].append(ev)
    
    data = np.array([[x['LocationLat'], x['LocationLng']] for _, x in linearized_traffic_events], dtype=float)
    if np.isnan(data).any():
        print(events)
    data = np.deg2rad(data)
    
    first_slot_events = len([x for x in events[0][1] if x['TypeWT'] == 'T'])
    if first_slot_events == 0:
        return []
    bt = BallTree(data, metric='haversine')
    angular_dist = max_spatial_dist / 6_371_000 # meters
    centers_data = data[:first_slot_events, :]

    neigh_list = bt.query_radius(centers_data, r=angular_dist)
    res = []
    for idx, neighbors in enumerate(neigh_list):
        subres = [[] for x in range(max_temporal_delta)]
        tidx, center = linearized_traffic_events[idx]
        for neigh in neighbors:
            tidy, other = linearized_traffic_events[neigh]
            spat_dist = spatial_corr_func(center, other)
            t_dist = tidy - tidx
            assert spat_dist >= 0
            
            subres[abs(t_dist)].append(f'{other["RefinedType"]}_S{spat_dist}_T{t_dist}')
        for idy, neigh in weather_dict[center['AirportCode']]:
            spat_dist = spatial_corr_func(center, neigh)
            t_dist = idy - tidx
            assert spat_dist == 0
            if spat_dist >= 0:
                subres[abs(t_dist)].append(f'{other["RefinedType"]}_S{spat_dist}_T{t_dist}')
            
        res.append(subres)
        
    return res
            

def optimized_radius_generation(rdd: RDD,
                                spatial_corr_func,
                                max_spatial_dist: float,
                                max_temporal_delta: int) -> DataFrame:
    res = rdd.flatMapValues(partial(generate_optimized_radius_sequence,
                                    spatial_corr_func=spatial_corr_func,
                                    max_spatial_dist=max_spatial_dist,
                                    max_temporal_delta=max_temporal_delta
                                   )) \
                .values() \
                .map(lambda it: Row(sequence=it)) \
                .toDF()
    
    return res

def parse():
    parser = argparse.ArgumentParser(description='Pattern extraction on LSTW dataset')
    parser.add_argument('-t', type=int, help='Time threshold in minutes')
    parser.add_argument('-s', type=int, help='Spatial threshold in meters')
    parser.add_argument('-nt', type=int, help='Number of temporal steps')
    parser.add_argument('-ns', type=int, help='Number of spatial steps')
    parser.add_argument('--tag', type=str, default='', help='Tag for spark job.')
    parser.add_argument('--support', type=float, help='Minimum support used for extraction')
    parser.add_argument('--max-pattern-len', dest='max_pattern_len', type=int, default=10, help='Maximum pattern (total number of elements) length.')
    parser.add_argument('--cities', type=str, help='Cities on which the extraction is performed. If "all", extraction is performed for each city separately; if "global", a single global extraction is performed on the entire dataset or on the zip codes specified in the zip-file option; otherwise it contains the cities names, comma-separated. Given a comma-separated list of cities, a single extraction is performed over all the cities.')
    parser.add_argument('--zip-file', type=str, default='', dest='zip_file', help='Zip codes on which the extraction is performed if cities == "global", otherwise it is ignored.')
    parser.add_argument('-o', type=str, help='Output path for the extracted patterns on HDFS.')
    parser.add_argument('--optimize-neighborhood', default=False, action='store_true', dest='optimized_neighborhood', help='Enables BallTree search on neighbors.')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse()
    TEMPORAL_THRESHOLD = args.t
    SPATIAL_THRESHOLD = args.s
    TEMPORAL_STEPS = args.nt
    SPATIAL_STEPS = args.ns
    TAG = args.tag
    CITY_CONFIG = args.cities
    MIN_SUPP = args.support
    MAX_PATT_LEN = args.max_pattern_len
    OUTPUT_PATH = args.o
    ZIP_FILE = args.zip_file
    OPTIM_SEARCH = args.optimized_neighborhood
    if CITY_CONFIG == 'all':
        PER_CITY_EXTRACTION = True
        cities = None
    elif CITY_CONFIG == 'global':
        PER_CITY_EXTRACTION = False
        cities = None
    else:
        PER_CITY_EXTRACTION = False
        cities = set(CITY_CONFIG.split(','))
        
    use_spatial_corr_func = not OPTIM_SEARCH
    spatial_corr_func = partial(spatial_corr_func,
                                 spat_thr=SPATIAL_THRESHOLD,
                                 max_spat_range=SPATIAL_STEPS)
    analyze_func = partial(optimized_radius_generation,
                           spatial_corr_func=spatial_corr_func,
                           max_spatial_dist=SPATIAL_THRESHOLD * SPATIAL_STEPS,
                           max_temporal_delta=TEMPORAL_STEPS) if OPTIM_SEARCH else None
    
    spark = SparkSession.builder \
                    .appName(f'{TAG}') \
                    .getOrCreate()
    sc = spark.sparkContext
#     sc.addPyFile('stpm.zip')
    
    print('#' * 50)
    print(f'TEMPORAL THRESHOLD: {TEMPORAL_THRESHOLD}')
    print(f'SPATIAL THRESHOLD: {SPATIAL_THRESHOLD}')
    print(f'TEMPORAL STEPS: {TEMPORAL_STEPS}')
    print(f'SPATIAL STEPS: {SPATIAL_STEPS}')
    print(f'CITY CONFIGURATION: {CITY_CONFIG}')
    print(f'MIN SUPPORT: {MIN_SUPP}')
    print(f'MAX SEQ LEN: {MAX_PATT_LEN}')
    print(f'OUTPUT PATH: {OUTPUT_PATH}')
    print(f'ZIP FILE: {ZIP_FILE}')
    print(f'Optim search: {OPTIM_SEARCH}')
    
    print('#' * 50)
    
    data_path = 'datasets/ShortLongTermTrafficIncidents/AllEvents_Distinct_original.csv'

    df = spark.read.csv(data_path,
                        header=True,
                        sep=',',
                        inferSchema=True,
                        nullValue='N/A') \
                    .withColumnRenamed('Type(W/T)', 'TypeWT') \
                    .withColumnRenamed('StartTime(UTC)', 'StartTimeUTC') \
                    .withColumnRenamed('EndTime(UTC)', 'EndTimeUTC') \
                    .withColumnRenamed('Distance(mi)', 'DistanceMi')
    columns_of_interest = [
        'TypeWT',
        'RefinedType',
        'StartTimeUTC',
        'LocationLat',
        'LocationLng',
        'AirportCode',
        'ZipCode',
        'City'
    ]

    df = df.select(*columns_of_interest) \
            .filter('StartTimeUTC IS NOT NULL')
    
    df = df.replace({
        'Rain-Light': 'Rain',
        'Rain-Moderate': 'Rain',
        'Rain-Heavy': 'Rain',
        'Fog-Severe': 'Fog',
        'Fog-Moderate': 'Fog',
        'Snow-Light': 'Snow',
        'Snow-Moderate': 'Snow',
        'Snow-Heavy': 'Snow',
        'Cold-Severe': 'Cold',
        'Storm-Severe': 'Storm'
    }, subset=['RefinedType'])
    
    if PER_CITY_EXTRACTION:
        df = df.cache()
    df.printSchema()
    
    weather_refined_type = df.filter(df['TypeWT'] == 'W') \
                            .select('RefinedType') \
                            .distinct() \
                            .collect()
    weather_refined_type = set(x['RefinedType'] for x in weather_refined_type)
    
    def generate_and_extract(df: DataFrame,
                         temporal_threshold: int, # minutes
                         t_time_steps: int, # number of steps
                         spatial_threshold: int, # meters
                         spatial_steps: int, # number of steps
                         ignore_event_set: Set[str],
                         min_supp: float=0.0,
                         max_seq_len: int=10) -> Tuple[DataFrame, DataFrame]:
        seq_gen = SequenceGenerator(time_thr=datetime.timedelta(minutes=temporal_threshold),
                                    t_time_steps=t_time_steps,
                                    spatial_corr_func=spatial_corr_func,
                                    ignore_event_type=ignore_event_set,
                                    event_type_col_name='RefinedType',
                                    timestamp_col_name='StartTimeUTC',
                                    latitude_col_name='LocationLat',
                                    longitude_col_name='LocationLng',
                                    use_spatial_corr_func=use_spatial_corr_func,
                                    analyze_func=analyze_func)
        event_df = seq_gen.generate(df).persist(ps.StorageLevel.MEMORY_AND_DISK)
        print(f'Analyzing {event_df.count()} events')
        seq_extractor = SequenceExtractor(min_supp=min_supp,
                                          max_pattern_len=max_seq_len,
                                          maxLocalProjDBSize=10_000_000)
        patterns = seq_extractor.extract(event_df).persist(ps.StorageLevel.MEMORY_AND_DISK)
        return event_df, patterns
    
    if PER_CITY_EXTRACTION:
        cities = set(df.select('City') \
                         .filter('City IS NOT NULL AND length(trim(City)) > 0') \
                         .distinct() \
                         .rdd \
                         .map(lambda it: it['City']) \
                     .collect())
        print(f'Cities: {cities}')
        for idx, c in enumerate(cities):
            print(f'Analyzing city {c} ({idx + 1}/{len(cities)})')
            # keep weather events and traffic events for that specific city
            filtered_df = df.filter(f'((City == "{c}") AND (TypeWT == "T")) OR (TypeWT == "W")')
            event_df, patterns_df = generate_and_extract(filtered_df,
                                                         temporal_threshold=TEMPORAL_THRESHOLD,
                                                         t_time_steps=TEMPORAL_STEPS,
                                                         spatial_threshold=SPATIAL_THRESHOLD,
                                                         spatial_steps=SPATIAL_STEPS,
                                                         ignore_event_set=weather_refined_type,
                                                         min_supp=MIN_SUPP,
                                                         max_seq_len=MAX_PATT_LEN)
            patterns_df.withColumn('sequence', patterns_df.sequence.cast('string')) \
                        .write.csv(OUTPUT_PATH)
            patterns_df.unpersist()
            event_df.unpersist()
            print('done')
    else:
        if cities is None:
            print('starting generation and extraction for all cities')
            if len(ZIP_FILE) == 0:
                filtered_df = df
                print('no filtering')
            else:
                zip_df = pd.read_csv(ZIP_FILE)
                zip_df['zip'] = zip_df['zip'].astype(int)
                zip_set = set(zip_df['zip'].tolist())
                print(f'filtering zip codes: {zip_set}')
                filtered_df1 = df.filter((df.ZipCode.isin(zip_set) & (df.TypeWT == 'T')) | (df.TypeWT == 'W'))
                selected_airport_codes = filtered_df1 \
                                            .filter('TypeWT == "T" AND AirportCode IS NOT NULL') \
                                            .select('AirportCode') \
                                            .distinct() \
                                            .collect()
                selected_airport_codes = set(x['AirportCode'] for x in selected_airport_codes)
                filtered_df = filtered_df1.filter((filtered_df1.TypeWT == 'T') | ((filtered_df1.TypeWT == 'W') & (filtered_df1.AirportCode.isin(selected_airport_codes))))
        else:
            print(f'starting generation and extraction for the following cities: {cities}')
            filtered_df1 = df.filter((df.City.isin(cities) & (df.TypeWT == 'T')) | (df.TypeWT == 'W'))
            selected_airport_codes = filtered_df1 \
                                        .filter('TypeWT == "T" AND AirportCode IS NOT NULL') \
                                        .select('AirportCode') \
                                        .distinct() \
                                        .collect()
            selected_airport_codes = set(x['AirportCode'] for x in selected_airport_codes)
            filtered_df = filtered_df1.filter((filtered_df1.TypeWT == 'T') | ((filtered_df1.TypeWT == 'W') & (filtered_df1.AirportCode.isin(selected_airport_codes))))
        event_df, patterns_df = generate_and_extract(filtered_df,
                                                         temporal_threshold=TEMPORAL_THRESHOLD,
                                                         t_time_steps=TEMPORAL_STEPS,
                                                         spatial_threshold=SPATIAL_THRESHOLD,
                                                         spatial_steps=SPATIAL_STEPS,
                                                         ignore_event_set=weather_refined_type,
                                                         min_supp=MIN_SUPP,
                                                         max_seq_len=MAX_PATT_LEN)
        patterns_df.withColumn('sequence', patterns_df.sequence.cast('string')) \
                    .write.csv(OUTPUT_PATH)
        event_df.unpersist()
        patterns_df.unpersist()
        print('done')
        
    sc.stop()