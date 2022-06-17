import datetime
import pandas as pd
import time
import numpy as np

from pyspark.sql import Row, DataFrame
from pyspark import RDD
from sklearn.neighbors import BallTree
from typing import Union, List, Tuple, Set, Optional
from functools import partial

def extract_temporal_id(timestamp: Union[pd.Timestamp, datetime.datetime],
                        time_granularity: Union[pd.Timedelta, datetime.timedelta]) -> int:
    assert isinstance(timestamp, (pd.Timestamp, datetime.datetime))
    assert isinstance(time_granularity, (pd.Timedelta, datetime.timedelta))
    
    if isinstance(timestamp, datetime.datetime):
        unix_time = time.mktime(timestamp.timetuple())
    elif isinstance(timestamp, pd.Timestamp):
        unix_time = timestamp.timestamp()

    return int(unix_time // time_granularity.seconds)

def lag(item: Tuple[int, Row], n: int, check_idx: bool=True, mode: str='future') -> List[Tuple[int, Tuple[int, Row]]]:
    # mode == 'future' -> look into the future, so future events must be "lagged" to the past
    # mode == 'past' -> look into the past, so past events must be "reverse-lagged" into the future
    assert mode in {'future', 'past'}
    res = []
    for offset in range(0, n):
        new_index = item[0] - offset if mode == 'future' else item[0] + offset
        if (new_index >= 0 and check_idx) or not check_idx:
            res.append((new_index, (item[0], item[1])))

    return res

def groupby_time_idx(events: List[Tuple[int, Row]], mode: str='future') -> List[Tuple[int, List[Row]]]:
    assert mode in {'future', 'past'}
    acc = {}
    res = []
    min_idx, max_idx = None, None
    for t_idx, event in events:
        max_idx = max(t_idx, max_idx) if not max_idx is None else t_idx
        min_idx = min(t_idx, min_idx) if not min_idx is None else t_idx
        if not t_idx in acc:
            acc[t_idx] = []

        acc[t_idx].append(event)

    for idx in range(min_idx, max_idx + 1):
        res.append((idx, acc.get(idx, [])))
    if mode == 'past':
        res.reverse()
    return res    

def generate_seq(events: List[Tuple[int, List[Row]]],
                spatial_corr_func,
                event_type_key: str='eventType',
                ignore_event_set: Optional[Set[str]]=None,
                mode: str='future') -> List[List[List[str]]]:
    assert mode in {'future', 'past'}
    res = []
    events = [ev for _, ev in events]
    def _remove_empty_end_elements(l):
        while len(l) > 0 and len(l[-1]) == 0:
            l.pop()
        return l
    for center_idx in range(len(events[0])):
        center = events[0][center_idx]
        if not ignore_event_set is None and \
            center[event_type_key] in ignore_event_set:
            continue
        seq = []
        for time_delta in range(len(events)):
            spatial_corr_ev = []
            time_delta_write = time_delta if mode == 'future' else -time_delta
            for other_idx in range(len(events[time_delta])):
                other = events[time_delta][other_idx]
                spatial_delta = spatial_corr_func(center, other)
                if spatial_delta < 0:
                    continue
                spatial_corr_ev.append(f'{other[event_type_key]}_S{spatial_delta}_T{time_delta_write}')
            seq.append(spatial_corr_ev)
        seq = _remove_empty_end_elements(seq)
        if len(seq) > 0:
            res.append(seq)

    return res

def generate_absolute_seq(events: List[Tuple[int, List[Row]]],
                spatial_corr_func,
                event_type_key: str='eventType',
                event_id: str='id',
                ignore_event_set: Optional[Set[str]]=None,
                mode: str='future') -> List[List[List[str]]]:
    assert mode in {'future', 'past'}
    res = []
    events = [ev for _, ev in events]
    def _remove_empty_end_elements(l):
        while len(l) > 0 and len(l[-1]) == 0:
            l.pop()
        return l
    for center_idx in range(len(events[0])):
        center = events[0][center_idx]
        if not ignore_event_set is None and \
            center[event_type_key] in ignore_event_set:
            continue
        seq = []
        for time_delta in range(len(events)):
            spatial_corr_ev = []
            time_delta_write = time_delta if mode == 'future' else -time_delta
            for other_idx in range(len(events[time_delta])):
                other = events[time_delta][other_idx]
                spatial_delta = spatial_corr_func(center, other)
                if spatial_delta < 0:
                    continue
                spatial_corr_ev.append(f'{other[event_type_key]}_ID{other[event_id]}_T{time_delta_write}')
            seq.append(spatial_corr_ev)
        seq = _remove_empty_end_elements(seq)
        if len(seq) > 0:
            res.append(seq)

    return res

def generate_optimized_radius_sequence(events: List[Tuple[int, List[Row]]],
                                       spatial_corr_func,
                                       max_spatial_dist: float,
                                       max_temporal_delta: int,
                                       event_type_col_name: str='eventType',
                                       id_col_name: str='id',
                                       lat_col_name: str='latitudine',
                                       long_col_name: str='longitude',
                                       proj_type: str='relative') -> List[List[List[str]]]:
    assert proj_type in {'relative', 'absolute'}
    linearized_events_list = [(idx, x) for idx, y in events for x in y]
    data = np.array([[x[lat_col_name], x[long_col_name]] for _, y in events for x in y], dtype=float)
    data = np.deg2rad(data)
    
    first_slot_events = len(events[0][1])
    bt = BallTree(data, metric='haversine')
    angular_dist = max_spatial_dist / 6_371_000 # meters
    centers_data = data[:first_slot_events, :]
    
    neigh_list = bt.query_radius(centers_data, r=angular_dist)
    res = []
    for idx, neighbors in enumerate(neigh_list):
        subres = [[] for x in range(max_temporal_delta)]
        tidx, center = linearized_events_list[idx]
        for neigh in neighbors:
            tidy, other = linearized_events_list[neigh]                        
            spat_dist = spatial_corr_func(center, other)
            t_dist = tidy - tidx
            assert spat_dist >= 0
            
            if proj_type == 'relative':
                subres[abs(t_dist)].append(f'{other[event_type_col_name]}_S{spat_dist}_T{t_dist}')
            else:
                subres[abs(t_dist)].append(f'{other[event_type_col_name]}_ID{other[id_col_name]}_T{t_dist}')
        res.append(subres)
        
    return res
            

def optimized_radius_generation(rdd: RDD,
                                spatial_corr_func,
                                max_spatial_dist: float,
                                max_temporal_delta: int,
                                event_type_col_name: str='eventType',
                                id_col_name: str='id',
                                lat_col_name: str='latitudine',
                                long_col_name: str='longitude',
                                proj_type: str='relative') -> DataFrame:
    res = rdd.flatMapValues(partial(generate_optimized_radius_sequence,
                                    spatial_corr_func=spatial_corr_func,
                                    max_spatial_dist=max_spatial_dist,
                                    max_temporal_delta=max_temporal_delta,
                                    event_type_col_name=event_type_col_name,
                                    id_col_name=id_col_name,
                                    lat_col_name=lat_col_name,
                                    long_col_name=long_col_name,
                                    proj_type=proj_type
                                   )) \
                .values() \
                .map(lambda it: Row(sequence=it)) \
                .toDF()
    
    return res