import pyspark as ps
import pandas as pd

from pyspark import SparkContext
from pyspark.sql import Row, SparkSession, DataFrame

from typing import Union, List, Dict, Optional, Tuple
from collections import defaultdict
from functools import partial

def sum_delta_distrib(r: List[List[str]], delta_type: str) -> Tuple[int, int]:
    assert delta_type in {'spatial', 'temporal'}
    idx = -2 if delta_type == 'spatial' else -1
    res = 0
    count = 0
    
    for sublist in r:
        for el in sublist:
            reference = el.split('_')[idx]
            value = int(reference[1:])
            res += value
            count += 1
    
    return (res, count)
    
    
def compute_mean_delta(df: DataFrame, delta_type: str) -> float:
    res = df.rdd \
            .map(lambda it: it['sequence']) \
            .map(partial(sum_delta_distrib, delta_type=delta_type)) \
            .reduce(lambda i1, i2: (i1[0] + i2[0], i1[1] + i2[1]))
    
    return res[0] / res[1]

def count_n_elements(pattern: List[List[str]]) -> int:
    count = 0
    for sublist in pattern:
        count += len(sublist)
    return count

def count_delta_diff_from_zero(pattern: List[List[str]], delta_type: str) -> int:
    assert delta_type in {'spatial', 'temporal', 'both'}
    spatial_set = set()
    temporal_set = set()
    for sublist in pattern:
        for el in sublist:
            split = el.split('_')
            spatial, temporal = split[-2], split[-1]
            spatial, temporal = int(spatial[1:]), int(temporal[1:])
            if spatial != 0:
                spatial_set.add(el)
            if temporal != 0:
                temporal_set.add(el)
    if delta_type == 'spatial':
        return len(spatial_set)
    elif delta_type == 'temporal':
        return len(temporal_set)
    else:
        return len(spatial_set.intersection(temporal_set))