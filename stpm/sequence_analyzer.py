import datetime
import pandas as pd
import pyspark as ps
import math

from functools import partial
from .utils import extract_temporal_id, groupby_time_idx, lag, generate_seq, generate_absolute_seq
from pyspark.sql import Row, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.ml.fpm import PrefixSpan

from typing import Union, Optional, List, Dict, Tuple, Set

class SequenceGenerator:
    def __init__(self,
                time_thr: Union[datetime.timedelta, pd.Timedelta],
                t_time_steps: int,
                spatial_corr_func,
                ignore_event_type: Optional[Set[str]]=None,
                event_type_col_name: Optional[str]='eventType',
                timestamp_col_name: Optional[str]='timestamp',
                latitude_col_name: Optional[str]='latitude',
                longitude_col_name: Optional[str]='longitude',
                use_spatial_corr_func: bool=True,
                analyze_func=None,
                mode: str='future') -> None:
        self.time_thr = time_thr
        self.t_time_steps = t_time_steps
        self.spatial_corr_func = spatial_corr_func
        self.ignore_event_type = ignore_event_type
        self.event_type_col_name = event_type_col_name
        self.timestamp_col_name = timestamp_col_name
        self.latitude_col_name = latitude_col_name
        self.longitude_col_name = longitude_col_name
        self.use_spatial_corr_func = use_spatial_corr_func
        self.analyze_func = analyze_func
        self.mode = mode
        
        assert mode in {'future', 'past'}
        assert not analyze_func is None or \
                (use_spatial_corr_func and not spatial_corr_func is None)

        return

    def generate(self, df: DataFrame) -> DataFrame:
        grouped_rdd = df.rdd \
                        .map(lambda row: (extract_temporal_id(row[self.timestamp_col_name],
                                                            self.time_thr),
                                        row)) \
                        .cache()

        min_idx = grouped_rdd.keys().min()
        grouped_rdd_norm = grouped_rdd.map(lambda it: (it[0] - min_idx, it[1]))

        lagged_func = partial(lag, n=self.t_time_steps, check_idx=True, mode=self.mode)
        # after groupByKey
        # key = time index
        # value = list of (original time idx, event)
        # if original time idx == key, it indicates that there is at least one event in the key time slots,
        # i.e. the timeslot in key is not composed only by lagged events from other time slots
        window_rdd = grouped_rdd_norm.flatMap(lagged_func) \
                                    .groupByKey() \
                                    .filter(lambda it: any(x[0] == it[0] for x in it[1])) \
                                    .mapValues(partial(groupby_time_idx, mode=self.mode)) \
                                    .cache()
        
        grouped_rdd.unpersist()
                                    
        if not self.use_spatial_corr_func:
            res = self.analyze_func(window_rdd)
        else:
#             max_len = window_rdd.values().map(lambda it: sum([len(x) for _, x in it])).max()
#             print(f'max numb of events: {max_len}')
            res = window_rdd.flatMapValues(partial(generate_seq,
                                                    spatial_corr_func=self.spatial_corr_func,
                                                    event_type_key=self.event_type_col_name,
                                                    ignore_event_set=self.ignore_event_type,
                                                    mode=self.mode)) \
                            .values() \
                            .map(lambda it: Row(sequence=it)) \
                            .toDF()
    
        window_rdd.unpersist()

        return res

class SequenceContinuousGenerator(SequenceGenerator):
    def __init__(self,
                time_thr: Union[datetime.timedelta, pd.Timedelta],
                t_time_steps: int,
                spatial_corr_func,
                ignore_event_type: Optional[Set[str]]=None,
                event_type_col_name: Optional[str]='eventType',
                timestamp_col_name: Optional[str]='timestamp',
                latitude_col_name: Optional[str]='latitude',
                longitude_col_name: Optional[str]='longitude',
                use_spatial_corr_func: bool=True,
                analyze_func=None,
                mode: str='future') -> None:
        super().__init__(time_thr,
                        t_time_steps,
                        spatial_corr_func,
                        ignore_event_type,
                        event_type_col_name,
                        timestamp_col_name,
                        latitude_col_name,
                        longitude_col_name,
                        use_spatial_corr_func,
                        analyze_func,
                        mode)
        return

    def generate(self, df: DataFrame) -> DataFrame:
        offset_seconds = int(self.time_thr.total_seconds() * self.t_time_steps - 1)
        if self.mode == 'future':
            w_tuple = (0, offset_seconds)
        else:
            w_tuple = (-offset_seconds, 0)
        assert not 'unix_ts' in df.columns
        unix_df = df.withColumn('unix_ts', F.unix_timestamp(self.timestamp_col_name))
        min_unix_ts = unix_df.select(F.min('unix_ts')).collect()[0]['min(unix_ts)']
        normalized_unix_df = unix_df.withColumn('unix_ts', unix_df.unix_ts - min_unix_ts)

        w = Window.orderBy('unix_ts').rangeBetween(*w_tuple)
        assert not 'accumulator' in normalized_unix_df.columns
        grouped_df = normalized_unix_df.withColumn('accumulator',
                                                    F.collect_list(
                                                        F.struct(*[x for x in df.columns])
                                                    ).over(w)
                                                )

        grouped_rdd = grouped_df.rdd #.persist(ps.StorageLevel.MEMORY_AND_DISK)
        def _project(row: Row, cols: List[str]) -> Row:
            acc = row['accumulator']
            center = Row(**{x: row[x] for x in cols})
            res = [[] for _ in range(self.t_time_steps + 1)]
            for other in acc:
                delta_time = (other[self.timestamp_col_name] - center[self.timestamp_col_name]).total_seconds()
                discr_temp_dist = math.ceil(delta_time / self.time_thr.total_seconds())
                spatial_dist = self.spatial_corr_func(center, other)
                if spatial_dist < 0:
                    continue
                if self.mode == 'past':
                    discr_temp_dist *= -1
                res[abs(discr_temp_dist)].append(f'{other[self.event_type_col_name]}_S{spatial_dist}_T{discr_temp_dist}')

            return Row(sequence=res)
        
        return grouped_rdd.map(partial(_project, cols=df.columns)).toDF()

class SequenceAbsoluteGenerator(SequenceGenerator):
    def __init__(self,
                time_thr: Union[datetime.timedelta, pd.Timedelta],
                t_time_steps: int,
                spatial_corr_func,
                ignore_event_type: Optional[Set[str]] = None,
                event_type_col_name: Optional[str] = 'eventType',
                event_id_col_name: Optional[str]='id',
                timestamp_col_name: Optional[str] = 'timestamp',
                latitude_col_name: Optional[str] = 'latitude',
                longitude_col_name: Optional[str] = 'longitude',
                use_spatial_corr_func: bool = True,
                analyze_func=None,
                mode: str = 'future') -> None:
        super().__init__(time_thr,
                        t_time_steps,
                        spatial_corr_func,
                        ignore_event_type,
                        event_type_col_name,
                        timestamp_col_name,
                        latitude_col_name,
                        longitude_col_name,
                        use_spatial_corr_func,
                        analyze_func,
                        mode)
        self.event_id_col_name = event_id_col_name
        return

    def generate(self, df: DataFrame) -> DataFrame:
        grouped_rdd = df.rdd \
                        .map(lambda row: (extract_temporal_id(row[self.timestamp_col_name],
                                                            self.time_thr),
                                        row)) \
                        .cache()

        min_idx = grouped_rdd.keys().min()
        grouped_rdd_norm = grouped_rdd.map(lambda it: (it[0] - min_idx, it[1]))

        lagged_func = partial(lag, n=self.t_time_steps, check_idx=True, mode=self.mode)
        # after groupByKey
        # key = time index
        # value = list of (original time idx, event)
        # if original time idx == key, it indicates that there is at least one event in the key time slots,
        # i.e. the timeslot in key is not composed only by lagged events from other time slots
        window_rdd = grouped_rdd_norm.flatMap(lagged_func) \
                                    .groupByKey() \
                                    .filter(lambda it: any(x[0] == it[0] for x in it[1])) \
                                    .mapValues(partial(groupby_time_idx, mode=self.mode)) \
                                    .cache()
        
        grouped_rdd.unpersist()
                                    
        if not self.use_spatial_corr_func:
            res = self.analyze_func(window_rdd)
        else:
#             max_len = window_rdd.values().map(lambda it: sum([len(x) for _, x in it])).max()
#             print(f'max numb of events: {max_len}')
            res = window_rdd.flatMapValues(partial(generate_absolute_seq,
                                                    spatial_corr_func=self.spatial_corr_func,
                                                    event_type_key=self.event_type_col_name,
                                                    event_id=self.event_id_col_name,
                                                    ignore_event_set=self.ignore_event_type,
                                                    mode=self.mode)) \
                            .values() \
                            .map(lambda it: Row(sequence=it)) \
                            .toDF()
    
        window_rdd.unpersist()

        return res
        
class SequenceExtractor:
    def __init__(self,
                 min_supp: float,
                 max_pattern_len: int,
                 maxLocalProjDBSize: int=32000000,
                 sequenceCol: str='sequence',
                 spark: Optional[SparkSession]=None) -> None:
        self.min_supp = min_supp
        self.max_pattern_len = max_pattern_len
        self.maxLocalProjDBSize = maxLocalProjDBSize
        self.sequenceCol = sequenceCol
        self.spark = spark if not spark is None else SparkSession.builder.getOrCreate()
        
        self.prefix_span = PrefixSpan(minSupport=min_supp,
                                      maxPatternLength=max_pattern_len,
                                      maxLocalProjDBSize=maxLocalProjDBSize,
                                      sequenceCol=sequenceCol)
        return
    
    def extract(self, df: DataFrame) -> DataFrame:
        patterns = self.prefix_span.findFrequentSequentialPatterns(df)
        tot = df.count()
        patterns = self.compute_confidence(patterns, total=tot)
        return patterns
    
    def compute_confidence(self, df: DataFrame, total: int) -> DataFrame:
        df = df.selectExpr(f'freq / {total} as relFreq', '*')
        self.spark.udf.register('compute_subsequence', lambda it: it[:-1])
        
        df2 = df
        for col in df2.columns:
            df2 = df2.withColumnRenamed(col, f'{col}_second')
        df = df.selectExpr('compute_subsequence(sequence) as subseq', '*')
        
        df = df.join(df2, df.subseq.cast('string') == df2.sequence_second.cast('string'), how='left_outer') \
                .selectExpr('freq', 'relFreq', 'freq / freq_second as confidence', 'sequence')
        return df