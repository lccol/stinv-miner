#!bin/bash

OUTPUT="datasets/SFBayAreaBikeSharing/patterns_scalability_tmp"
TEMPORAL_THR=10
SPATIAL_THR=100
TEMPORAL_STEPS=6
SPATIAL_STEPS=5
N_EVENTS=5000
TIME_EXPORT_PATH="scalability_results/executors_fixed/events_5k_variable_pattern_len_v2/SanFrancisco_run3"

if [ -f stpm.zip ]
then
    rm stpm.zip
fi
    
zip -r stpm.zip stpm

MAXLEN=5
echo "Configuration 1 - SF radius maxlen $MAXLEN"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=32g \
            --conf spark.yarn.queue="root.s278237" \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sj-sc6 \
            --support 0 \
            --max-pattern-len $MAXLEN \
            --cities "San Francisco" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/sf_maxlen$MAXLEN.csv \
            --n-events $N_EVENTS \
            --exec-time $TIME_EXPORT_PATH/sf_maxlen$MAXLEN.csv
            
MAXLEN=6
echo "Configuration - SF radius maxlen $MAXLEN"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=32g \
            --conf spark.yarn.queue="root.s278237" \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sj-sc6 \
            --support 0 \
            --max-pattern-len $MAXLEN \
            --cities "San Francisco" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/sf_maxlen$MAXLEN.csv \
            --n-events $N_EVENTS \
            --exec-time $TIME_EXPORT_PATH/sf_maxlen$MAXLEN.csv
            
MAXLEN=7
echo "Configuration - SF radius maxlen $MAXLEN"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=32g \
            --conf spark.yarn.queue="root.s278237" \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sj-sc6 \
            --support 0 \
            --max-pattern-len $MAXLEN \
            --cities "San Francisco" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/sf_maxlen$MAXLEN.csv \
            --n-events $N_EVENTS \
            --exec-time $TIME_EXPORT_PATH/sf_maxlen$MAXLEN.csv
                
MAXLEN=8
echo "Configuration - SF radius maxlen $MAXLEN"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=32g \
            --conf spark.yarn.queue="root.s278237" \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sj-sc6 \
            --support 0 \
            --max-pattern-len $MAXLEN \
            --cities "San Francisco" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/sf_maxlen$MAXLEN.csv \
            --n-events $N_EVENTS \
            --exec-time $TIME_EXPORT_PATH/sf_maxlen$MAXLEN.csv
            
MAXLEN=9
echo "Configuration - SF radius maxlen $MAXLEN"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=32g \
            --conf spark.yarn.queue="root.s278237" \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sj-sc6 \
            --support 0 \
            --max-pattern-len $MAXLEN \
            --cities "San Francisco" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/sf_maxlen$MAXLEN.csv \
            --n-events $N_EVENTS \
            --exec-time $TIME_EXPORT_PATH/sf_maxlen$MAXLEN.csv
            
MAXLEN=10
echo "Configuration - SF radius maxlen $MAXLEN"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=32g \
            --conf spark.yarn.queue="root.s278237" \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sj-sc6 \
            --support 0 \
            --max-pattern-len $MAXLEN \
            --cities "San Francisco" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/sf_maxlen$MAXLEN.csv \
            --n-events $N_EVENTS \
            --exec-time $TIME_EXPORT_PATH/sf_maxlen$MAXLEN.csv
            
MAXLEN=15
echo "Configuration - SF radius maxlen $MAXLEN"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=32g \
            --conf spark.yarn.queue="root.s278237" \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sj-sc6 \
            --support 0 \
            --max-pattern-len $MAXLEN \
            --cities "San Francisco" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/sf_maxlen$MAXLEN.csv \
            --n-events $N_EVENTS \
            --exec-time $TIME_EXPORT_PATH/sf_maxlen$MAXLEN.csv
            
MAXLEN=20
echo "Configuration - SF radius maxlen $MAXLEN"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=32g \
            --conf spark.yarn.queue="root.s278237" \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sj-sc6 \
            --support 0 \
            --max-pattern-len $MAXLEN \
            --cities "San Francisco" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/sf_maxlen$MAXLEN.csv \
            --n-events $N_EVENTS \
            --exec-time $TIME_EXPORT_PATH/sf_maxlen$MAXLEN.csv