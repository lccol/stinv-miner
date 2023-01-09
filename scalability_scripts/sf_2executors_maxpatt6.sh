#!bin/bash

OUTPUT="datasets/SFBayAreaBikeSharing/patterns_scalability6"
TEMPORAL_THR=10
SPATIAL_THR=100
TEMPORAL_STEPS=6
SPATIAL_STEPS=5
TIME_EXPORT_PATH="scalability_results/executors_fixed/max_pattern_len6/SanFrancisco_run3"

if [ -f stpm.zip ]
then
    rm stpm.zip
fi
    
zip -r stpm.zip stpm

echo "Configuration 1 - SF radius"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=16g \
            --conf spark.yarn.queue="root.s278237" \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sj-sc1k \
            --support 0 \
            --max-pattern-len 6 \
            --cities "San Francisco" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/sf_radius_1000events.csv \
            --n-events 1000 \
            --exec-time $TIME_EXPORT_PATH/sf_1000events.csv
            
echo "Configuration 2 - SF radius"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=16g \
            --conf spark.yarn.queue="root.s278237" \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sj-sc3k \
            --support 0 \
            --max-pattern-len 6 \
            --cities "San Francisco" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/sf_radius_3000events.csv \
            --n-events 3000 \
            --exec-time $TIME_EXPORT_PATH/sf_3000events.csv
            
echo "Configuration 3 - SF radius"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=16g \
            --conf spark.yarn.queue="root.s278237" \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sj-sc5k \
            --support 0 \
            --max-pattern-len 6 \
            --cities "San Francisco" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/sf_radius_5000events.csv \
            --n-events 5000 \
            --exec-time $TIME_EXPORT_PATH/sf_5000events.csv
            
echo "Configuration 4 - SF radius"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=16g \
            --conf spark.yarn.queue="root.s278237" \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sj-sc7k \
            --support 0 \
            --max-pattern-len 6 \
            --cities "San Francisco" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/sf_radius_7000events.csv \
            --n-events 7000 \
            --exec-time $TIME_EXPORT_PATH/sf_7000events.csv
            
echo "Configuration 5 - SF radius"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=16g \
            --conf spark.yarn.queue="root.s278237" \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sj-sc10k \
            --support 0 \
            --max-pattern-len 6 \
            --cities "San Francisco" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/sf_radius_10000events.csv \
            --n-events 10000 \
            --exec-time $TIME_EXPORT_PATH/sf_10000events.csv
            
echo "Configuration 6 - SF radius"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=16g \
            --conf spark.yarn.queue="root.s278237" \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sj-sc25k \
            --support 0 \
            --max-pattern-len 6 \
            --cities "San Francisco" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/sf_radius_25000events.csv \
            --n-events 25000 \
            --exec-time $TIME_EXPORT_PATH/sf_25000events.csv
            
echo "Configuration 7 - SF radius"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=16g \
            --conf spark.yarn.queue="root.s278237" \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sj-sc50k \
            --support 0 \
            --max-pattern-len 6 \
            --cities "San Francisco" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/sf_radius_50000events.csv \
            --n-events 50000 \
            --exec-time $TIME_EXPORT_PATH/sf_50000events.csv
            
echo "Configuration 8 - SF radius"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=16g \
            --conf spark.yarn.queue="root.s278237" \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sj-sc75k \
            --support 0 \
            --max-pattern-len 6 \
            --cities "San Francisco" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/sf_radius_75000events.csv \
            --n-events 75000 \
            --exec-time $TIME_EXPORT_PATH/sf_75000events.csv
            
echo "Configuration 9 - SF radius"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=16g \
            --conf spark.yarn.queue="root.s278237" \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sj-sc100k \
            --support 0 \
            --max-pattern-len 6 \
            --cities "San Francisco" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/sf_radius_100000events.csv \
            --n-events 100000 \
            --exec-time $TIME_EXPORT_PATH/sf_100000events.csv
            
echo "Configuration 10 - SF radius"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=16g \
            --conf spark.yarn.queue="root.s278237" \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sj-sc250k \
            --support 0 \
            --max-pattern-len 6 \
            --cities "San Francisco" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/sf_radius_250000events.csv \
            --n-events 250000 \
            --exec-time $TIME_EXPORT_PATH/sf_250000events.csv
            
echo "Configuration 11 - SF radius"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=16g \
            --conf spark.yarn.queue="root.s278237" \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sj-sc500k \
            --support 0 \
            --max-pattern-len 6 \
            --cities "San Francisco" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/sf_radius_500000events.csv \
            --n-events 500000 \
            --exec-time $TIME_EXPORT_PATH/sf_500000events.csv