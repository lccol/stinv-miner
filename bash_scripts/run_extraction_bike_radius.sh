#!/bin/bash

OUTPUT="datasets/SFBayAreaBikeSharing/patterns_REVISED/patterns_radius_100m_DEF"
TEMPORAL_THR=10
SPATIAL_THR=100
TEMPORAL_STEPS=6
SPATIAL_STEPS=5

if [ -f stpm.zip ]
then
    rm stpm.zip
fi
    
zip -r stpm.zip stpm

# radius mode
# configuration 1
echo "Configuration 1 - SF radius"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=8g \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sf-rad100 \
            --support 0 \
            --max-pattern-len 6 \
            --cities "San Francisco" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/sf_radius.csv
            
# configuration 2
echo "Configuration 2 - San Jose radius"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=8g \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sj-rad100 \
            --support 0 \
            --max-pattern-len 6 \
            --cities "San Jose" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/sj_radius.csv
            
# configuration 3
echo "Configuration 3 - Redwood City radius"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=8g \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag rc-rad100 \
            --support 0 \
            --max-pattern-len 6 \
            --cities "Redwood City" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/rc_radius.csv
            
# configuration 4
echo "Configuration 4 - Palo Alto radius"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=8g \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag pa-rad100
            --support 0 \
            --max-pattern-len 6 \
            --cities "Palo Alto" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/pa_radius.csv
            
# configuration 5
echo "Configuration 5 - Mountain View radius"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=8g \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag mv-rad100 \
            --support 0 \
            --max-pattern-len 6 \
            --cities "Mountain View" \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/mv_radius.csv
            
# configuration 6
echo "Configuration 6 - Global radius"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=8g \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag gl-rad100 \
            --support 0 \
            --max-pattern-len 6 \
            --cities global \
            --event-types full,almost_full,increase \
            --neigh-type radius \
            -o $OUTPUT/global_radius.csv
