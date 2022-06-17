#!/bin/bash

OUTPUT="datasets/SFBayAreaBikeSharing/patterns_REVISED/patterns_incoming_absolute_100m_DEF"
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
            --tag rc-inc100abs \
            --support 0 \
            --max-pattern-len 6 \
            --cities "Redwood City" \
            --event-types full,almost_full,increase,decrease \
            -o $OUTPUT/rc_incoming.csv \
            --absolute \
            --top-n 5 \
            --neigh-type incoming \
            --ignore-set decrease
            
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
            --tag pa-inc100abs \
            --support 0 \
            --max-pattern-len 6 \
            --cities "Palo Alto" \
            --event-types full,almost_full,increase,decrease \
            -o $OUTPUT/pa_incoming.csv \
            --absolute \
            --top-n 3 \
            --neigh-type incoming \
            --ignore-set decrease
            
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
            --tag mv-inc100abs \
            --support 0 \
            --max-pattern-len 6 \
            --cities "Mountain View" \
            --event-types full,almost_full,increase,decrease \
            -o $OUTPUT/mv_incoming.csv \
            --absolute \
            --top-n 5 \
            --neigh-type incoming \
            --ignore-set decrease


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
            --tag sf-inc100abs \
            --support 0 \
            --max-pattern-len 6 \
            --cities "San Francisco" \
            --event-types full,almost_full,increase,decrease \
            -o $OUTPUT/sf_incoming.csv \
            --absolute \
            --top-n 5 \
            --neigh-type incoming \
            --ignore-set decrease
            
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
            --tag sj-inc100abs \
            --support 0 \
            --max-pattern-len 6 \
            --cities "San Jose" \
            --event-types full,almost_full,increase,decrease \
            -o $OUTPUT/sj_incoming.csv \
            --absolute \
            --top-n 5 \
            --neigh-type incoming \
            --ignore-set decrease
            
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
            --tag gl-inc100abs \
            --support 0 \
            --max-pattern-len 6 \
            --cities global \
            --event-types full,almost_full,increase,decrease \
            -o $OUTPUT/global_incoming.csv \
            --absolute \
            --top-n 5 \
            --neigh-type incoming \
            --ignore-set decrease
