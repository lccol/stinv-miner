#!/bin/bash

OUTPUT="datasets/SFBayAreaBikeSharing/patterns_REVISED/patterns_incoming_100m_DEF"
TEMPORAL_THR=10
SPATIAL_THR=100
TEMPORAL_STEPS=6
SPATIAL_STEPS=5

if [ -f stpm.zip ]
then
    rm stpm.zip
fi
  
zip -r stpm.zip stpm
            
# incoming mode
# configuration 8
echo "Configuration 8 - San Jose incoming"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=8g \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sj-inc100 \
            --support 0 \
            --max-pattern-len 6 \
            --cities "San Jose" \
            --event-types full,almost_full,increase,decrease \
            --neigh-type incoming \
            --top-n 5 \
            --ignore-set decrease \
            -o $OUTPUT/sj_incoming.csv
            
# configuration 9
echo "Configuration 9 - Redwood City incoming"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=8g \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag rc-inc100 \
            --support 0 \
            --max-pattern-len 6 \
            --cities "Redwood City" \
            --event-types full,almost_full,increase,decrease \
            --neigh-type incoming \
            --top-n 5 \
            --ignore-set decrease \
            -o $OUTPUT/rc_incoming.csv
            
# configuration 10
echo "Configuration 10 - Palo Alto incoming"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=8g \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag pa-inc100 \
            --support 0 \
            --max-pattern-len 6 \
            --cities "Palo Alto" \
            --event-types full,almost_full,increase,decrease \
            --neigh-type incoming \
            --top-n 3 \
            --ignore-set decrease \
            -o $OUTPUT/pa_incoming.csv
            
# configuration 11
echo "Configuration 11 - Mountain View incoming"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=8g \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag mv-inc100 \
            --support 0 \
            --max-pattern-len 6 \
            --cities "Mountain View" \
            --event-types full,almost_full,increase,decrease \
            --neigh-type incoming \
            --top-n 5 \
            --ignore-set decrease \
            -o $OUTPUT/mv_incoming.csv


# configuration 7
echo "Configuration 7 - SF incoming"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=8g \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag sf-inc100 \
            --support 0 \
            --max-pattern-len 6 \
            --cities "San Francisco" \
            --event-types full,almost_full,increase,decrease \
            --neigh-type incoming \
            --top-n 5 \
            --ignore-set decrease \
            -o $OUTPUT/sf_incoming.csv
            
            
# configuration 12
echo "Configuration 12 - Global incoming"
spark-submit --master yarn \
            --deploy-mode client \
            --conf spark.executor.memory=8g \
            --py-files stpm.zip \
            pattern_extraction_bike_sharing.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag gl-inc100 \
            --support 0 \
            --max-pattern-len 6 \
            --cities global \
            --event-types full,almost_full,increase,decrease \
            --neigh-type incoming \
            --top-n 5 \
            --ignore-set decrease \
            -o $OUTPUT/global_incoming.csv
