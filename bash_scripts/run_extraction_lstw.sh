#!/bin/bash

OUTPUT="datasets/ShortLongTermTrafficIncidents/patterns_REVISED"
TEMPORAL_THR=10
SPATIAL_THR=500
TEMPORAL_STEPS=3
SPATIAL_STEPS=3

if [ -f stpm.zip ]
then
    rm stpm.zip
fi
    
zip -r stpm.zip stpm
            
# configuration 1 - NYC
echo "Configuration 1 - NYC"
spark-submit --master yarn \
            --deploy-mode client \
            --py-files stpm.zip \
            --conf spark.executor.memory=8g \
            pattern_extraction_traffic_incidents.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag nyc-10 \
            --support 0 \
            --max-pattern-len 10 \
            --cities global \
            --zip-file nyc_zip.csv \
            -o $OUTPUT/patterns_nyc.csv
           
# configuration 2 - Boston
echo "Configuration 2 - Boston"
spark-submit --master yarn \
            --deploy-mode client \
            --py-files stpm.zip \
            --conf spark.executor.memory=8g \
            pattern_extraction_traffic_incidents.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag bo-10 \
            --support 0 \
            --max-pattern-len 10 \
            --cities global \
            --zip-file boston_zip.csv \
            -o $OUTPUT/patterns_boston.csv
           
# configuration 3 - LA
echo "Configuration 3 - LA"
spark-submit --master yarn \
            --deploy-mode client \
            --py-files stpm.zip \
            --conf spark.executor.memory=8g \
            pattern_extraction_traffic_incidents.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag la-10 \
            --support 0 \
            --max-pattern-len 10 \
            --cities global \
            --zip-file la_zip.csv \
            -o $OUTPUT/patterns_la.csv
            
# configuration 4 - global
echo "Configuration 4 - Global extraction"
spark-submit --master yarn \
            --deploy-mode client \
            --py-files stpm.zip \
            --conf spark.executor.memory=8g \
            pattern_extraction_traffic_incidents.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag gl-10 \
            --support 0 \
            --max-pattern-len 10 \
            --cities global \
            -o $OUTPUT/patterns_global.csv
