#!/bin/bash

OUTPUT="datasets/ShortLongTermTrafficIncidents/patterns_REVISED"
TEMPORAL_THR=10
SPATIAL_THR=322
TEMPORAL_STEPS=3
SPATIAL_STEPS=3

if [ -f stpm.zip ]
then
    rm stpm.zip
fi
    
zip -r stpm.zip stpm
            
            
# configuration 1 - Boston
echo "Configuration 1 - Boston single-step"
spark-submit --master yarn \
            --deploy-mode client \
            --py-files stpm.zip \
            --conf spark.executor.memory=8g \
            pattern_extraction_traffic_incidents.py \
            -t $TEMPORAL_THR \
            -s $SPATIAL_THR \
            -nt $TEMPORAL_STEPS \
            -ns $SPATIAL_STEPS \
            --tag bo-25-single \
            --support 0 \
            --max-pattern-len 25 \
            --cities global \
            --zip-file boston_zip.csv \
            -o $OUTPUT/patterns_boston_kdd_singlestep.csv