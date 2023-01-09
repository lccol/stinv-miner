#!/bin/bash
RESULT_BASEFOLDER="scalability_results/executors_fixed"
EXPORT_BASEFOLDER="scalability_results/plots"

python merge_scalability_results.py \
        -f $RESULT_BASEFOLDER/max_pattern_len6 \
        -o $EXPORT_BASEFOLDER/scalability_fixed_maxpattern.png \
        --files sf_1000events,sf_3000events,sf_5000events,sf_7000events,sf_10000events,sf_25000events,sf_50000events,sf_75000events,sf_100000events,sf_250000events,sf_500000events \
        -x "Number of events" \
        -y "Execution time (s)" \
        --max-x 100000 \
        --regex "sf_(\d+)events"
        
python merge_scalability_results.py \
        -f $RESULT_BASEFOLDER/events_5k_variable_pattern_len_v2 \
        -o $EXPORT_BASEFOLDER/scalability_variable_maxpattern.png \
        --files sf_maxlen5,sf_maxlen6,sf_maxlen7,sf_maxlen8,sf_maxlen9,sf_maxlen10,sf_maxlen15,sf_maxlen20,sf_maxlen25,sf_maxlen30,sf_maxlen40,sf_maxlen50 \
        -x "Maximum number of items" \
        -y "Execution time (s)" \
        --max-x 20 \
        --regex "sf_maxlen(\d+)"