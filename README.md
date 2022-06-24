# STInv-Miner
Official code repository for the STInv-Miner algorithm.

The code repository is organized as follows:
- `obtain_zips.py`: script used to retrieve zip codes for New York City, Boston and Los Angeles;
- `pattern_extraction_bike_sharing.py`: script used to run the STInv pattern extraction on the bike sharing dataset;
- `pattern_extraction_traffic_incidents.py`: script used to run the STInv pattern extraction on the LSTW dataset;
- `stpm` module: contains all the support classes to extract the STInvs;
- `bash_scripts` folder: contains all the bash scripts to execute the extraction with the tested configurations;
- `analyze_patterns_bike_sharing.ipynb`: notebook to compute some statistics on the patterns extracted from bike sharing dataset;
- `analyze_patterns_lstw.ipynb`: notebook to compute some statistics on the patterns extracted from LSTW dataset;
- `tree_to_seq_matching.ipynb`: performs matching between STInv patterns and tree-based patterns;

The code was tested with Python 3.7.9.
All packages are specified in `requirements.txt`.
