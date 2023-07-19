# STInv-Miner
Official code repository for the STInv-Miner algorithm. This work was published at SIGSPATIAL'22 conference.

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

# Citation
```
@inproceedings{10.1145/3557915.3560998,
  author = {Colomba, Luca and Cagliero, Luca and Garza, Paolo},
  title = {Mining Spatiotemporally Invariant Patterns},
  year = {2022},
  isbn = {9781450395298},
  publisher = {Association for Computing Machinery},
  address = {New York, NY, USA},
  url = {https://doi.org/10.1145/3557915.3560998},
  doi = {10.1145/3557915.3560998},
  booktitle = {Proceedings of the 30th International Conference on Advances in Geographic Information Systems},
  articleno = {63},
  numpages = {4},
  keywords = {pattern mining, data mining, spatiotemporal data},
  location = {Seattle, Washington},
  series = {SIGSPATIAL '22}
}
```
