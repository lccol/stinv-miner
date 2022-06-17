import pandas as pd
import zipcodes

from pathlib import Path
from typing import Union

def zip_to_csv(city: str, output_path: Union[Path, str]) -> None:
    zips = zipcodes.filter_by(city=city)
    zip_city = [x['zip_code'] for x in zips]
    print(f'Found {len(zip_city)} for {city}')

    df = pd.DataFrame(zip_city, columns=['zip'])
    df.to_csv(output_path, index=False)
    return

if __name__ == '__main__':
    basepath = Path.cwd()
    zip_to_csv(city='New York',
                output_path=basepath / 'nyc_zip.csv')
    zip_to_csv(city='Boston',
                output_path=basepath / 'boston_zip.csv')
    zip_to_csv(city='Los Angeles',
                output_path=basepath / 'la_zip.csv')