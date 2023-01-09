import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import re

from pathlib import Path
from collections import defaultdict

def parse():
    parser = argparse.ArgumentParser(description='Parse scalability experiments and generate plots.')
    parser.add_argument('-f', dest='folder', type=str, help='Folder in which results are stored')
    parser.add_argument('-o', dest='output', type=str, help='Output path for resulting image.')
    parser.add_argument('--files', dest='files', type=str, help='File list to be analyzed, without the csv extension')
    parser.add_argument('-x', type=str, help='X title')
    parser.add_argument('-y', type=str, help='Y title')
    parser.add_argument('--max-x', dest='max_x', type=int, default=None, help='Maximum x axis value')
    parser.add_argument('--regex', dest='regex', type=str, help='Regex to extract the number from csv filename')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse()
    INPUT_FOLDER = args.folder
    DEST_FOLDER = args.output
    FILE_LIST = args.files.split(',')
    XTITLE = args.x
    YTITLE = args.y
    REGEX = args.regex
    MAX_X = args.max_x
    
    # plt.style.use('ggplot')
    
    basepath = Path(INPUT_FOLDER)
    
    plot_savepath = Path(DEST_FOLDER)
    
    if not plot_savepath.parent.is_dir():
        plot_savepath.mkdir(parents=True)
    
    folder_list = [
        'SanFrancisco_run1',
        'SanFrancisco_run2',
        'SanFrancisco_run3'
    ]
    
    filenames = [x + '.csv' for x in FILE_LIST]
    
    folder_full_list = [basepath / x for x in folder_list]
    
    assert all(x.is_dir() for x in folder_full_list)
    
    df_dict = defaultdict(list)
    df_dict['filename'].extend(filenames)
    
    pattern = re.compile(REGEX)
    nevents_list = [int(pattern.search(s).group(1)) for s in filenames]
    df_dict['feature'].extend(nevents_list)
    
    for dir_path in folder_full_list:
        for f in filenames:
            fullpath = dir_path / f
            df = pd.read_csv(fullpath)
            
            t_start_bool = df['entry'] == 'event_df_generation'
            t_end_bool = df['entry'] == 'extracted'
            
            assert df['time'][t_start_bool].size == 1
            assert df['time'][t_end_bool].size == 1
            
            t_start = df['time'][t_start_bool].iloc[0]
            t_end = df['time'][t_end_bool].iloc[0]
            
            delta_t = t_end - t_start
            
            df_dict[dir_path.stem].append(delta_t)
            
    df_final = pd.DataFrame(df_dict)
    print(df_final)
    
    fig = plt.figure(figsize=(7, 6))
    x = df_final['feature']
    y = df_final[folder_list].values.mean(axis=1)
    
    yerr = df_final[folder_list].values.std(axis=1)
    
    if not MAX_X is None:
        mask = x <= MAX_X
        x = x[mask]
        y = y[mask]
        yerr = yerr[mask]
    
    plt.errorbar(x, y, yerr=yerr, c='blue')
    plt.xlabel(XTITLE)
    plt.xticks(x, rotation=90)
    plt.ylabel(YTITLE)
    plt.grid(True)
    plt.tight_layout()
    fig.savefig(DEST_FOLDER)#, bbox_inches='tight', pad_inches=0)
    plt.close('all')