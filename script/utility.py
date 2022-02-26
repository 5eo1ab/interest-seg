import os
import pathlib
import csv

def read_path_mapping_file(map_file_path:str, delimiter:str=',') -> dict:
    map_path = dict()
    with open(map_file_path, encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=delimiter)
        header = reader.__next__()
        map_path['key'] = header[1:]
        for row in reader:
            map_path[row[0]] = row[1:]
    return map_path


if __name__ == "__main__":
    
    current_path = os.path.dirname(os.path.abspath(__file__))
    base_path = pathlib.Path(current_path).parent
    print(base_path)

    file_nm = f'{base_path}/data/map_path_test.csv'
    path_dict = read_path_mapping_file(file_nm)
    print(path_dict)