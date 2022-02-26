import os
import pathlib
import csv

def read_path_mapping_file(file_path:str=None, delimiter:str=',') -> dict:
    if file_path is None:
        base_path = os.getcwd()
        file_path = f'{base_path}/data/map_path_test.csv'
    map_path = dict()
    with open(file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=delimiter)
        header = reader.__next__()
        print("path mapping", header)
        for row in reader:
            map_path[row[0]] = row[1]
    print("path map keys:", map_path.keys())
    return map_path


if __name__ == "__main__":
    
    current_path = os.path.dirname(os.path.abspath(__file__))
    print(current_path)
    base_path = pathlib.Path(current_path).parent
    print(base_path)

    file_nm = f'{base_path}/data/map_path_test.csv'
    path_dict = read_path_mapping_file(file_nm)
    print(path_dict)

    print(read_path_mapping_file())