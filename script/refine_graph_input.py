import sys, os
import pandas as pd
import numpy as np
sys.path.append(os.getcwd())
from script.utility import read_path_mapping_file

NODE_INPUT_KEY = 'Site'
NODE_OUTPUT_KEY = 'Site_in'
NODE_COLUMN_MAPPER = {
    'id': 'site_id',
    'label': 'site_nm',
}
EDGE_INPUT_KEYS = ['T2S_MY', 'T2S_MN', 'T2S_FY', 'T2S_FN']
EDGE_OUTPUT_KEYS = ['T2S_MY_in', 'T2S_MN_in', 'T2S_FY_in', 'T2S_FN_in']


def replace_node_header(data:pd.DataFrame, column_mapper:dict) -> pd.DataFrame:
    out_columns = list(data.columns)
    for k,v in NODE_COLUMN_MAPPER.items():
        idx = out_columns.index(v)
        out_columns[idx] = k
    data.columns = out_columns
    return data


def convert_cosine_similarity_edge_list(data:pd.DataFrame) -> pd.DataFrame:
    src_origin, dst_origin, value_origin = data.columns
    pivoted_data = data.pivot(
        index=src_origin,       # imsi_no
        columns=dst_origin,     # site_id
        values=value_origin,    # use_amount
    ).fillna(0)

    matmul = np.matmul(pivoted_data.T.values, pivoted_data.values)
    diag_sqrt = np.sqrt(np.diag(matmul))
    diag_sqrt_rcp = np.array([np.reciprocal(diag_sqrt)]) # 2d-array
    cosine_base = np.matmul(diag_sqrt_rcp.T, diag_sqrt_rcp)
    cosine_mat = np.triu(np.multiply(matmul, cosine_base), k=1)

    out_mat = pd.DataFrame(data=cosine_mat, columns=pivoted_data.columns)
    out_mat[dst_origin] = pivoted_data.columns
    out_data = pd.melt(out_mat, id_vars=dst_origin, var_name='dummy_var', value_name='weight')
    out_data = out_data[out_data['weight']>0]
    out_data.columns = ['source', 'target', 'weight']
    return out_data


def main():
    path_map = read_path_mapping_file()
    
    node_data = pd.read_csv(path_map[NODE_INPUT_KEY])
    node_data = replace_node_header(
        data=node_data,
        column_mapper=NODE_COLUMN_MAPPER,
    )
    node_data.to_csv(path_map[NODE_OUTPUT_KEY], header=True, index=False)
    print('>>', path_map[NODE_OUTPUT_KEY])

    for in_key, out_key in zip(EDGE_INPUT_KEYS, EDGE_OUTPUT_KEYS):
        in_data = pd.read_csv(path_map[in_key])
        out_data = convert_cosine_similarity_edge_list(in_data)
        out_data.to_csv(path_map[out_key], header=True, index=False)
        print('>>', path_map[out_key])


if __name__ == "__main__":
    main()