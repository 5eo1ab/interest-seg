import csv
import pandas as pd
import numpy as np

NODE_COLUMN_PRESET = ['id', 'label']
EDGE_COLUMN_SET = ['source', 'target', 'weight']

def convert_site_node_list(in_path, out_path):
	out_data = pd.read_csv(in_path)
	out_columns = NODE_COLUMN_PRESET + list(out_data.columns)[2:]
	out_data.columns = out_columns
	out_data.to_csv(out_path, header=True, index=False, encoding='utf-8')


def convert_target2site_edge_list(
		in_path, 
		out_path, 
		index='imsi_no', 
		columns='site_id',
		values='use_amount',
	):
	in_data = pd.read_csv(in_path)
	in_mat = in_data.pivot(index=index, columns=columns, values=values).fillna(0)

	matmul = np.matmul(in_mat.T.values, in_mat.values)
	diag_sqrt = np.sqrt(np.diag(matmul))
	diag_sqrt_rcp = np.array([np.reciprocal(diag_sqrt)]) # 2d-array
	cosine_base = np.matmul(diag_sqrt_rcp.T, diag_sqrt_rcp)
	cosine_mat = np.triu(np.multiply(matmul, cosine_base), k=1)

	out_mat = pd.DataFrame(data=cosine_mat, columns=in_mat.columns)
	out_mat['site_id'] = in_mat.columns
	out_data = pd.melt(out_mat, id_vars=columns, var_name='dummy_var', value_name='weight')
	out_data = out_data[out_data['weight']>0]
	out_data.columns = EDGE_COLUMN_SET 
	out_data.to_csv(out_path, header=True, index=False, encoding='utf-8')

# def main():
# 	map_path = dict()
# 	with open()


if __name__ == '__main__':
	# main()
	
	base_path = 'D:/hanbin5eo/work/interest-seg/data/20220225'
	file_nm = 'L2CK_HB5EO_MASTER_SITE_TEMP_202202251318.csv'
	in_path = '{}/{}'.format(base_path, file_nm)
	out_path = '{}/node_master.csv'.format(base_path)
	# convert_site_node_list(in_path, out_path)

	file_nms = {
		'F0': 'F0_SELECT_C_FROM_SBXL2CK_L2CK_HB5EO_MASTER_TARGET2SITE_CNC_TEMP_AS_202202251310.csv',
		'F1': 'F1_SELECT_C_FROM_SBXL2CK_L2CK_HB5EO_MASTER_TARGET2SITE_CNC_TEMP_AS_202202251306.csv',
		'M0': 'M0_SELECT_C_FROM_SBXL2CK_L2CK_HB5EO_MASTER_TARGET2SITE_CNC_TEMP_AS_202202251313.csv',
		'M1': 'M1_SELECT_C_FROM_SBXL2CK_L2CK_HB5EO_MASTER_TARGET2SITE_CNC_TEMP_AS_202202251316.csv',
	}
	for k_idx, file_nm in file_nms.items():
		print(k_idx)
		in_path = '{}/{}'.format(base_path, file_nm)
		out_path = '{}/edge_master_{}.csv'.format(base_path, k_idx)
		convert_target2site_edge_list(in_path, out_path)