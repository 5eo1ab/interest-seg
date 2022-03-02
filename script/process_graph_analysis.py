import sys, os

import networkx as nx
from networkx import community as nxcomm
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib import cm, colors

sys.path.append(os.getcwd())
from script.utility import read_path_mapping_file

NODE_INPUT_KEYS = ['Site_MY_in', 'Site_MN_in', 'Site_FY_in', 'Site_FN_in']
EDGE_INPUT_KEYS = ['T2S_MY_in', 'T2S_MN_in', 'T2S_FY_in', 'T2S_FN_in']
NODE_OUTPUT_KEYS = ['Site_MY_out', 'Site_MN_out', 'Site_FY_out', 'Site_FN_out']
EDGE_OUTPUT_KEYS = ['T2S_MY_out', 'T2S_MN_out', 'T2S_FY_out', 'T2S_FN_out']

EDGE_WEIGHT = 0.2


def build_graph(node_data:pd.DataFrame, edge_data:pd.DataFrame):
    G = nx.Graph()
    node_formatted_data = list()
    for row in node_data.values:
        instance = (row[0], {k:row[i] for i,k in enumerate(node_data.columns[1:])})
        node_formatted_data.append(instance)
    G.add_nodes_from(node_formatted_data)
    edge_formatted_data = list()
    for s, d, w in edge_data.values:
        if w < EDGE_WEIGHT:
            continue
        edge_formatted_data.append((s, d, w))
    G.add_weighted_edges_from(edge_formatted_data)
    print("Count of Isolations: ", len(list(nx.isolates(G))))
    G.remove_nodes_from(list(nx.isolates(G)))
    print("Count of Nodes: ", len(G.nodes), len(G.nodes)/len(node_data))
    print("Count of Edges: ", len(G.edges), len(G.edges)/len(edge_data))
    print("Count of Components: ", len(list(nx.connected_components(G))))
    return G


def append_modularity_community(node_data, graph):
    comm = nxcomm.greedy_modularity_communities(graph, weight='weight')
    print("Count of Modularity: ", len(comm))
    map_comm_label = dict()
    for idx, node_set in enumerate(comm):
        for n in node_set:
            map_comm_label[n] = idx
    community_labels = list()
    for n_id in node_data['id']:
        if n_id not in map_comm_label.keys():
            community_labels.append(None)
            continue
        community_labels.append(map_comm_label[n_id])
    node_data = node_data.assign(modularity=community_labels)
    node_data = node_data[node_data['modularity'].isnull()==False]
    return node_data.astype({'modularity': 'int32'})


def append_node_centrality(node_data, graph):
    node_degree_centrality = nx.degree_centrality(graph)
    node_data = node_data.assign(degree=[node_degree_centrality[n_id] for n_id in node_data['id']])
    node_betweenness_centrality = nx.betweenness_centrality(graph)
    node_data = node_data.assign(betweenness=[node_betweenness_centrality[n_id] for n_id in node_data['id']])
    node_closeness_centrality = nx.closeness_centrality(graph)
    node_data = node_data.assign(closeness=[node_closeness_centrality[n_id] for n_id in node_data['id']])
    return node_data


def main():
    path_map = read_path_mapping_file()
    key_set = zip(NODE_INPUT_KEYS, EDGE_INPUT_KEYS, NODE_OUTPUT_KEYS, EDGE_OUTPUT_KEYS)
    for in_n_key, in_e_key, out_n_key, out_e_key in key_set:
        in_nodes = pd.read_csv(path_map[in_n_key])
        in_edges = pd.read_csv(path_map[in_e_key])
        graph = build_graph(node_data=in_nodes, edge_data=in_edges)

        node_data = append_modularity_community(node_data=in_nodes, graph=graph)
        node_data = append_node_centrality(node_data=node_data, graph=graph)
        node_data.to_csv(path_map[out_n_key], index=False, header=True)      
        print(">>", path_map[out_n_key])

        filtered_edges = list()
        for s, d, attr in graph.edges(data=True):
            instance = (s, d, attr['weight'])
            filtered_edges.append(instance)
        edge_data = pd.DataFrame(filtered_edges, columns=in_edges.columns)
        edge_data.to_csv(path_map[out_e_key], index=False, header=True)
        print(">>", path_map[out_e_key])


if __name__ == "__main__":
    main()