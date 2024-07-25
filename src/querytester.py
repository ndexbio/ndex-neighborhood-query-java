#!/usr/bin/env python

import sys
import random
import json
import ndex2
from ndex2.cx2 import RawCX2NetworkFactory, CX2NetworkXFactory
from ndex2.exceptions import NDExError

server = sys.argv[1]

client = ndex2.client.Ndex2(host=server, skip_version_check=True)

# Create CX2Network factory
factory = RawCX2NetworkFactory()

res = client.search_networks(search_string='*', size=100000)
for key in res.keys():
    if key != 'networks':
        print(str(key) + ' => ' + str(res[key]))

for net_dict in res['networks']:
    if net_dict['nodeCount'] <= 0 or net_dict['edgeCount'] <= 0:
        print('\tNetwork: ' + str(net_dict['name']) + ' lacks edges or nodes. Skipping')
        continue
    if net_dict['nodeCount'] > 100000 or net_dict['edgeCount'] > 100000:
        print('\tNetwork: ' + str(net_dict['name']) + ' is too big. Skipping')
        continue
    if net_dict['subnetworkIds'] is not None and len(net_dict['subnetworkIds']) > 0:
        print('\tNetwork: ' + str(net_dict['name']) + ' is a collection. Skipping')
        continue
    print('Loading ' + str(net_dict['name']) + ' with id: ' + str(net_dict['externalId']))
    try:
        # get some nodes and then do a query
        client_resp = client.get_network_as_cx2_stream(net_dict['externalId'])
        net_cx = factory.get_cx2network(json.loads(client_resp.content))
    except NDExError as ne:
        print(json.dumps(net_dict, indent=2))
        print('\t\tCaught error: ' + str(ne))
        sys.stdout.flush()
        sys.stderr.flush()
        continue

    node_name_set = set()
    for node_id, node_obj in net_cx.get_nodes().items():
        if 'name' in node_obj['v']:
            node_name_set.add(node_obj['v']['name'])
        # print(node_frag)
    # print(node_name_set)
    node_name_list = list(node_name_set)
    rand_node_str = random.choice(node_name_list)

    # lets do a query with this str
    # TODO check the results
    client.get_neighborhood(net_dict['externalId'], rand_node_str, search_depth=1)
    client.get_neighborhood(net_dict['externalId'], rand_node_str, search_depth=2)

    # grab 5 nodes and do the search again
    query = ''
    for x in range(5):
        query += random.choice(node_name_list)
    # TODO check the results
    client.get_neighborhood(net_dict['externalId'], query, search_depth=1)






