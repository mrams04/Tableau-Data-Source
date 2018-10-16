#usage (For Extracts) - python tab_json.py abc JSON ip.json ip rmutupuru
#usage (For Live connection) - python tab_json.py abc SQL "select col_a, col_b from table_a"  abc rmutupuru

import json
import sys
import tab_ds

v_ds_name = sys.argv[1]
v_input_type = sys.argv[2]
v_input_data = sys.argv[3]
v_file_name = sys.argv[4]
v_user = sys.argv[5]

print v_input_data

if v_input_type == 'JSON':
    fp = open(v_input_data)
    contents = fp.read()
    #print contents
    json_data = json.loads(contents)

    file_name = v_file_name+'.tde'
    a = tab_ds.tab_ds(file_name, json_data, 'JSON', v_ds_name, v_user)
else:
    a = tab_ds.tab_ds(v_file_name+'.tds', v_input_data.encode('utf8'), 'SQL', v_ds_name, v_user)
