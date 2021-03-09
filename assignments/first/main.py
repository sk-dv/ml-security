import pandas as pd
from datetime import datetime

print('=> RUNNING DATA ANALYSIS...')

# formatted_dataset = pd.read_csv(
#     'conn.log',
#     sep="\t",
#     header=None,
#     names=['ts', 'uid', 'id_orig_h', 'id_orig_p', 'id_resp_h', 'id_resp_p', 'proto', 'service', 'duration',
#            'orig_bytes', 'resp_bytes', 'conn_state', 'local_orig', 'missed_bytes', 'history', 'orig_pkts',
#            'orig_ip_bytes', 'resp_pkts', 'resp_ip_bytes', 'tunnel_parents', 'threat', 'sample']
# ) 

# sample = formatted_dataset.sample(frac=0.1)
# sample.to_parquet('conn_sample.parq', index=False) 

print('=> LOADING PARQUET...')
data_frame = pd.read_parquet('conn_sample.parq', columns=['ts', 'id_orig_h', 'id_resp_p', 'duration'])

data_frame['ts'] = [datetime.fromtimestamp(float(date)) for date in data_frame['ts']]
data_frame['duration'] = [0.0 if '-' in item_dur else float(item_dur) for item_dur in data_frame['duration']]
print(data_frame)

diff_to_80_8080 = data_frame[~data_frame['id_resp_p'].isin([80, 8080])]
group_by_ports = data_frame.groupby(['id_orig_h', 'id_resp_p']).size().to_frame()

print(group_by_ports.sort_values(by=0, ascending=False))
print(diff_to_80_8080)

print('=> FINISHED...')



