import pandas as pd

print('=> RUNNING DATA ANALYSIS...')

formatted_dataset = pd.read_csv(
    'conn.log',
    sep="\t",
    header=None,
    names=[
        'ts',
        'uid',
        'id.orig_h',
        'id.orig_p',
        'id.resp_h',
        'id.resp_p',
        'proto',
        'service',
        'duration',
        'orig_bytes',
        'resp_bytes',
        'conn_state',
        'local_orig',
        'missed_bytes',
        'history',
        'orig_pkts',
        'orig_ip_bytes',
        'resp_pkts',
        'resp_ip_bytes',
        'tunnel_parents',
    ])

print('=> GET SAMPLE...')

# Generate a 10% random sample to analyze.
sample = formatted_dataset.sample(frac=0.1)

print('=> DISPLAY SAMPLE...')

print(sample)
