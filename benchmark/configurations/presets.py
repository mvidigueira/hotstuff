regions = ["us-east-1", "us-east-2", "us-west-1", "us-west-2", 
"af-south-1", "ap-east-1", "ap-south-1", "ap-northeast-1", 
"ap-northeast-2", "ap-northeast-3", "ap-southeast-1", "ap-southeast-2", 
"ca-central-1", "eu-central-1", "eu-west-1", "eu-west-2", 
"eu-west-3", "eu-south-1", "eu-north-1", "me-south-1", "sa-east-1"]

# Main setup

# 'nodes': [{ "us-east-1": 3, "us-east-2": 3, "us-west-1": 3, "us-west-2": 3, \
#     "af-south-1": 4, "ap-east-1": 4, "ap-south-1": 4, "ap-northeast-1": 2,\
#         "ap-northeast-2": 3, "ap-southeast-1": 3, "ap-southeast-2": 3,\
#             "ca-central-1": 4, "eu-central-1": 3, "eu-west-1": 3, "eu-west-2": 4, \
#                 "eu-west-3": 2, "eu-south-1": 3, "eu-north-1": 3, "me-south-1": 4, \
#                     "sa-east-1": 3 }],
# 'fast_brokers': [{ "us-east-1": 3, "us-east-2": 3, "us-west-1": 3, "us-west-2": 3, \
#     "af-south-1": 4, "ap-east-1": 4, "ap-south-1": 4, "ap-northeast-1": 2,\
#         "ap-northeast-2": 3, "ap-southeast-1": 3, "ap-southeast-2": 3,\
#             "ca-central-1": 4, "eu-west-1": 3, "eu-west-2": 4, \
#                 "eu-west-3": 2, "eu-south-1": 3, "eu-north-1": 5, "me-south-1": 4, \
#                     "sa-east-1": 3 }],

# 32 machines

# 'nodes': [{ "us-east-1": 2, "us-east-2": 2, "us-west-1": 2, "us-west-2": 2, \
#     "af-south-1": 2, "ap-east-1": 2, "ap-south-1": 2, "ap-northeast-1": 1,\
#         "ap-northeast-2": 1, "ap-southeast-1": 1, "ap-southeast-2": 1,\
#             "ca-central-1": 2, "eu-central-1": 1, "eu-west-1": 1, "eu-west-2": 2, \
#                 "eu-west-3": 1, "eu-south-1": 1, "eu-north-1": 2, "me-south-1": 2, \
#                     "sa-east-1": 2 }],
# 'fast_brokers': [{ "us-east-1": 2, "us-east-2": 2, "us-west-1": 2, "us-west-2": 2, \
#     "af-south-1": 2, "ap-east-1": 2, "ap-south-1": 2, "ap-northeast-1": 1,\
#         "ap-northeast-2": 1, "ap-southeast-1": 1, "ap-southeast-2": 1,\
#             "ca-central-1": 1, "eu-central-1": 1, "eu-west-1": 1, "eu-west-2": 2, \
#                 "eu-west-3": 1, "eu-south-1": 1, "eu-north-1": 2, "me-south-1": 2, \
#                     "sa-east-1": 2 }],

# 16 machines

# 'nodes': [{ "us-east-1": 1, "us-east-2": 1, "us-west-1": 1, "us-west-2": 1, \
#     "ap-east-1": 1, "ap-south-1": 1, \
#         "ap-northeast-2": 1, "ap-southeast-1": 1, \
#             "ca-central-1": 1, "eu-central-1": 1, "eu-west-1": 1, "eu-west-2": 1, \
#                 "eu-west-3": 1, "eu-south-1": 1, "eu-north-1": 1, "me-south-1": 1, }],
# 'fast_brokers': [{ "us-east-1": 1, "us-east-2": 1, "us-west-1": 1, "us-west-2": 1, \
#     "ap-east-1": 1, "ap-south-1": 1, \
#         "ap-northeast-2": 1, "ap-southeast-1": 1, \
#             "ca-central-1": 1, "eu-west-1": 1, "eu-west-2": 1, \
#                 "eu-west-3": 1, "eu-south-1": 1, "eu-north-1": 1, "me-south-1": 1, }],

config_0 = { "us-east-1": 1, "us-east-2": 1, "us-west-1": 1, "us-west-2": 1, \
    "ap-east-1": 1, "ap-south-1": 1, \
        "ap-northeast-2": 1, "ap-southeast-1": 1, \
            "ca-central-1": 1, "eu-central-1": 1, "eu-west-1": 1, "eu-west-2": 1, \
                "eu-west-3": 1, "eu-south-1": 1, "eu-north-1": 1, "me-south-1": 1, }

remote_bench_parameters = {
    'nodes': config_0,
    'fast_brokers': config_0,
    'full_brokers': [],
    'full_clients': [],
    'rate': 1_000_000,
    'duration': 120,
    'runs': 1,
}

creation_nodes = config_0

remote_node_parameters = {
    'broker': {
        'signup_batch_number': 10,
        'signup_batch_size': 5000,
        'prepare_batch_size': 50000,
        'prepare_batch_number': 5,
        'prepare_single_sign_percentage': 100,
        'brokerage_timeout': 1000, # millis
        'reduction_timeout': 1000, # millis
    },
}