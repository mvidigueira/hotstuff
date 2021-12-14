from fabric import task

from benchmark.logs import ParseError, LogParser
from benchmark.utils import Print
from benchmark.plot import Ploter, PlotError
from aws.instance import InstanceManager
from aws.remote import Bench, BenchError

# ["us-east-2", "us-west-2", "sa-east-1", "eu-north-1", "me-south-1", "af-south-1", "ap-south-1", "ap-southeast-1", "ap-southeast-2"]
# ["us-east-1", "us-west-1", "ap-east-1", "ap-northeast-1", "ap-northeast-2", "ap-northeast-3", "ca-central-1", "eu-central-1", "eu-west-1", "eu-west-2", "eu-west-3", "eu-south-1"]

# 'nodes': [{ "us-east-1": 3, "us-east-2": 3, "us-west-1": 3, "us-west-2": 3, \
#     "af-south-1": 4, "ap-east-1": 4, "ap-south-1": 4, "ap-northeast-1": 2,\
#          "ap-northeast-2": 3, "ap-southeast-1": 3, "ap-southeast-2": 3,\
#               "ca-central-1": 4, "eu-central-1": 3, "eu-west-1": 3, "eu-west-2": 4, \
#                   "eu-west-3": 2, "eu-south-1": 3, "eu-north-1": 3, "me-south-1": 4, \
#                       "sa-east-1": 3 }],

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

creation_nodes = { "eu-north-1": 2 }

remote_bench_params = {
    'nodes': [{ "us-east-1": 1, "us-east-2": 1, "us-west-1": 1, "us-west-2": 1, \
    "ap-east-1": 1, "ap-south-1": 1, \
        "ap-northeast-2": 1, "ap-southeast-1": 1, \
            "ca-central-1": 1, "eu-central-1": 1, "eu-west-1": 1, "eu-west-2": 1, \
                "eu-west-3": 1, "eu-south-1": 1, "eu-north-1": 1, "me-south-1": 1, }],
    'fast_brokers': [{ "us-east-1": 1, "us-east-2": 1, "us-west-1": 1, "us-west-2": 1, \
    "ap-east-1": 1, "ap-south-1": 1, \
        "ap-northeast-2": 1, "ap-southeast-1": 1, \
            "ca-central-1": 1, "eu-central-1": 1, "eu-west-1": 1, "eu-west-2": 1, \
                "eu-west-3": 1, "eu-south-1": 1, "eu-north-1": 1, "me-south-1": 1, }],
    'full_brokers': [],
    'full_clients': [],
    'rate': 1_000_000,
    'faults': 0,
    'duration': 120,
    'runs': 1,
}

remote_node_params = {
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

@task
def create(ctx):
    ''' Create a testbed'''

    try:
        InstanceManager.make().create_instances(creation_nodes)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    ''' Destroy the testbed '''
    try:
        InstanceManager.make().terminate_instances()
    except BenchError as e:
        Print.error(e)


@task
def start(ctx, max = 8):
    ''' Start at most `max` machines per data center '''
    try:
        InstanceManager.make().start_instances(max)
    except BenchError as e:
        Print.error(e)


@task
def stop(ctx):
    ''' Stop all machines '''
    try:
        InstanceManager.make().stop_instances()
    except BenchError as e:
        Print.error(e)


@task
def info(ctx):
    ''' Display connect information about all the available machines '''
    try:
        InstanceManager.make().print_info()
    except BenchError as e:
        Print.error(e)


@task
def install(ctx):
    ''' Install the codebase on all machines '''
    try:
        Bench(ctx).install()
    except BenchError as e:
        Print.error(e)


@task
def remote(ctx):
    ''' Run benchmarks on AWS '''
    try:
        Bench(ctx).run(remote_bench_params, remote_node_params, debug=False)
    except BenchError as e:
        Print.error(e)


@task
def plot(ctx):
    ''' Plot performance using the logs generated by "fab remote" '''
    plot_params = {
        'nodes': [36],
        'brokers': [36],
        'rate': [36000000],
        'faults': [0],
    }
    try:
        Ploter.plot(plot_params)
    except PlotError as e:
        Print.error(BenchError('Failed to plot performance', e))


@task
def kill(ctx):
    ''' Stop any HotStuff execution on all machines '''
    try:
        Bench(ctx).kill()
    except BenchError as e:
        Print.error(e)


@task
def logs(ctx):
    ''' Print a summary of the logs '''
    try:
        Bench(ctx).dl_logs(remote_bench_params)
        print(LogParser.process('./logs').result())
    except ParseError as e:
        Print.error(BenchError('Failed to parse logs', e))

