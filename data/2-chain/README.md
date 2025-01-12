# 2-Chain HotStuff
This folder contains some benchmark data and plots for the code tagged as [v0.2.4](https://github.com/asonnino/hotstuff/tree/v0.2.4).

### Experimental step
The content of our [settings.json](https://github.com/asonnino/hotstuff/blob/main/benchmark/settings.json) file looks as follows:
```json
{
    "key": {
        "name": "aws",
        "path": "/absolute/key/path"
    },
    "ports": {
        "consensus": 8000,
        "mempool": 7000,
        "front": 6000
    },
    "repo": {
        "name": "hotstuff",
        "url": "https://github.com/asonnino/hotstuff.git",
        "branch": "main"
    },
    "instances": {
        "type": "m5d.8xlarge",
        "regions": ["us-east-1", "eu-north-1", "ap-southeast-2", "us-west-1", "ap-northeast-1"]
    }
}
```
When benchmarking the code base for 10 and 20 nodes, we set the following `node_params` in our [fabfile](https://github.com/asonnino/hotstuff/blob/main/benchmark/fabfile.py):
```python
node_params = {
    'consensus': {
        'timeout_delay': 5_000,       # ms
        'sync_retry_delay': 100_000,  # ms
        'max_payload_size': 1_000,    # bytes
        'min_block_delay': 100        # ms
    },
    'mempool': {
        'queue_capacity': 100_000,    # bytes
        'sync_retry_delay': 100_000,  # ms
        'max_payload_size': 500_000,  # bytes
        'min_block_delay': 100        # ms
    }
}
```
We increase the consensus timeout delay (`timeout_delay`) to 10 seconds when benchmarking the testbed with 50 nodes.

