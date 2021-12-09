from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean

from benchmark.utils import Print


class ParseError(Exception):
    pass


class LogParser:
    def __init__(self, nodes, brokers, faults=0):
        inputs = [nodes, brokers]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.faults = faults
        self.num_replicas = len(nodes) + faults
        self.num_brokers = len(brokers)

        # Parse the brokers' logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_brokers, brokers)
        except (ValueError, IndexError) as e:
            raise ParseError(f'Failed to parse brokers logs: {e}')

        self.signup_batch_number, self.signup_batch_size, self.signup_batches_start, \
            self.signup_batches_end, self.signup_start, self.signup_end \
            = zip(*results)

        # Parse the nodes' logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_nodes, nodes)
        except (ValueError, IndexError) as e:
            raise ParseError(f'Failed to parse brokers logs: {e}')

        self.configs = results

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _parse_brokers(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Broker(s) panicked')

        signup_batch_number = int(search(r'Signup batch number: (\d+)', log).group(1))
        signup_batch_size = int(search(r'Signup batch size: (\d+)', log).group(1))

        tmp = findall(r'\[(.*Z) .* Signing up batch (\d+)', log)
        signup_batches_start = {int(s): self._to_posix(t) for t, s in tmp}

        tmp = findall(r'\[(.*Z) .* Completed signing up batch (\d+)', log)
        signup_batches_end = {int(s): self._to_posix(t) for t, s in tmp}

        tmp = search(r'\[(.*Z) .* Starting signup...', log).group(1)
        start = self._to_posix(tmp)

        tmp = search(r'\[(.*Z) .* Signup complete!', log).group(1)
        end = self._to_posix(tmp)

        return signup_batch_number, signup_batch_size, signup_batches_start, signup_batches_end, start, end

    def _parse_clients(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Client(s) panicked')

        size = int(search(r'Transactions size: (\d+)', log).group(1))
        rate = int(search(r'Transactions rate: (\d+)', log).group(1))

        tmp = search(r'\[(.*Z) .* Start ', log).group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'\[(.*Z) .* sample transaction (\d+)', log)
        samples = {int(s): self._to_posix(t) for t, s in tmp}

        return size, rate, start, misses, samples

    def _parse_nodes(self, log):
        if search(r'panic', log) is not None:
            raise ParseError('Node(s) panicked')

        configs = {
            'broker': {
                'signup_batch_number': int(
                    search(r'Broker signup batch number (\d+)', log).group(1)
                ),
                'signup_batch_size': int(
                    search(r'Broker signup batch size (\d+)', log).group(1)
                ),
            },
        }

        return configs

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def _signup_throughput(self):
        start, end = min(self.signup_start), max(self.signup_end)
        duration = end - start
        signups = self.signup_batch_number[0] * self.signup_batch_size[0]
        throughput = signups / duration
        return throughput, duration

    def _signup_latency(self):
        latency = [c - starts[d] \
            for (starts, ends) in zip(self.signup_batches_start, self.signup_batches_end) for d, c in ends.items()]

        return mean(latency) if latency else 0

    def _consensus_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _consensus_latency(self):
        latency = [c - self.proposals[d] for d, c in self.commits.items()]
        return mean(latency) if latency else 0

    def _end_to_end_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.start), max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _end_to_end_latency(self):
        latency = []
        for sent, received in zip(self.sent_samples, self.received_samples):
            for tx_id, batch_id in received.items():
                if batch_id in self.commits:
                    assert tx_id in sent  # We receive txs that we sent.
                    start = sent[tx_id]
                    end = self.commits[batch_id]
                    latency += [end-start]
        return mean(latency) if latency else 0

    def result(self):
        signup_throughput, duration = self._signup_throughput()
        signup_latency = self._signup_latency() * 1000
        # consensus_latency = self._consensus_latency() * 1000
        # consensus_tps, consensus_bps, _ = self._consensus_throughput()
        # end_to_end_tps, end_to_end_bps, duration = self._end_to_end_throughput()
        # end_to_end_latency = self._end_to_end_latency() * 1000
        signup_batch_number = self.configs[0]['broker']['signup_batch_number']
        signup_batch_size = self.configs[0]['broker']['signup_batch_size']

        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Number of replicas: {self.num_replicas}\n'
            f' Number of brokers: {self.num_brokers}\n'
            f' Faults: {self.faults} nodes\n'
            f' Execution time: {round(duration):,} s\n'
            '\n'
            f' Number of signup batches: {signup_batch_number} \n'
            f' Size of a signup batch: {signup_batch_size} \n'
            f' Total number of signups: {signup_batch_number * signup_batch_size} \n'
            '\n'
            ' + RESULTS:\n'
            f' Signup TPS: {round(signup_throughput):,} signups/s\n'
            f' Signup latency: {round(signup_latency):,} ms\n'
            '\n'
            '-----------------------------------------\n'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f:
            f.write(self.result())

    @classmethod
    def process(cls, directory, faults=0):
        assert isinstance(directory, str)

        brokers = []
        for filename in sorted(glob(join(directory, 'broker-*.log'))):
            with open(filename, 'r') as f:
                brokers += [f.read()]
        nodes = []
        for filename in sorted(glob(join(directory, 'replica-*.log'))):
            with open(filename, 'r') as f:
                nodes += [f.read()]

        return cls(nodes, brokers, faults=faults)
