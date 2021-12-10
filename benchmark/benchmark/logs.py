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

        self.signup_batch_number, self.signup_batch_size, self.prepare_batch_number, \
            self.prepare_batch_size, self.prepare_single_sign_percentage, self.signup_start, self.signup_end, \
            self.signup_batches_start, self.signup_batches_end, self.prepare_start, self.prepare_end, \
            self.prepare_precompute_start, self.prepare_precompute_end, \
            self.prepare_batches_start, self.prepare_batches_end, \
            = zip(*results)

        # Parse the nodes' logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_nodes, nodes)
        except (ValueError, IndexError) as e:
            raise ParseError(f'Failed to parse brokers logs: {e}')

        self.id_requests_processing, self.id_claims_proccessing, \
            self.id_assignments_processing, self.configs = zip(*results)

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
        prepare_batch_number = int(search(r'Prepare batch number: (\d+)', log).group(1))
        prepare_batch_size = int(search(r'Prepare batch size: (\d+)', log).group(1))
        prepare_single_sign_percentage = int(search(r'Prepare single sign percentage: (\d+)', log).group(1))

        tmp = findall(r'\[(.*Z) .* Signing up batch (\d+)', log)
        signup_batches_start = {int(s): self._to_posix(t) for t, s in tmp}

        tmp = findall(r'\[(.*Z) .* Completed signing up batch (\d+)', log)
        signup_batches_end = {int(s): self._to_posix(t) for t, s in tmp}

        tmp = search(r'\[(.*Z) .* Starting signup...', log).group(1)
        signup_start = self._to_posix(tmp)

        tmp = search(r'\[(.*Z) .* Signup complete!', log).group(1)
        signup_end = self._to_posix(tmp)

        tmp = findall(r'\[(.*Z) .* Submitting prepare batch (\d+)', log)
        if tmp:
            prepare_batches_start = {int(s): self._to_posix(t) for t, s in tmp}
        else:
            prepare_batches_start = {}

        tmp = findall(r'\[(.*Z) .* Committed prepare batch (\d+)', log)
        if tmp:
            prepare_batches_end = {int(s): self._to_posix(t) for t, s in tmp}
        else:
            prepare_batches_end = {}

        tmp = search(r'\[(.*Z) .* Starting prepare...', log)
        prepare_start = self._to_posix(tmp.group(1)) if tmp else 0

        tmp = search(r'\[(.*Z) .* Prepare complete!', log)
        prepare_end = self._to_posix(tmp.group(1)) if tmp else 0

        tmp = search(r'\[(.*Z) .* Pre-computing submissions...', log)
        prepare_precompute_start = self._to_posix(tmp.group(1)) if tmp else 0

        tmp = search(r'\[(.*Z) .* Pre-computing submissions...', log)
        prepare_precompute_end = self._to_posix(tmp.group(1)) if tmp else 0

        return signup_batch_number, signup_batch_size, prepare_batch_number, \
            prepare_batch_size, prepare_single_sign_percentage, signup_start, signup_end, \
            signup_batches_start, signup_batches_end, prepare_start, prepare_end, \
            prepare_precompute_start, prepare_precompute_end, \
            prepare_batches_start, prepare_batches_end,

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

        if search(r'Processed id requests in (\d+)', log):
            tmp = findall(r'Processed id requests in (\d+)', log)
            id_requests_processing = [int(s) for s in tmp]
        else:
            id_requests_processing = []

        tmp = findall(r'Processed id claims in (\d+)', log)
        id_claims_proccessing = [int(s) for s in tmp]

        tmp = findall(r'Processed id assignments in (\d+)', log)
        id_assignments_processing = [int(s) for s in tmp] 


        configs = {
            'broker': {
                'signup_batch_number': int(
                    search(r'Broker signup batch number (\d+)', log).group(1)
                ),
                'signup_batch_size': int(
                    search(r'Broker signup batch size (\d+)', log).group(1)
                ),
                'prepare_batch_number': int(
                    search(r'Broker prepare batch number (\d+)', log).group(1)
                ),
                'prepare_batch_size': int(
                    search(r'Broker prepare batch size (\d+)', log).group(1)
                ),
                'prepare_single_sign_percentage': int(
                    search(r'Broker prepare single sign percentage (\d+)', log).group(1)
                ),
            },
        }

        return id_requests_processing, id_claims_proccessing, id_assignments_processing, configs

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def _signup_throughput(self):
        start, end = min(self.signup_start), max(self.signup_end)
        duration = end - start
        signups = self.signup_batch_number[0] * self.signup_batch_size[0] * self.num_brokers
        throughput = signups / duration
        return throughput, duration

    def _signup_latency(self):
        latency = [c - starts[d] \
            for (starts, ends) in zip(self.signup_batches_start, self.signup_batches_end) for d, c in ends.items()]

        return mean(latency) if latency else 0

    def _prepare_throughput(self):
        start, end = min(self.prepare_start), max(self.prepare_end)
        duration = end - start
        prepares = self.prepare_batch_number[0] * self.prepare_batch_size[0] * self.num_brokers
        throughput = prepares / duration if duration != 0 else 0
        return throughput, duration

    def _prepare_latency(self):
        latency = [c - starts[d] \
            for (starts, ends) in zip(self.prepare_batches_start, self.prepare_batches_end) for d, c in ends.items()]

        return mean(latency) if latency else 0

    def _signup_p_latency(self):
        req = [i for l in self.id_requests_processing for i in l]
        claim = [i for l in self.id_claims_proccessing for i in l]
        assignments = [i for l in self.id_assignments_processing for i in l]
        return mean(req) if req else 0, mean(claim) if claim else 0, mean(assignments) if assignments else 0

    def _prepare_pre_p_latency(self):
        latency = [end - start \
            for (start, end) in zip(self.prepare_precompute_start, self.prepare_precompute_end)]

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
        signup_throughput, signup_duration = self._signup_throughput()
        signup_latency = self._signup_latency() * 1000
        id_requests_p_latency, id_claims_p_latency, id_assignments_p_latency = self._signup_p_latency()

        prepare_throughput, prepare_duration = self._prepare_throughput()
        prepare_latency = self._prepare_latency() * 1000
        prepare_pre_compute_latency = self._prepare_pre_p_latency()

        signup_batch_number = self.configs[0]['broker']['signup_batch_number']
        signup_batch_size = self.configs[0]['broker']['signup_batch_size']
        prepare_batch_number = self.configs[0]['broker']['prepare_batch_number']
        prepare_batch_size = self.configs[0]['broker']['prepare_batch_size']
        prepare_single_sign_percentage = self.configs[0]['broker']['prepare_single_sign_percentage']

        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Number of replicas: {self.num_replicas}\n'
            f' Number of brokers: {self.num_brokers}\n'
            f' Faults: {self.faults} nodes\n'
            f' Signup execution time: {round(signup_duration):,} s\n'
            f' Prepare execution time: {round(prepare_duration):,} s\n'
            '\n'
            f' Number of signup batches: {signup_batch_number} \n'
            f' Size of a signup batch: {signup_batch_size} \n'
            f' Total number of signups: {signup_batch_number * signup_batch_size * self.num_brokers} \n'
            f' Number of prepare batches: {prepare_batch_number} \n'
            f' Size of a prepare batch: {prepare_batch_size} \n'
            f' Precentage of single signatures: {prepare_single_sign_percentage} %\n'
            f' Total number of TX: {prepare_batch_number * prepare_batch_size * self.num_brokers} \n'
            '\n'
            ' + RESULTS:\n'
            f' Prepare TPS: {round(prepare_throughput):,} Prepare/s\n'
            f' Prepare latency: {round(prepare_latency):,} ms\n'
            f' Prepare pre-compute latency: {round(prepare_pre_compute_latency):,} ms\n'
            '\n'
            f' Signup TPS: {round(signup_throughput):,} Signup/s\n'
            f' Signup latency: {round(signup_latency):,} ms\n'
            f' Id request processing latency: {round(id_requests_p_latency):,} ms\n'
            f' Id claim processing latency: {round(id_claims_p_latency):,} ms\n'
            f' Id assignments processing latency: {round(id_assignments_p_latency):,} ms\n'
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
