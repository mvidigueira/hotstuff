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
    def __init__(self, nodes, fast, full, clients, faults=0):
        inputs = [nodes, fast, full, clients]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert nodes

        self.faults = faults
        self.num_replicas = len(nodes) + faults
        self.num_fast_brokers = len(fast)
        self.num_full_brokers = len(full)
        self.num_clients = len(clients)

        # Parse the brokers' logs.
        if fast:
            try:
                with Pool() as p:
                    results = p.map(self._parse_fast_brokers, fast)
            except (ValueError, IndexError) as e:
                raise ParseError(f'Failed to parse fast brokers logs: {e}')

            self.rate, self.signup_batch_number, self.signup_batch_size, self.prepare_batch_number, \
                self.prepare_batch_size, self.prepare_single_sign_percentage, self.signup_start, self.signup_end, \
                self.signup_batches_start, self.signup_batches_end, self.prepare_start, self.prepare_end, \
                self.prepare_precompute_start, self.prepare_precompute_end, \
                self.prepare_batches_start, self.prepare_batches_end, \
                = zip(*results)

        if full:
            try:
                with Pool() as p:
                    results = p.map(self._parse_full_brokers, full)
            except (ValueError, IndexError) as e:
                raise ParseError(f'Failed to parse fast brokers logs: {e}')

            self.prepare_batch_size, self.brokerage_timeout, self.reduction_percentage, \
                self.reduction_timeout, self.batch_brokerages, self.batch_reductions \
                = zip(*results)

        if clients:
            try:
                with Pool() as p:
                    results = p.map(self._parse_clients, clients)
            except (ValueError, IndexError) as e:
                raise ParseError(f'Failed to parse fast brokers logs: {e}')

            self.client_prepare_start, self.client_prepare_end \
                = zip(*results)

        # Parse the nodes' logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_nodes, nodes)
        except (ValueError, IndexError) as e:
            raise ParseError(f'Failed to parse nodes logs: {e}')

        self.id_requests_processing, self.id_claims_processing, self.id_assignments_processing, \
            self.prepare_batch_processing, self.prepare_commits_processing, self.prepare_batch_validation, \
            self.prepare_batch_apply, self.prepare_witness_validate, self.configs = zip(*results)

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _parse_fast_brokers(self, log):
        # if search(r'Error', log) is not None:
        #     raise ParseError('Fast broker(s) panicked')

        rate = int(search(r'Rate limit: (\d+)', log).group(1))
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
        prepare_batches_start = {int(s): self._to_posix(t) for t, s in tmp} if tmp else {}

        tmp = findall(r'\[(.*Z) .* Completed prepare batch (\d+)', log)
        prepare_batches_end = {int(s): self._to_posix(t) for t, s in tmp} if tmp else {}

        tmp = search(r'\[(.*Z) .* Starting prepare...', log)
        prepare_start = self._to_posix(tmp.group(1)) if tmp else 0

        tmp = search(r'\[(.*Z) .* All transactions completed!', log)
        prepare_end = self._to_posix(tmp.group(1)) if tmp else 0

        tmp = search(r'\[(.*Z) .* Pre-computing submissions...', log)
        prepare_precompute_start = self._to_posix(tmp.group(1)) if tmp else 0

        tmp = search(r'\[(.*Z) .* All submissions pre-computed!', log)
        prepare_precompute_end = self._to_posix(tmp.group(1)) if tmp else 0

        return rate, signup_batch_number, signup_batch_size, prepare_batch_number, \
            prepare_batch_size, prepare_single_sign_percentage, signup_start, signup_end, \
            signup_batches_start, signup_batches_end, prepare_start, prepare_end, \
            prepare_precompute_start, prepare_precompute_end, \
            prepare_batches_start, prepare_batches_end,

    def _parse_full_brokers(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Fast broker(s) panicked')

        prepare_batch_size = int(search(r'Prepare batch size: (\d+)', log).group(1))
        brokerage_timeout = int(search(r'Brokerage timeout: (\d+)', log).group(1))
        reduction_percentage = int(search(r'Reduction percentage: (\d+)', log).group(1))
        reduction_timeout = int(search(r'Reduction timeout: (\d+)', log).group(1))

        tmp = findall(r'Number of brokerages: (\d+)', log)
        batch_brokerages = [int(s) for s in tmp]

        tmp = findall(r'Number of reductions: (\d+)', log)
        batch_reductions = [int(s) for s in tmp]

        return prepare_batch_size, brokerage_timeout, reduction_percentage, reduction_timeout, batch_brokerages, batch_reductions

    def _parse_clients(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Client(s) panicked')

        tmp = findall(r'\[(.*Z) .* Client sending prepare for height (\d+)', log)
        client_prepare_start = {int(s): self._to_posix(t) for t, s in tmp} if tmp else {}

        tmp = findall(r'\[(.*Z) .* Client finished prepare for height (\d+)', log)
        client_prepare_end = {int(s): self._to_posix(t) for t, s in tmp} if tmp else {}

        return client_prepare_start, client_prepare_end

    def _parse_nodes(self, log):
        if search(r'panic', log) is not None:
            raise ParseError('Node(s) panicked')

        if search(r'Processed id requests in (\d+)', log):
            tmp = findall(r'Processed id requests in (\d+)', log)
            id_requests_processing = [int(s) for s in tmp]
        else:
            id_requests_processing = []

        tmp = findall(r'Processed id claims in (\d+)', log)
        id_claims_processing = [int(s) for s in tmp]

        tmp = findall(r'Processed id assignments in (\d+)', log)
        id_assignments_processing = [int(s) for s in tmp] 

        tmp = findall(r'Processed prepare batch in (\d+)', log)
        prepare_batch_processing = [int(s) for s in tmp] 

        tmp = findall(r'Processed prepare commits in (\d+)', log)
        prepare_commits_processing = [int(s) for s in tmp] 

        tmp = findall(r'Validated batch in (\d+)', log)
        prepare_batch_validation = [int(s) for s in tmp]

        tmp = findall(r'Applied batch in (\d+)', log)
        prepare_batch_apply = [int(s) for s in tmp] 

        tmp = findall(r'Validated witness in (\d+)', log)
        prepare_witness_validate = [int(s) for s in tmp] 


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

        return id_requests_processing, id_claims_processing, id_assignments_processing, \
            prepare_batch_processing, prepare_commits_processing, prepare_batch_validation, \
            prepare_batch_apply, prepare_witness_validate, configs

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def _signup_throughput(self):
        if self.num_fast_brokers == 0:
            return 0, 0

        start, end = min(self.signup_start), max(self.signup_end)
        duration = end - start
        signups = self.signup_batch_number[0] * self.signup_batch_size[0] * self.num_fast_brokers
        throughput = signups / duration
        return throughput, duration

    def _signup_latency(self):
        if self.num_fast_brokers == 0:
            return 0

        latency = [c - starts[d] \
            for (starts, ends) in zip(self.signup_batches_start, self.signup_batches_end) for d, c in ends.items()]

        return mean(latency) if latency else 0

    def _signup_p_latency(self):
        if self.num_fast_brokers == 0:
            return 0, 0, 0

        req = [i for l in self.id_requests_processing for i in l]
        claim = [i for l in self.id_claims_processing for i in l]
        assignments = [i for l in self.id_assignments_processing for i in l]
        return mean(req) if req else 0, mean(claim) if claim else 0, mean(assignments) if assignments else 0

    def _prepare_throughput(self):
        if self.num_fast_brokers == 0:
            return 0, 0

        start, end = min(self.prepare_start), max(self.prepare_end)
        duration = end - start
        prepares = self.prepare_batch_number[0] * self.prepare_batch_size[0] * self.num_fast_brokers
        throughput = prepares / duration if duration != 0 else 0
        return throughput, duration

    def _prepare_latency(self):
        if self.num_clients == 0:
            return 0

        latency = [c - starts[d] \
            for (starts, ends) in zip(self.client_prepare_start, self.client_prepare_end) for d, c in ends.items()]

        return mean(latency) if latency else 0

    def _prepare_pre_p_latency(self):
        if self.num_fast_brokers == 0:
            return 0

        latency = [end - start \
            for (start, end) in zip(self.prepare_precompute_start, self.prepare_precompute_end)]

        return mean(latency) if latency else 0

    def _prepare_p_latency(self):
        if self.num_fast_brokers == 0:
            return 0, 0, 0, 0, 0

        batch = [i for l in self.prepare_batch_processing for i in l if l]
        commits = [i for l in self.prepare_commits_processing for i in l if l]
        batch_apply = [i for l in self.prepare_batch_apply for i in l if l]
        batch_validation = [i for l in self.prepare_batch_validation for i in l if l]
        batch_witness = [i for l in self.prepare_witness_validate for i in l if l]
        return mean(batch) if batch else 0,\
            mean(commits) if commits else 0,\
            mean(batch_apply) if batch_apply else 0,\
            mean(batch_validation) if batch_validation else 0,\
            mean(batch_witness) if batch_witness else 0

    def _sponge_results(self):
        if self.num_full_brokers == 0:
            return 0, 0

        batch_brokerages = [float(i) for l in self.batch_brokerages for i in l]
        batch_reductions = [float(i) for l in self.batch_reductions for i in l]

        mean_brokerage_batch_size = mean(batch_brokerages) * 100 / float(self.prepare_batch_size[0])
        mean_reduction_percentage =  mean([100 * reductions / batch for (reductions, batch) in zip(batch_reductions, batch_brokerages)])
        return mean_brokerage_batch_size, mean_reduction_percentage

    def result(self):
        signup_throughput, signup_duration = self._signup_throughput()
        signup_latency = self._signup_latency() * 1000
        id_requests_p_latency, id_claims_p_latency, id_assignments_p_latency = self._signup_p_latency()

        prepare_throughput, prepare_duration = self._prepare_throughput()
        prepare_latency = self._prepare_latency() * 1000
        prepare_pre_compute_latency = self._prepare_pre_p_latency() * 1000
        prepare_batch_p_latency, prepare_commits_p_latency,\
            batch_apply, batch_validate, batch_witness = self._prepare_p_latency()

        mean_brokerage_batch_size, mean_reduction_percentage = self._sponge_results()

        signup_batch_number = self.configs[0]['broker']['signup_batch_number']
        signup_batch_size = self.configs[0]['broker']['signup_batch_size']
        prepare_batch_number = self.configs[0]['broker']['prepare_batch_number']
        prepare_batch_size = self.configs[0]['broker']['prepare_batch_size']
        prepare_single_sign_percentage = self.configs[0]['broker']['prepare_single_sign_percentage']

        rate = "-" if self.num_fast_brokers == 0 else self.rate[0]
        brokerage_timeout = "-" if self.num_full_brokers == 0 else self.brokerage_timeout[0]
        reduction_timeout = "-" if self.num_full_brokers == 0 else self.reduction_timeout[0]

        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Number of replicas: {self.num_replicas}\n'
            f' Number of fast brokers: {self.num_fast_brokers}\n'
            f' Number of full brokers: {self.num_full_brokers}\n'
            f' Number of clients: {self.num_clients}\n'
            f' Input rate cap: {rate}\n'
            f' Faults: {self.faults} nodes\n'
            f' Signup execution time: {round(signup_duration):,} s\n'
            f' Prepare execution time: {round(prepare_duration):,} s\n'
            f' Single sign percentage: {round(prepare_latency):,} ms\n'
            '\n'
            f' Size of a prepare batch: {prepare_batch_size} \n'
            f' Percentage of single signatures: {prepare_single_sign_percentage} %\n'
            f' Brokerage timeout: {brokerage_timeout} ms\n'
            f' Reduction timeout: {reduction_timeout} ms\n'
            '\n'
            f' Number of signup batches: {signup_batch_number} \n'
            f' Size of a signup batch: {signup_batch_size} \n'
            f' Total number of signups: {signup_batch_number * signup_batch_size * self.num_fast_brokers} \n'
            f' Number of prepare batches: {prepare_batch_number} \n'
            f' Total number of TX (fast): {prepare_batch_number * prepare_batch_size * self.num_fast_brokers} \n'
            '\n'
            ' + RESULTS:\n'
            f' Prepare TPS: {round(prepare_throughput):,} Prepare/s\n'
            f' Prepare end-to-end latency: {round(prepare_latency):,} ms\n'
            '\n'
            f' Mean brokerage batch size: {round(mean_brokerage_batch_size, 1):,} %\n'
            f' Mean reduction percentage: {round(mean_reduction_percentage, 1):,} %\n'
            '\n'
            f' Prepare pre-compute latency: {round(prepare_pre_compute_latency):,} ms\n'
            f' Total prepare batch processing latency: {round(prepare_batch_p_latency):,} ms\n'
            f' - batch correctness validate latency: {round(batch_validate):,} ms\n'
            f' - batch witness validate latency: {round(batch_witness):,} ms\n'
            f' - batch apply latency: {round(batch_apply):,} ms\n'
            f' Total prepare commits processing latency: {round(prepare_commits_p_latency):,} ms\n'
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
    def process(cls, directory, faults=0) :
        assert isinstance(directory, str)

        fast = []
        for filename in sorted(glob(join(directory, 'fast-broker-*.log'))):
            with open(filename, 'r') as f:
                fast += [f.read()]
        full = []
        for filename in sorted(glob(join(directory, 'full-broker-*.log'))):
            with open(filename, 'r') as f:
                full += [f.read()]
        clients = []
        for filename in sorted(glob(join(directory, 'client-*.log'))):
            with open(filename, 'r') as f:
                clients += [f.read()]
        nodes = []
        for filename in sorted(glob(join(directory, 'replica-*.log'))):
            with open(filename, 'r') as f:
                nodes += [f.read()]

        return cls(nodes, fast, full, clients, faults=faults)
