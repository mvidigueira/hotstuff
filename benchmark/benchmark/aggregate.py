from re import search
from collections import defaultdict
from statistics import mean, stdev
from glob import glob
from copy import deepcopy
from os.path import join
import os

from benchmark.utils import PathMaker


class Setup:
    def __init__(self, nodes, brokers, rate, faults):
        self.nodes = nodes
        self.brokers = brokers
        self.rate = rate
        self.faults = faults

    def __str__(self):
        return (
            f' Number of replicas: {self.nodes}\n'
            f' Number of brokers: {self.brokers}\n'
            f' Input rate: {self.rate} ops/s\n'
            f' Faults: {self.faults} replicas\n'
        )

    def __eq__(self, other):
        return isinstance(other, Setup) and str(self) == str(other)

    def __hash__(self):
        return hash(str(self))

    @classmethod
    def from_str(cls, raw):
        nodes = int(search(r'.* Number of replicas: (\d+)', raw).group(1))
        brokers = int(search(r'.* Number of fast brokers: (\d+)', raw).group(1))
        full = int(search(r'.* Number of full brokers: (\d+)', raw).group(1))
        rate = int(search(r'.* Input rate cap: (\d+)', raw).group(1))
        faults = int(search(r'.* Faults: (\d+)', raw).group(1))
        return cls(nodes, brokers + full, rate, faults)


class Result:
    def __init__(self, mean_tps, mean_latency, std_tps=0, std_latency=0):
        self.mean_tps = mean_tps
        self.mean_latency = mean_latency
        self.std_tps = std_tps
        self.std_latency = std_latency

    def __str__(self):
        return(
            f' TPS: {self.mean_tps} +/- {self.std_tps} tx/s\n'
            f' Op Latency: {self.mean_latency} +/- {self.std_latency} ms\n'
        )

    @classmethod
    def from_str(cls, raw):
        tps = int(search(r'.* Prepare TPS: (\d+)', raw).group(1))
        latency = int(search(r'.* Prepare end-to-end latency: (\d+)', raw).group(1))
        return cls(tps, latency)

    @classmethod
    def aggregate(cls, results):
        if len(results) == 1:
            return results[0]

        mean_tps = round(mean([x.mean_tps for x in results]))
        mean_latency = round(mean([x.mean_latency for x in results]))
        std_tps = round(stdev([x.mean_tps for x in results]))
        std_latency = round(stdev([x.mean_latency for x in results]))
        return cls(mean_tps, mean_latency, std_tps, std_latency)


class LogAggregator:
    def __init__(self):
        data = ''
        for filename in glob(join(PathMaker.results_path(), '*.txt')):
            with open(filename, 'r') as f:
                data += f.read()

        records = defaultdict(list)
        for chunk in data.replace(',', '').split('SUMMARY')[1:]:
            if chunk:
                records[Setup.from_str(chunk)] += [Result.from_str(chunk)]

        self.records = {k: Result.aggregate(v) for k, v in records.items()}

    def print(self):
        if not os.path.exists(PathMaker.plots_path()):
            os.makedirs(PathMaker.plots_path())

        results = [
            self._print_latency()
        ]
        for name, records in results:
            for setup, values in records.items():
                data = '\n'.join(
                    f' Variable value: X={x}\n{y}' for x, y in values
                )
                string = (
                    '\n'
                    '-----------------------------------------\n'
                    ' RESULTS:\n'
                    '-----------------------------------------\n'
                    f'{setup}'
                    '\n'
                    f'{data}'
                    '-----------------------------------------\n'
                )
                filename = PathMaker.agg_file(
                    name,
                    setup.nodes, 
                    setup.brokers,
                    setup.faults,
                )
                with open(filename, 'w') as f:
                    f.write(string)

    def _print_latency(self):
        records = deepcopy(self.records)
        organized = defaultdict(list)
        for setup, result in records.items():
            rate = setup.rate
            setup.rate = 'any'
            organized[setup] += [(result.mean_tps, result, rate)]

        for setup, results in list(organized.items()):
            results.sort(key=lambda x: x[2])
            organized[setup] = [(x, y) for x, y, _ in results]

        return 'latency', organized

    def _print_tps(self):
        records = deepcopy(self.records)
        organized = defaultdict(list)
        for max_latency in self.max_latencies:
            for setup, result in records.items():
                setup = deepcopy(setup)
                if result.mean_latency <= max_latency:
                    nodes = setup.nodes
                    setup.nodes = 'x'
                    setup.rate = 'any'
                    setup.max_latency = max_latency

                    new_point = all(nodes != x[0] for x in organized[setup])
                    highest_tps = False
                    for w, r in organized[setup]:
                        if result.mean_tps > r.mean_tps and nodes == w:
                            organized[setup].remove((w, r))
                            highest_tps = True
                    if new_point or highest_tps:
                        organized[setup] += [(nodes, result)]

        [v.sort(key=lambda x: x[0]) for v in organized.values()]
        return 'tps', organized

    def _print_robustness(self):
        records = deepcopy(self.records)
        organized = defaultdict(list)
        for setup, result in records.items():
            rate = setup.rate
            setup.rate = 'x'
            organized[setup] += [(rate, result)]

        [v.sort(key=lambda x: x[0]) for v in organized.values()]
        return 'robustness', organized
