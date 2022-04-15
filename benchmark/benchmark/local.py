import subprocess
from os.path import basename, splitext, join
from time import sleep

from json import load
from benchmark.commands import CommandMaker
from benchmark.config import NodeParameters, ConfigError
from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print, BenchError, PathMaker

from re import search
from glob import glob

def _load_repo_settings(filename):
    try:
        with open(filename, 'r') as f:
            data = load(f)

        return (data['repo']['name'], data['repo']['url'], data['repo']['branch'])
    except Exception as e:
        raise BenchError(f'Error parsing settings file', e)

class LocalBench:
    def __init__(self, bench_parameters_dict, node_parameters_dict, settings_file='settings.json'):
        try:
            self.validators = bench_parameters_dict['validators']
            self.brokers = bench_parameters_dict['brokers']
            self.clients = bench_parameters_dict['clients']
            self.rate = bench_parameters_dict['rate']
            self.duration = bench_parameters_dict['duration']

            self.node_parameters = NodeParameters(node_parameters_dict)
            self.repo_name, self.repo_url, self.repo_branch = _load_repo_settings(settings_file)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _background_run(self, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'{command} |& tee {log_file}'
        subprocess.run(['tmux', 'new', '-d', '-s', name, cmd], check=True, cwd="./repo")

    def _kill_nodes(self):
        try:
            cmd = CommandMaker.kill().split()
            subprocess.run(cmd, stderr=subprocess.DEVNULL)
        except subprocess.SubprocessError as e:
            raise BenchError('Failed to kill testbed', e)

    def _start_rendezvous(self, num_validators, num_brokers, num_clients):
        cmd = CommandMaker.run_rendezvous(num_validators, num_brokers, num_clients, local="true")
        log_file = PathMaker.rendezvous_log_file()

        self._background_run(cmd, log_file)

        return

    def logs():
        return LogParser.process(join('repo', PathMaker.logs_path()))

    def _benchmark_done(self):
        return self.clients == 0 or self._clients_done()

    def _clients_done(self):
        logs = []
        for filename in sorted(glob(join("repo", "logs", 'client-*.log'))):
            with open(filename, 'r') as f:
                logs += [f.read()]
        return len(logs) != 0 and all([search(r'Client done!', log) is not None for log in logs])

    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting local benchmark')

        # Kill any previous testbed.
        self._kill_nodes()

        try:
            Print.info('Setting up testbed...')

            # Cleanup all files.
            cmd = f'{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}'
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL, cwd="./repo")
            sleep(0.5) # Removing may take time.

            cmd = CommandMaker.update(self.repo_branch)
            subprocess.run([cmd], shell=True, cwd=PathMaker.node_crate_path(self.repo_name))

            # Recompile the latest code.
            cmd = CommandMaker.compile()
            subprocess.run([cmd], shell=True, cwd=PathMaker.node_crate_path(self.repo_name))
            
            sleep(1)

            # Create alias for the client and nodes binary.
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path(self.repo_name))
            subprocess.run([cmd], shell=True, cwd='./repo')

            sleep(1)

            self.node_parameters.print(join("./repo", PathMaker.parameters_file()))

            Print.info('Binaries processed!')

            Print.info('Starting rendezvous server...')

            # Run the rendezvous server
            cmd = self._start_rendezvous(
                self.validators,
                self.brokers,
                self.clients,
            )

            rendezvous_server = "127.0.0.1:9000"

            Print.info('Starting servers...')

            # Run the nodes.
            node_logs = [PathMaker.node_log_file(i) for i in range(self.validators)]
            for log_file in node_logs:
                cmd = CommandMaker.run_node(
                    rendezvous = rendezvous_server,
                    discovery = rendezvous_server,
                    parameters = PathMaker.parameters_file(),
                    debug=debug
                )
                self._background_run(cmd, log_file)

            if self.fast_brokers > 0:
                Print.info('Starting fast brokers...')

            # Run the full brokers
            broker_logs = [PathMaker.full_broker_log_file(i) for i in range(self.brokers)]
            for log_file in broker_logs:
                cmd = CommandMaker.run_broker(
                    rendezvous = rendezvous_server,
                    parameters = PathMaker.parameters_file(),
                    rate = self.rate,
                    full = True,
                    debug=debug
                )
                self._background_run(cmd, log_file)

            if self.clients > 0:
                Print.info('Starting full clients...')

            # Run the clients 
            client_logs = [PathMaker.client_log_file(i) for i in range(self.clients)]
            for log_file in client_logs:
                cmd = CommandMaker.run_client(
                    rendezvous = rendezvous_server,
                    parameters = PathMaker.parameters_file(),
                    num_clients = self.clients,
                    debug=debug
                )
                self._background_run(cmd, log_file)

            log_file = PathMaker.dstat_file(0)
            cmd = CommandMaker.run_dstat()
            self._background_run(cmd, log_file)

            # Wait for all transactions to be processed.
            Print.info(f'Running benchmark ({self.duration} sec)...')
            for i in range(0, self.duration, 5):
                if self._benchmark_done():
                    Print.info(f'All brokers/clients done! Benchmark finished in {i} seconds.')
                    break
                sleep(5)
            self._kill_nodes()

            if not self._benchmark_done():
                Print.info(f'Some brokers/clients did not finish! Benchmark terminated in {self.duration} seconds.')

            # Parse logs and return the parser.
            Print.info('Parsing logs...')
            return LogParser.process(join('repo', PathMaker.logs_path()))

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_nodes()
            raise BenchError('Failed to run benchmark', e)