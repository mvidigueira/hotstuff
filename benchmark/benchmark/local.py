import subprocess
from os.path import basename, splitext, join
from time import sleep

from json import load
from benchmark.commands import CommandMaker
from benchmark.config import NodeParameters, ConfigError
from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print, BenchError, PathMaker

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
            self.nodes = bench_parameters_dict['nodes']
            self.fast_brokers = bench_parameters_dict['fast_brokers']
            self.full_brokers = bench_parameters_dict['full_brokers']
            self.full_clients = bench_parameters_dict['full_clients']
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

    def _start_rendezvous(self, num_nodes, num_fast, num_full, num_clients):
        cmd = CommandMaker.run_rendezvous(num_nodes, num_fast, num_full, num_clients, local="true")
        log_file = PathMaker.rendezvous_log_file()

        self._background_run(cmd, log_file)

        return

    def logs():
        return LogParser.process(join('repo', PathMaker.logs_path()))

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

            # Create alias for the client and nodes binary.
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path(self.repo_name))
            subprocess.run([cmd], shell=True, cwd='./repo')

            self.node_parameters.print(join("./repo", PathMaker.parameters_file()))

            Print.info('Binaries processed!')

            Print.info('Starting rendezvous server...')

            # Run the rendezvous server
            cmd = self._start_rendezvous(
                self.nodes,
                self.fast_brokers, 
                self.full_brokers,
                self.full_clients,
            )

            rendezvous_server = "127.0.0.1:9000"

            Print.info('Starting servers...')

            # Run the nodes.
            node_logs = [PathMaker.node_log_file(i) for i in range(self.nodes)]
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

            # Run the fast brokers
            fast_broker_logs = [PathMaker.fast_broker_log_file(i) for i in range(self.fast_brokers)]
            for log_file in fast_broker_logs:
                cmd = CommandMaker.run_broker(
                    rendezvous = rendezvous_server,
                    parameters = PathMaker.parameters_file(),
                    rate = self.rate,
                    debug=debug
                )
                self._background_run(cmd, log_file)

            if self.full_brokers > 0:
                Print.info('Starting full brokers...')

            # Run the full brokers
            full_broker_logs = [PathMaker.full_broker_log_file(i) for i in range(self.full_brokers)]
            for log_file in full_broker_logs:
                cmd = CommandMaker.run_broker(
                    rendezvous = rendezvous_server,
                    parameters = PathMaker.parameters_file(),
                    rate = self.rate,
                    full = True,
                    debug=debug
                )
                self._background_run(cmd, log_file)

            if self.full_clients > 0:
                Print.info('Starting full clients...')

            # Run the clients 
            client_logs = [PathMaker.client_log_file(i) for i in range(self.full_clients)]
            for log_file in client_logs:
                cmd = CommandMaker.run_client(
                    rendezvous = rendezvous_server,
                    parameters = PathMaker.parameters_file(),
                    num_clients = self.full_clients,
                    debug=debug
                )
                self._background_run(cmd, log_file)

            # Wait for the nodes to synchronize
            Print.info('Waiting for the nodes to synchronize...')
            sleep(10)

            # Wait for all transactions to be processed.
            Print.info(f'Running benchmark ({self.duration} sec)...')
            sleep(self.duration)
            self._kill_nodes()

            # Parse logs and return the parser.
            Print.info('Parsing logs...')
            return LogParser.process(join('repo', PathMaker.logs_path()))

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_nodes()
            raise BenchError('Failed to run benchmark', e)
