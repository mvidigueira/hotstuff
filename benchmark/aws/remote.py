from os import error
from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os.path import basename, splitext
from time import sleep
from math import ceil
from os.path import join
import subprocess

from benchmark.config import Committee, Key, NodeParameters, BenchParameters, ConfigError
from benchmark.utils import BenchError, Print, PathMaker, progress_bar
from benchmark.commands import CommandMaker
from benchmark.logs import LogParser, ParseError
from aws.instance import InstanceManager


class FabricError(Exception):
    ''' Wrapper for Fabric exception with a meaningfull error message. '''

    def __init__(self, error):
        assert isinstance(error, GroupException)
        message = list(error.result.values())[-1]
        super().__init__(message)


class ExecutionError(Exception):
    pass


class Bench:
    def __init__(self, ctx):
        self.manager = InstanceManager.make()
        self.settings = self.manager.settings
        try:
            ctx.connect_kwargs.pkey = RSAKey.from_private_key_file(
                self.manager.settings.key_path
            )
            self.connect = ctx.connect_kwargs
        except (IOError, PasswordRequiredException, SSHException) as e:
            raise BenchError('Failed to load SSH key', e)

    def _check_stderr(self, output):
        if isinstance(output, dict):
            for x in output.values():
                if x.stderr:
                    raise ExecutionError(x.stderr)
        else:
            if output.stderr:
                raise ExecutionError(output.stderr)

    def install(self):
        Print.info('Installing rust and cloning the repo...')
        cmd = [
            'sudo apt-get update',
            'sudo apt-get -y upgrade',
            'sudo apt-get -y autoremove',

            # The following dependencies prevent the error: [error: linker `cc` not found].
            'sudo apt-get -y install build-essential',
            'sudo apt-get -y install cmake',

            # Install rust (non-interactive).
            'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y',
            'source $HOME/.cargo/env',
            'rustup default stable',

            # Clone the repo.
            f'(git clone {self.settings.repo_url} || (cd {self.settings.repo_name} ; git pull))'
        ]
        hosts = self.manager.hosts(flat=True)
        try:
            g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)
            Print.heading(f'Initialized testbed of {len(hosts)} nodes')
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to install repo on testbed', e)

    def kill(self, hosts=[], delete_logs=False):
        assert isinstance(hosts, list)
        assert isinstance(delete_logs, bool)
        hosts = hosts if hosts else self.manager.hosts(flat=True)
        delete_logs = CommandMaker.clean_logs() if delete_logs else 'true'
        cmd = [delete_logs, f'({CommandMaker.kill()} || true)']
        try:
            g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)
        except GroupException as e:
            raise BenchError('Failed to kill nodes', FabricError(e))

    def _select_hosts(self, bench_parameters):
        # Ensure there are enough hosts.
        selected = []
        fast = []

        hosts = self.manager.hosts()

        for run in bench_parameters.nodes:
            run_hosts = []
            run_fast = []
            for region, number in run.items():
                num_fast = bench_parameters.fast_brokers[0].get(region, 0)

                if region in hosts:
                    ips = hosts[region]
                    if len(ips) >= number + num_fast:
                        run_hosts += ips[:number]
                        run_fast += ips[number:number+num_fast]
                    else:
                        Print.warn('Only ' + len(ips) + ' out of ' + number  + '+' + num_fast + ' (replicas + fast brokers) instances available in region \'' + region + "\'")
                        return ([], [])
                else:
                    Print.warn('Region \'' + region + "\' is not included in the list of hosts (check settings.json)")
                    return ([], [])
            selected.append(run_hosts)
            fast.append(run_fast)
            
        return (selected, fast)

    def _background_run(self, host, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
        output = c.run(cmd, hide=True)
        self._check_stderr(output)

    def _update(self, hosts):
        Print.info(
            f'Updating {len(hosts)} nodes (branch "{self.settings.branch}")...'
        )

        tcp_settings = [
            # allow testing with buffers up to 128MB
            'sudo sysctl -w \'net.core.rmem_max=134217728\'',
            'sudo sysctl -w \'net.core.wmem_max=134217728\'',
            # increase Linux autotuning TCP buffer limit to 64MB
            'sudo sysctl -w \'net.ipv4.tcp_rmem=4096 87380 67108864\'',
            'sudo sysctl -w \'net.ipv4.tcp_wmem=4096 65536 67108864\'',
            # recommended default congestion control is htcp 
            'sudo sysctl -w \'net.ipv4.tcp_congestion_control=htcp\'',
            # recommended for hosts with jumbo frames enabled
            'sudo sysctl -w \'net.ipv4.tcp_mtu_probing=1\'',
            # recommended to enable 'fair queueing'
            'sudo sysctl -w \'net.core.default_qdisc=fq\'',
        ]

        cmd = [
            f'(cd {self.settings.repo_name} && git fetch -f)',
            f'(cd {self.settings.repo_name} && git checkout -f {self.settings.branch})',
            f'(cd {self.settings.repo_name} && git pull -f)',
            'source $HOME/.cargo/env',
            f'(cd {self.settings.repo_name} && cargo update)',
            f'(cd {self.settings.repo_name} && {CommandMaker.compile()})',
            CommandMaker.alias_binaries(
                f'./{self.settings.repo_name}/target/release/'
            )
        ]
        g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
        g.run(' && '.join(tcp_settings), hide=True)
        g.run(' && '.join(cmd), hide=True)

    def _config(self, hosts):
        Print.info('Cleaning up nodes...')

        # Cleanup all nodes.
        cmd = f'{CommandMaker.cleanup()} || true'
        g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
        g.run(cmd, hide=True)

        # node_parameters.print(PathMaker.parameters_file())
        
        # Upload configuration files.
        # progress = progress_bar(hosts, prefix='Uploading config files:')
        # for host in progress:
        #     c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
        #     c.put(PathMaker.parameters_file(), '.')

        return

    def _run_single(self, hosts, brokers, rate, bench_parameters, debug=False):
        Print.info('Booting testbed...')

        # Kill any potentially unfinished run and delete logs.
        self.kill(hosts=hosts, delete_logs=True)
        self.kill(hosts=brokers, delete_logs=True)

        Print.info('Starting rendezvous server...')

        # Run the rendezvous server
        num_nodes = len(hosts)
        self._start_rendezvous(hosts[0], num_nodes)

        rendezvous_server = hosts[0] + ":9000"

        Print.info('Rendezvous server: ' + rendezvous_server)

        # Run the nodes.
        node_logs = [PathMaker.node_log_file(i) for i in range(len(hosts))]
        for host, log_file in zip(hosts, node_logs):
            cmd = CommandMaker.run_node(
                rendezvous = rendezvous_server,
                discovery = rendezvous_server,
                debug=debug
            )
            self._background_run(host, cmd, log_file)

        # Wait for the nodes to synchronize
        Print.info('Waiting for the nodes to synchronize...')
        sleep(10)
        Print.info('Starting the broker(s)...')

        # Run the brokers
        broker_logs = [PathMaker.broker_log_file(i) for i in range(len(brokers))]
        for broker, log_file in zip(brokers, broker_logs):
            cmd = CommandMaker.run_broker(
                rendezvous = rendezvous_server,
                debug=debug
            )
            self._background_run(broker, cmd, log_file)

        Print.info('Waiting for the broker(s) to finish...')
        sleep(60)

        self.kill(hosts=hosts, delete_logs=False)
        self.kill(hosts=brokers, delete_logs=False)

    def _logs(self, hosts, brokers, faults):
        # Delete local logs (if any).
        cmd = CommandMaker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        Print.info('Getting rendezvous log...')

        c = Connection(hosts[0], user='ubuntu', connect_kwargs=self.connect)
        c.get(PathMaker.rendezvous_log_file(), local=PathMaker.rendezvous_log_file())

        # Download log files.
        progress = progress_bar(brokers, prefix='Downloading logs:')
        for i, host in enumerate(progress):
            c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
            c.get(PathMaker.broker_log_file(i), local=PathMaker.broker_log_file(i))

        # Download log files.
        progress = progress_bar(hosts, prefix='Downloading logs:')
        for i, host in enumerate(progress):
            c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
            c.get(PathMaker.node_log_file(i), local=PathMaker.node_log_file(i))

        # Parse logs and return the parser.
        Print.info('All logs downloaded!')
        return
        return LogParser.process(PathMaker.logs_path(), faults=faults)

    def _start_rendezvous(self, host, num_nodes):
        cmd = CommandMaker.run_rendezvous(num_nodes)
        log_file = PathMaker.rendezvous_log_file()

        self._background_run(host, cmd, log_file)

        return

    def run(self, bench_parameters_dict, node_parameters_dict, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting remote benchmark')
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            # node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

        # Select which hosts to use.
        (selected_hosts, selected_brokers) = self._select_hosts(bench_parameters)
        if not selected_hosts:
            return

        merged_hosts = list(set.union(*[set(x) for x in selected_hosts]))
        merged_brokers = list(set.union(*[set(x) for x in selected_brokers]))

        Print.heading('Merged hosts: ' + str(merged_hosts))
        Print.heading('Merged brokers: ' + str(merged_brokers))

        # Update nodes.
        try:
            self._update(merged_hosts + merged_brokers)
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to update nodes', e)

        # Run benchmarks.
        for i, nodes in enumerate(bench_parameters.nodes):
            for r in bench_parameters.rate:
                n = sum(nodes.values())
                Print.heading(f'\nRunning {n} nodes (input rate: {r:,} tx/s)')
                hosts = selected_hosts[i]

                b = sum(bench_parameters.fast_brokers[0].values())
                Print.heading(f'\nRunning {b} fast brokers')
                brokers = selected_brokers[i]

                # Upload all configuration files.
                try:
                    self._config(hosts + brokers)
                except (subprocess.SubprocessError, GroupException) as e:
                    e = FabricError(e) if isinstance(e, GroupException) else e
                    Print.error(BenchError('Failed to configure nodes', e))
                    continue

                # Do not boot faulty nodes.
                faults = bench_parameters.faults
                hosts = hosts[:n-faults]

                # Run the benchmark.
                for i in range(bench_parameters.runs):
                    Print.heading(f'Run {i+1}/{bench_parameters.runs}')
                    try:
                        self._run_single(
                            hosts, brokers, r, bench_parameters, debug
                        )
                        sleep(2)
                        self._logs(hosts, brokers, faults)
                        # .print(PathMaker.result_file(
                        #     n, r, bench_parameters.tx_size, faults
                        # ))
                    except (subprocess.SubprocessError, GroupException, ParseError) as e:
                        self.kill(hosts=hosts)
                        if isinstance(e, GroupException):
                            e = FabricError(e)
                        Print.error(BenchError('Benchmark failed', e))
                        continue
