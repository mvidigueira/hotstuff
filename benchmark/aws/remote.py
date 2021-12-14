from os import error
from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os.path import basename, splitext
from time import sleep
import subprocess

from benchmark.config import NodeParameters, BenchParameters, ConfigError
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
            # 'sudo apt-get -y upgrade',
            # 'sudo apt-get -y autoremove',

            # The following dependencies prevent the error: [error: linker `cc` not found].
            'sudo apt-get -y install build-essential',
            'sudo apt-get -y install cmake',
            'sudo apt-get -y install tmux',

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
        full = []
        clients = []

        hosts = self.manager.hosts()

        Print.info(str({region: len(ips) for (region, ips) in hosts.items()}))

        for config in bench_parameters.nodes:
            run_hosts = []
            run_fast = []
            run_full = []
            run_clients = []
            for region, number in config.items():
                num_fast = bench_parameters.fast_brokers[0].get(region, 0)
                num_full = bench_parameters.full_brokers[0].get(region, 0)
                num_clients = bench_parameters.full_clients[0].get(region, 0)

                if region in hosts:
                    ips = hosts[region]
                    if len(ips) >= number + num_fast + num_full + num_clients:
                        run_hosts += ips[:number]
                        run_fast += ips[number:number+num_fast]
                        run_full += ips[number+num_fast:number+num_fast+num_full]
                        run_clients += ips[number+num_fast+num_full:number+num_fast+num_full+num_clients]
                    else:
                        Print.warn('Only ' + str(len(ips)) + ' out of ' + str(number) +\
                             '+' + str(num_fast) + '+' + str(num_full) + '+' + str(num_clients) \
                                 + ' (replicas + fast + full + clients) instances available in region \'' + region + "\'")
                        return ([], [])
                else:
                    Print.warn('Region \'' + region + "\' is not included in the list of hosts (check settings.json)")
                    return ([], [])
            selected.append(run_hosts)
            fast.append(run_fast)
            full.append(run_full)
            clients.append(run_clients)
            
        return (selected, fast, full, clients)

    def _background_run(self, host, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
        output = c.run(cmd, hide=True)
        self._check_stderr(output)

    def _background_run_multiple(self, hosts, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
        output = g.run(cmd, hide=True)
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
            # recommended to enable 'fair queueing'
            'sudo sysctl -w \'net.core.default_qdisc=fq\'',
            # increase tcp sockets,
            'sudo sysctl -w \'net.core.somaxconn=500000\'',
            'sudo sysctl -w \'net.core.netdev_max_backlog=20000\'',
            'sudo sysctl -w \'net.ipv4.tcp_max_syn_backlog=20000\'',
            'sudo sysctl -w \'net.ipv4.tcp_tw_reuse=1\'',
            'sudo sysctl -w \'net.ipv4.tcp_fin_timeout=60\'',
            
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

    def _config(self, hosts, node_parameters):
        Print.info('Cleaning up nodes...')

        # Cleanup all nodes.
        cmd = f'{CommandMaker.cleanup()} || true'
        g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
        g.run(cmd, hide=True)

        node_parameters.print(PathMaker.parameters_file())
        Print.info('Uploading config files...')
        g.put(PathMaker.parameters_file(), '.')

        return

    def _run_single(self, hosts, fast_brokers, full_brokers, clients, rate, debug=False):
        Print.info('Booting testbed...')

        # Kill any potentially unfinished run and delete logs.
        self.kill(hosts=hosts + fast_brokers + full_brokers + clients, delete_logs=True)

        Print.info('Starting rendezvous server...')

        # Run the rendezvous server
        num_nodes = len(hosts)
        num_fast = len(fast_brokers)
        num_full = len(full_brokers)
        num_clients = len(clients)
        self._start_rendezvous(hosts[0], num_nodes, num_fast, num_full, num_clients)

        rendezvous_server = hosts[0] + ":9000"

        Print.info('Rendezvous server: ' + rendezvous_server)

        Print.info('Starting the nodes...')

        # Run the nodes.
        # node_logs = [PathMaker.node_log_file(i) for i in range(len(hosts))]
        # for host, log_file in zip(hosts, node_logs):
        #     cmd = CommandMaker.run_node(
        #         rendezvous = rendezvous_server,
        #         discovery = rendezvous_server,
        #         parameters = PathMaker.parameters_file(),
        #         debug=debug
        #     )
        #     self._background_run(host, cmd, log_file)

        log_file = PathMaker.node_log_file(0)
        cmd = CommandMaker.run_node(
                rendezvous = rendezvous_server,
                discovery = rendezvous_server,
                parameters = PathMaker.parameters_file(),
                debug=debug
            )
        self._background_run_multiple(hosts, cmd, log_file)

        # Wait for the nodes to synchronize
        Print.info('Waiting for the nodes to synchronize...')
        sleep(10)
        Print.info('Starting the broker(s)...')

        # Run the fast brokers
        # fast_broker_logs = [PathMaker.fast_broker_log_file(i) for i in range(len(fast_brokers))]
        # for broker, log_file in zip(fast_brokers, fast_broker_logs):
        #     cmd = CommandMaker.run_broker(
        #         rendezvous = rendezvous_server,
        #         parameters = PathMaker.parameters_file(),
        #         rate = rate,
        #         debug=debug
        #     )
        #     self._background_run(broker, cmd, log_file)

        log_file = PathMaker.fast_broker_log_file(0)
        cmd = CommandMaker.run_broker(
                rendezvous = rendezvous_server,
                parameters = PathMaker.parameters_file(),
                rate = rate,
                debug=debug
            )
        self._background_run_multiple(fast_brokers, cmd, log_file)

        # Run the full brokers
        full_broker_logs = [PathMaker.full_broker_log_file(i) for i in range(len(full_brokers))]
        for broker, log_file in zip(full_brokers, full_broker_logs):
            cmd = CommandMaker.run_broker(
                rendezvous = rendezvous_server,
                parameters = PathMaker.parameters_file(),
                rate = rate,
                full=True,
                debug=debug
            )
            self._background_run(broker, cmd, log_file)

        # Run the clients 
        client_logs = [PathMaker.client_log_file(i) for i in range(len(clients))]
        for client, log_file in zip(clients, client_logs):
            cmd = CommandMaker.run_client(
                rendezvous = rendezvous_server,
                parameters = PathMaker.parameters_file(),
                num_clients = len(clients),
                debug=debug
            )
            self._background_run(client, cmd, log_file)

        sleep_time = 200
        interval = sleep_time / 20
        progress = progress_bar([interval] * 20, prefix='Waiting for the broker(s) to finish signup. Sleeping...')
        for i in progress:
            sleep(i)

        Print.info('Broker(s) expected to have finished signup.')

        sleep_time = 600
        interval = sleep_time / 20
        progress = progress_bar([interval] * 20, prefix='Waiting for the broker(s) to finish prepare. Sleeping...')
        for i in progress:
            sleep(i)

        Print.info('Killing nodes...')

        self.kill(hosts=hosts + fast_brokers + full_brokers + clients, delete_logs=False)

    def _logs(self, hosts, fast_brokers, full_brokers, clients, faults):
        # Delete local logs (if any).
        cmd = CommandMaker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        Print.info('Getting rendezvous log...')

        c = Connection(hosts[0], user='ubuntu', connect_kwargs=self.connect)
        c.get(PathMaker.rendezvous_log_file(), local=PathMaker.rendezvous_log_file())

        # Download fast broker log files.
        if fast_brokers:
            progress = progress_bar(fast_brokers, prefix='Downloading fast broker logs:')
            for (i, fast_broker) in enumerate(progress):
                cmd = 'scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null\
                     -i ' + self.manager.settings.key_path + ' -C ubuntu@' + fast_broker + ':' \
                         + PathMaker.fast_broker_log_file(0) + " " + PathMaker.fast_broker_log_file(i) + " & "
                subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Download full broker log files.
        if full_brokers:
            progress = progress_bar(full_brokers, prefix='Downloading fast broker logs:')
            for (i, full_broker) in enumerate(progress):
                cmd = 'scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null\
                     -i ' + self.manager.settings.key_path + ' -C ubuntu@' + full_broker + ':' \
                         + PathMaker.full_broker_log_file(0) + " " + PathMaker.full_broker_log_file(i) + " & "
                subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Download client log files.
        if clients:
            progress = progress_bar(clients, prefix='Downloading client logs:')
            for i, host in enumerate(progress):
                c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
                c.get(PathMaker.client_log_file(i), local=PathMaker.client_log_file(i))

        # Download replica log files.
        progress = progress_bar(hosts, prefix='Downloading fast broker logs:')
        for (i, host) in enumerate(progress):
            cmd = 'scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null\
                    -i ' + self.manager.settings.key_path + ' -C ubuntu@' + host + ':' \
                        + PathMaker.node_log_file(0) + " " + PathMaker.node_log_file(i) + " & "
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        sleep(5)

        # Parse logs and return the parser.
        Print.info('All logs downloaded!')

        return LogParser.process(PathMaker.logs_path(), faults=faults)

    def _start_rendezvous(self, host, num_nodes, num_fast, num_full, num_clients):
        cmd = CommandMaker.run_rendezvous(num_nodes, num_fast, num_full, num_clients)
        log_file = PathMaker.rendezvous_log_file()

        self._background_run(host, cmd, log_file)

        return

    def dl_logs(self, bench_parameters_dict, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting logs download')
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

        # Select which hosts to use.
        (selected_hosts, selected_fast, selected_full, selected_clients) = self._select_hosts(bench_parameters)
        if not selected_hosts:
            return

        # Run benchmarks.
        n = sum(bench_parameters.nodes[0].values())
        hosts = selected_hosts[0]

        bfast = sum(bench_parameters.fast_brokers[0].values())
        fast = selected_fast[0]

        bfull = sum(bench_parameters.full_brokers[0].values())
        full = selected_full[0]

        b_clients = sum(bench_parameters.full_clients[0].values())
        clients = selected_clients[0]

        # Do not boot faulty nodes.
        faults = bench_parameters.faults
        
        self._logs(hosts, fast, full, clients, faults).print(PathMaker.result_file(
            n, bfast + bfull, b_clients, faults
        ))
            

    def run(self, bench_parameters_dict, node_parameters_dict, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting remote benchmark')
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

        # Select which hosts to use.
        (selected_hosts, selected_fast, selected_full, selected_clients) = self._select_hosts(bench_parameters)
        if not selected_hosts:
            return
    
        Print.heading('selected_full: ' + str(selected_full))

        merged_hosts = list(set.union(*[set(x) for x in selected_hosts]))
        merged_fast = list(set.union(*[set(x) for x in selected_fast]))
        merged_full = list(set.union(*[set(x) for x in selected_full]))
        merged_clients = list(set.union(*[set(x) for x in selected_clients]))

        Print.heading('Merged hosts: ' + str(merged_hosts))
        Print.heading('Merged fast brokers: ' + str(merged_fast))
        Print.heading('Merged full brokers: ' + str(merged_full))
        Print.heading('Merged clients: ' + str(merged_clients))

        # Update nodes.
        try:
            pass
            self._update(merged_hosts + merged_fast + merged_full + merged_clients)
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to update nodes', e)

        # Run benchmarks.
        for i, nodes in enumerate(bench_parameters.nodes):
            n = sum(nodes.values())
            Print.heading(f'\nRunning {n} nodes')
            hosts = selected_hosts[i]

            bfast = sum(bench_parameters.fast_brokers[0].values())
            Print.heading(f'\nRunning {bfast} fast broker(s)')
            fast = selected_fast[i]

            bfull = sum(bench_parameters.full_brokers[0].values())
            Print.heading(f'\nRunning {bfull} full broker(s)')
            full = selected_full[i]

            b_clients = sum(bench_parameters.full_clients[0].values())
            Print.heading(f'\nRunning {b_clients} client(s)')
            clients = selected_clients[i]

            rate = bench_parameters.rate

            # Upload all configuration files.
            try:
                pass
                self._config(hosts + fast + full + clients, node_parameters)
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
                        hosts, fast, full, clients, rate, debug
                    )
                    self._logs(hosts, fast, full, clients, faults).print(PathMaker.result_file(
                        n, bfast + bfull, b_clients, faults
                    ))
                except (subprocess.SubprocessError, GroupException, ParseError) as e:
                    self.kill(hosts=hosts + fast + full + clients)
                    if isinstance(e, GroupException):
                        e = FabricError(e)
                    Print.error(BenchError('Benchmark failed', e))
                    continue
