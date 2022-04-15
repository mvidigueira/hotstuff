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
        hosts = self.manager.hosts()

        Print.info(str({region: len(ips) for (region, ips) in hosts.items()}))

        validators = []
        brokers = []
        clients = []

        regions = set(bench_parameters.validators.keys())
        colocated = bench_parameters.colocated_brokers
        if not colocated:
            regions |= set(bench_parameters.brokers.keys())
        for region in regions:
            if region not in hosts.keys():
                raise BenchError('Bad launch configuration: Region \'' + region + '\' is not included in the list of hosts (check settings.json)')

        for region in regions:
            num_validators = bench_parameters.validators.get(region, 0)

            num_brokers = 0
            if not colocated:
                num_brokers = bench_parameters.brokers.get(region, 0)

            num_clients = num_brokers
            if colocated:
                num_clients = num_validators

            ips = hosts[region]
            if len(ips) >= num_validators + num_brokers + num_clients:
                validators += ips[:num_validators]

                if colocated:
                    brokers += ips[:num_validators]
                else:
                    brokers += ips[num_validators:num_validators+num_brokers]

                clients += ips[num_validators+num_brokers:num_validators+num_brokers+num_clients]
            else:
                Print.warn('Only ' + str(len(ips)) + ' out of ' + str(num_validators) +\
                        '+' + str(num_brokers) + '+' + str(num_clients) \
                            + ' (validators + separate brokers + clients) instances available in region \'' + region + "\'")
                return
            
        return (validators, brokers, clients)

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

    def _run_single(self, validators, brokers, clients, rate, duration, debug=False):
        Print.info('Booting testbed...')

        # Kill any potentially unfinished run and delete logs.
        self.kill(hosts=validators + brokers + clients, delete_logs=True)

        Print.info('Starting rendezvous server...')

        # Run the rendezvous server
        num_validators = len(validators)
        num_brokers = len(brokers)
        num_clients = len(clients)
        self._start_rendezvous(validators[0], num_validators, num_brokers, num_clients)

        rendezvous_server = validators[0] + ":9000"

        Print.info('Rendezvous server: ' + rendezvous_server)

        Print.info('Starting the validators...')

        log_file = PathMaker.node_log_file(0)
        cmd = CommandMaker.run_node(
                rendezvous = rendezvous_server,
                discovery = rendezvous_server,
                parameters = PathMaker.parameters_file(),
                debug=debug
            )
        self._background_run_multiple(validators, cmd, log_file)

        # Wait for the nodes to synchronize
        Print.info('Waiting for the nodes to synchronize...')
        sleep(10)
        Print.info('Starting the broker(s)...')

        # Run the full brokers
        broker_logs = [PathMaker.full_broker_log_file(i) for i in range(len(brokers))]
        for broker, log_file in zip(brokers, broker_logs):
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

        sleep_time = 200
        interval = sleep_time / 20
        progress = progress_bar([interval] * 20, prefix='Waiting for the broker(s) to finish prepare. Sleeping...')
        for i in progress:
            sleep(i)

        Print.info('Killing nodes...')

        self.kill(hosts=validators + brokers + clients, delete_logs=False)

    def _logs(self, validators, brokers, clients):
        # Delete local logs (if any).
        cmd = CommandMaker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        Print.info('Getting rendezvous log...')

        c = Connection(validators[0], user='ubuntu', connect_kwargs=self.connect)
        c.get(PathMaker.rendezvous_log_file(), local=PathMaker.rendezvous_log_file())

        # Download broker log files.
        if brokers:
            progress = progress_bar(brokers, prefix='Downloading broker logs:')
            for (i, broker) in enumerate(progress):
                cmd = 'scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null\
                     -i ' + self.manager.settings.key_path + ' -C ubuntu@' + broker + ':' \
                         + PathMaker.full_broker_log_file(0) + " " + PathMaker.full_broker_log_file(i) + " & "
                subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Download client log files.
        if clients:
            progress = progress_bar(clients, prefix='Downloading client logs:')
            for i, host in enumerate(progress):
                c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
                c.get(PathMaker.client_log_file(i), local=PathMaker.client_log_file(i))

        # Download validator log files.
        progress = progress_bar(validators, prefix='Downloading validator logs:')
        for (i, host) in enumerate(progress):
            cmd = 'scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null\
                    -i ' + self.manager.settings.key_path + ' -C ubuntu@' + host + ':' \
                        + PathMaker.node_log_file(0) + " " + PathMaker.node_log_file(i) + " & "
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        sleep(5)

        # Parse logs and return the parser.
        Print.info('All logs downloaded!')

        return LogParser.process(PathMaker.logs_path())

    def _start_rendezvous(self, host, num_validators, num_brokers, num_clients):
        cmd = CommandMaker.run_rendezvous(num_validators, num_brokers, num_clients)
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
        (validators, brokers, clients) = self._select_hosts(bench_parameters)
        if not validators:
            return
        
        self._logs(validators, brokers, clients).print(PathMaker.result_file(
            len(validators), len(brokers), len(clients), 0
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
        (validators, brokers, clients) = self._select_hosts(bench_parameters)
        if not validators:
            return
    
        Print.heading('Validators: ' + str(validators))
        Print.heading('Brokers: ' + str(brokers))
        Print.heading('Clients: ' + str(clients))

        hosts = validators + clients
        if not bench_parameters.colocated_brokers:
            hosts += brokers

        # Update nodes.
        try:
            self._update(hosts)
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to update nodes', e)

        # Run benchmarks.
        Print.heading(f'\nRunning {len(validators)} node(s)')
        Print.heading(f'\nRunning {len(brokers)} (non-colocated) broker(s)')
        Print.heading(f'\nRunning {len(clients)} client(s)')

        rate = bench_parameters.rate
        duration = bench_parameters.duration

        # Upload all configuration files.
        try:
            pass
            self._config(hosts, node_parameters)
        except (subprocess.SubprocessError, GroupException) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            Print.error(BenchError('Failed to configure nodes', e))
            return

        # Run the benchmark.
        for i in range(bench_parameters.runs):
            Print.heading(f'Run {i+1}/{bench_parameters.runs}')
            try:
                self._run_single(
                    validators, brokers, clients, rate, duration, debug
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
