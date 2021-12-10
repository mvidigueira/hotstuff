from os.path import join

from benchmark.utils import PathMaker


class CommandMaker:

    @staticmethod
    def cleanup():
        return (
            f'rm -r .db-* ; rm .*.json ; mkdir -p {PathMaker.results_path()}'
        )

    @staticmethod
    def clean_logs():
        return f'rm -r {PathMaker.logs_path()} ; mkdir -p {PathMaker.logs_path()}'

    @staticmethod
    def compile():
        return 'cargo build --quiet --release --features benchmark'

    @staticmethod
    def generate_key(filename):
        assert isinstance(filename, str)
        return f'./node keys --filename {filename}'

    @staticmethod
    def run_node(rendezvous, discovery, parameters, debug=False):
        assert isinstance(rendezvous, str)
        assert isinstance(discovery, str)
        # assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'./replica {v} run --rendezvous {rendezvous} --discovery {discovery} '
                f' --parameters {parameters}')

    @staticmethod
    def run_broker(rendezvous, parameters, full=False, debug=False):
        assert isinstance(rendezvous, str)
        assert isinstance(full, bool)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        full = '--full=true' if full else '--full=false'
        return (f'./broker {v} run --rendezvous {rendezvous} ' + full +
                f' --parameters {parameters}')

    @staticmethod
    def run_client(address, size, rate, timeout, nodes=[]):
        assert isinstance(address, str)
        assert isinstance(size, int) and size > 0
        assert isinstance(rate, int) and rate >= 0
        assert isinstance(nodes, list)
        assert all(isinstance(x, str) for x in nodes)
        nodes = f'--nodes {" ".join(nodes)}' if nodes else ''
        return (f'./client {address} --size {size} '
                f'--rate {rate} --timeout {timeout} {nodes}')

    @staticmethod
    def run_rendezvous(num_nodes, num_brokers):
        assert isinstance(num_nodes, int)
        return f'./rendezvous -vv run --size {num_nodes} --brokers {num_brokers}'

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        rendezvous, replica, broker = join(origin, 'rendezvous'), join(origin, 'replica'), join(origin, 'broker')
        return f'rm rendezvous; rm replica ; rm broker; ln -s {rendezvous} .; ln -s {replica} .; ln -s {broker} .'
