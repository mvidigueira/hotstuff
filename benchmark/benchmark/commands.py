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
    def run_node(rendezvous, discovery, parameters, debug=False):
        assert isinstance(rendezvous, str)
        assert isinstance(discovery, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'ulimit -n 1000000 && ./replica {v} run --rendezvous {rendezvous} --discovery {discovery} '
                f' --parameters {parameters}')

    @staticmethod
    def run_broker(rendezvous, parameters, rate, full=False, debug=False):
        assert isinstance(rendezvous, str)
        assert isinstance(full, bool)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        full = '--full=true' if full else '--full=false'
        return (f'ulimit -n 1000000 && ./broker {v} run --rendezvous {rendezvous} ' + full +
                f' --parameters {parameters} --rate {rate}')

    @staticmethod
    def run_client(rendezvous, parameters, num_clients, debug=False):
        assert isinstance(rendezvous, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'ulimit -n 1000000 && ./client {v} run --rendezvous {rendezvous}' +\
                f' --parameters {parameters} --num_clients {num_clients}')

    @staticmethod
    def run_rendezvous(num_nodes, num_fast, num_full, num_clients):
        assert isinstance(num_nodes, int)
        return f'ulimit -n 1000000 && ./rendezvous -vv run --size {num_nodes} --fast_brokers {num_fast} --full_brokers {num_full} --num_clients {num_clients}'

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        rendezvous, replica, broker, client = join(origin, 'rendezvous'), join(origin, 'replica'), join(origin, 'broker'), join(origin, 'client')
        return f'rm rendezvous; rm replica ; rm broker; rm client; ln -s {rendezvous} .; ln -s {replica} .; ln -s {broker} .; ln -s {client} .'
