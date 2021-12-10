from json import dump, load


class ConfigError(Exception):
    pass

class NodeParameters:
    def __init__(self, json):
        inputs = []
        try:
            inputs += [json['broker']['signup_batch_number']]
            inputs += [json['broker']['signup_batch_size']]
            inputs += [json['broker']['prepare_batch_size']]
            inputs += [json['broker']['prepare_batch_number']]
            inputs += [json['broker']['prepare_single_sign_percentage']]
        except KeyError as e:
            raise ConfigError(f'Malformed parameters: missing key {e}')

        if not all(isinstance(x, int) for x in inputs):
            raise ConfigError('Invalid parameters type')
            
        self.json = json

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'w') as f:
            dump(self.json, f, indent=4, sort_keys=True)


class BenchParameters:
    def __init__(self, json):
        try:
            nodes = json['nodes'] 
            nodes = nodes if isinstance(nodes, list) else [nodes]

            if not nodes or any(any(y < 1 for y in x.values()) for x in nodes):
                raise ConfigError('Missing or invalid number of nodes')

            self.nodes = [{y: int(x[y]) for y in x} for x in nodes]

            fast_brokers = json['fast_brokers'] 
            fast_brokers = fast_brokers if isinstance(fast_brokers, list) else [fast_brokers]

            if not fast_brokers:
                self.fast_brokers = [dict()]
            else:
                if any(any(y < 1 for y in x.values()) for x in fast_brokers):
                    raise ConfigError('Missing or invalid number of fast_brokers')
                self.fast_brokers = [{y: int(x[y]) for y in x} for x in fast_brokers]

            if len(self.fast_brokers) != 1:
                raise ConfigError('The number of fast brokers cannot change between runs')

            rate = json['rate'] 
            rate = rate if isinstance(rate, list) else [rate]
            if not rate:
                raise ConfigError('Missing input rate')

            self.rate = [int(x) for x in rate]
            self.tx_size = int(json['tx_size'])
            self.faults = int(json['faults'])
            self.duration = int(json['duration'])
            self.runs = int(json['runs']) if 'runs' in json else 1
        except KeyError as e:
            raise ConfigError(f'Malformed bench parameters: missing key {e}')

        except ValueError:
            raise ConfigError('Invalid parameters type')

        if any(min(x.values()) <= self.faults for x in self.nodes):
            raise ConfigError('There should be more nodes than faults')


class PlotParameters:
    def __init__(self, json):
        try:
            nodes = json['nodes'] 
            nodes = nodes if isinstance(nodes, list) else [nodes]
            if not nodes:
                raise ConfigError('Missing number of nodes')
            self.nodes = [int(x) for x in nodes]

            self.tx_size = int(json['tx_size'])

            faults = json['faults'] 
            faults = faults if isinstance(faults, list) else [faults]
            self.faults = [int(x) for x in faults] if faults else [0]

            max_lat = json['max_latency'] 
            max_lat = max_lat if isinstance(max_lat, list) else [max_lat]
            if not max_lat:
                raise ConfigError('Missing max latency')
            self.max_latency = [int(x) for x in max_lat]

        except KeyError as e:
            raise ConfigError(f'Malformed bench parameters: missing key {e}')

        except ValueError:
            raise ConfigError('Invalid parameters type')

