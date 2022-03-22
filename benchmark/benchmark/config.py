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
            inputs += [json['broker']['brokerage_timeout']]
            inputs += [json['broker']['reduction_timeout']]
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

            full_brokers = json['full_brokers'] 
            full_brokers = full_brokers if isinstance(full_brokers, list) else [full_brokers]

            if not full_brokers:
                self.full_brokers = [dict()]
            else:
                if any(any(y < 1 for y in x.values()) for x in full_brokers):
                    raise ConfigError('Missing or invalid number of full_brokers')
                self.full_brokers = [{y: int(x[y]) for y in x} for x in full_brokers]

            if len(self.full_brokers) != 1:
                raise ConfigError('The number of full brokers cannot change between runs')

            full_clients = json['full_clients'] 
            full_clients = full_clients if isinstance(full_clients, list) else [full_clients]

            if not full_clients:
                self.full_clients = [dict()]
            else:
                if any(any(y < 1 for y in x.values()) for x in full_clients):
                    raise ConfigError('Missing or invalid number of full_brokers')
                self.full_clients = [{y: int(x[y]) for y in x} for x in full_clients]

            if len(self.full_clients) != 1:
                raise ConfigError('The number of full brokers cannot change between runs')

            self.rate = int(json['rate'])
            self.duration = int(json['duration'])
            self.runs = int(json['runs']) if 'runs' in json else 1
        except KeyError as e:
            raise ConfigError(f'Malformed bench parameters: missing key {e}')

        except ValueError:
            raise ConfigError('Invalid parameters type')


class PlotParameters:
    def __init__(self, json):
        try:
            nodes = json['nodes'] 
            nodes = nodes if isinstance(nodes, list) else [nodes]
            if not nodes:
                raise ConfigError('Missing number of nodes')
            self.nodes = [int(x) for x in nodes]

            brokers = json['brokers'] 
            brokers = brokers if isinstance(brokers, list) else [brokers]
            if not brokers:
                raise ConfigError('Missing number of nodes')
            self.brokers = [int(x) for x in brokers]

            faults = json['faults'] 
            faults = faults if isinstance(faults, list) else [faults]
            self.faults = [int(x) for x in faults] if faults else [0]

        except KeyError as e:
            raise ConfigError(f'Malformed bench parameters: missing key {e}')

        except ValueError:
            raise ConfigError('Invalid parameters type')

