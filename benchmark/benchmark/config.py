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
            validators = json['validators']
            assert isinstance(validators, dict)

            if not validators or any(not isinstance(x, int) or x < 1 for x in validators.values()):
                raise ConfigError('Missing or invalid number of validators')

            self.validators = validators

            colocated_brokers = json['broker_colocation']
            if colocated_brokers:
                self.colocated_brokers = True
            else:
                self.colocated_brokers = False
                brokers = json['brokers']
                assert isinstance(brokers, dict)
                if not brokers or any(not isinstance(x, int) or x < 1 for x in validators.values()):
                    raise ConfigError('Missing or invalid number of brokers')
                self.brokers = brokers

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
            validators = json['validators'] 
            validators = validators if isinstance(validators, list) else [validators]
            if not validators:
                raise ConfigError('Missing number of validators')
            self.validators = [int(x) for x in validators]

            brokers = json['brokers'] 
            brokers = brokers if isinstance(brokers, list) else [brokers]
            if not brokers:
                raise ConfigError('Missing number of validators')
            self.brokers = [int(x) for x in brokers]

            faults = json['faults'] 
            faults = faults if isinstance(faults, list) else [faults]
            self.faults = [int(x) for x in faults] if faults else [0]

        except KeyError as e:
            raise ConfigError(f'Malformed bench parameters: missing key {e}')

        except ValueError:
            raise ConfigError('Invalid parameters type')

