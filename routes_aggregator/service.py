import logging

from routes_aggregator.db_accessor import DbAccessor
from routes_aggregator.exceptions import ApplicationException
from routes_aggregator.model_provider import ModelProvider
from routes_aggregator.utils import singleton, read_config_file
from routes_aggregator.storage_adapter import FilesystemStorageAdapter


def shielded_execute(executor):
    def shielded_executor(*args, **kwargs):
        try:
            return executor(*args, **kwargs)
        except Exception as e:
            raise ApplicationException()
    return shielded_executor


@singleton
class Service:

    def __init__(self, *args, **kwargs):
        if 'config_path' in kwargs:
            config = read_config_file(kwargs['config_path'])
        else:
            config = kwargs

        logger = self.init_logger('routes-aggregator', config)
        self.db_accessor = DbAccessor(
            (config['db_user'], config['db_password']),
            logger
        )
        self.model_provider = ModelProvider(
            FilesystemStorageAdapter(config['storage_path']),
            logger
        )

    @staticmethod
    def init_logger(logger_name, config):

        logger = logging.getLogger(logger_name)

        if 'error_log_path' in config:
            error_handler = logging.FileHandler(config['error_log_path'])
            error_handler.setLevel(logging.ERROR)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            error_handler.setFormatter(formatter)
            logger.addHandler(error_handler)

        if 'debug_log_path' in config:
            debug_handler = logging.FileHandler(config['debug_log_path'])
            debug_handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            debug_handler.setFormatter(formatter)
            logger.addHandler(debug_handler)

        return logger

    @shielded_execute
    def get_station(self, station_id, language):
        return self.db_accessor.get_station(station_id)

    @shielded_execute
    def find_stations(self, station_name, language,
                      search_mode=None, limit=None):
        return self.db_accessor.find_stations(station_name, language, search_mode, limit)

    @shielded_execute
    def get_route(self, route_id, language):
        return self.db_accessor.get_route(route_id)

    @shielded_execute
    def find_routes(self, language, route_number=None,
                    station_ids=None, search_mode=None, limit=None):
        if route_number:
            return self.db_accessor.find_routes_by_route_number(
                route_number, search_mode, limit
            )
        else:
            return self.db_accessor.find_routes_by_station_ids(
                station_ids, limit
            )

    @shielded_execute
    def find_paths(self, departure_station_ids, arrival_station_ids, search_mode=None,
                   transfers_count=None, max_transitions_count=None, limit=None):
        if not search_mode or search_mode.upper() == "REGULAR":
            return self.db_accessor.find_paths(
                departure_station_ids, arrival_station_ids,
                transfers_count, limit
            )
        else:
            return self.db_accessor.find_shortest_paths(
                departure_station_ids, arrival_station_ids,
                max_transitions_count, limit
            )

    @shielded_execute
    def request_model_update(self, agent_type, build_model):
        if build_model:
            model = self.model_provider.build_model(agent_type)
        else:
            model = self.model_provider.load_model(agent_type, 'current')
        self.db_accessor.build_model(model)
        return "ok"
