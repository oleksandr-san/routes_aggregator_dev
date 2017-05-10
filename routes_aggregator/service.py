import logging
import sys

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
            Service().logger.error(str(e))
            raise ApplicationException()
    return shielded_executor


@singleton
class Service:

    def __init__(self, *args, **kwargs):
        if 'config_path' in kwargs:
            config = read_config_file(kwargs['config_path'])
        else:
            config = kwargs

        self.logger = logging.getLogger('routes-aggregator')

        self.init_logger(self.logger, config)

        self.db_accessor = DbAccessor(
            (config['db_user'], config['db_password']),
            self.logger
        )
        self.model_provider = ModelProvider(
            FilesystemStorageAdapter(config['storage_path']),
            self.logger
        )

    @staticmethod
    def init_logger(logger, config):

        logger.setLevel(logging.DEBUG)

        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        stdout_handler.setFormatter(formatter)
        logger.addHandler(stdout_handler)

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
    def get_station(self, station_id):
        return self.db_accessor.get_station(station_id)

    @shielded_execute
    def find_stations(self, station_names, search_mode=None, limit=None):
        return self.db_accessor.find_stations(station_names, search_mode, limit)

    @shielded_execute
    def get_route(self, route_id):
        return self.db_accessor.get_route(route_id)

    @shielded_execute
    def find_routes(self, route_numbers=None, station_ids=None,
                    search_mode=None, limit=None):
        routes = []

        if route_numbers:
            routes.extend(
                self.db_accessor.find_routes_by_route_numbers(
                    route_numbers, search_mode, limit
                )
            )

        if station_ids:
            routes.extend(
                self.db_accessor.find_routes_by_station_ids(
                    station_ids, limit
                )
            )

        return routes

    @shielded_execute
    def find_paths(self, station_ids, search_mode=None,
                   max_transitions_count=None, limit=None):
        search_mode = search_mode.upper() if search_mode else "SIMPLE"

        if search_mode == "SIMPLE":
            return self.db_accessor.find_paths_with_single_route(station_ids, limit)
        elif search_mode == "TRANSFERS":
            return self.db_accessor.find_paths_with_multiple_routes(station_ids, limit)
        elif search_mode == "TRANSITIONS":
            return self.db_accessor.find_shortest_paths(
                station_ids[0], station_ids[-1],
                max_transitions_count, limit
            )
        else:
            return []

    @shielded_execute
    def request_model_update(self, agent_type, build_model):
        if build_model:
            model = self.model_provider.build_model(agent_type)
        else:
            model = self.model_provider.load_model(agent_type, 'current')
        self.db_accessor.build_model(model)
        return "ok"
