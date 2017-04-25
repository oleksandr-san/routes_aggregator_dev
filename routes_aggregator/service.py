import logging

from routes_aggregator.db_accessor import DbAccessor
from routes_aggregator.exceptions import ApplicationException
from routes_aggregator.model_provider import ModelProvider
from routes_aggregator.utils import singleton


def shielded_execute(executor):
    def shielded_executor(*args, **kwargs):
        try:
            return executor(*args, **kwargs)
        except Exception as e:
            raise ApplicationException()
    return shielded_executor


@singleton
class Service:

    def __init__(self, db_user, db_password, aws_access_key_id, aws_secret_access_key):
        logger = logging.getLogger("routes_aggregator")
        fh = logging.FileHandler('routes_aggregator.log')
        fh.setLevel(logging.INFO)
        logger.addHandler(fh)

        self.db_accessor = DbAccessor((db_user, db_password), logger)
        self.model_provider = ModelProvider(
            (aws_access_key_id, aws_secret_access_key), logger
        )

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
