import pickle

from routes_aggregator.utils import *
from routes_aggregator.exceptions import AbsentRoutePointException, AbsentPathItemException


class ModelAccessor:

    def __init__(self):
        self.agent_type = ''
        self.stations = {}
        self.routes = {}

    def find_station(self, station_id):
        return self.stations.get(station_id)

    def add_station(self, station):
        self.stations[station.station_id] = station

    def find_route(self, route_id):
        return self.routes.get(route_id)

    def add_route(self, route):
        self.routes[route.route_id] = route

    def save_binary(self, fileobj):
        pickle.dump(self.agent_type, fileobj)
        pickle.dump(self.stations, fileobj)
        pickle.dump(self.routes, fileobj)

    def restore_binary(self, fileobj):
        self.agent_type = pickle.load(fileobj)
        self.stations = pickle.load(fileobj)
        self.routes = pickle.load(fileobj)


class Entity:
    """Base class for entity representation with multilingual properties"""

    def __init__(self):
        self.__properties = None

    def set_property(self, name, language, value):
        self.ensure_properties()[self.prepare_property(name, language)] = value

    def get_property(self, name, language):
        return self.__properties and self.__properties.get(
            self.prepare_property(name, language), None)

    def get_properties(self):
        return self.__properties

    def ensure_properties(self):
        if self.__properties is None:
            self.__properties = {}
        return self.__properties

    @staticmethod
    def prepare_property(name, language):
        return name + "_" + language


class Station(Entity):

    def __init__(self, agent_type, station_id):
        super().__init__()

        self.agent_type = agent_type
        self.station_id = station_id

    @staticmethod
    def get_domain_id(agent_type, station_id):
        return agent_type + station_id

    @property
    def domain_id(self):
        return self.get_domain_id(self.agent_type, self.station_id)

    def set_station_name(self, station_name, language):
        self.set_property("station_name", language, station_name)

    def get_station_name(self, language):
        return self.get_property("station_name", language)

    def set_state_name(self, state_name, language):
        self.set_property("state_name", language, state_name)

    def get_state_name(self, language):
        return self.get_property("state_name", language)

    def set_country_name(self, country_name, language):
        self.set_property("country_name", language, country_name)

    def get_country_name(self, language):
        return self.get_property("country_name", language)


class Route(Entity):

    def __init__(self, agent_type, route_id):
        super().__init__()

        self.agent_type = agent_type
        self.route_id = route_id

        self.route_number = None
        self.route_points = []

        self.active_from_date = None
        self.active_to_date = None

    @staticmethod
    def get_domain_id(agent_type, route_id):
        return agent_type + route_id

    @property
    def domain_id(self):
        return self.get_domain_id(self.agent_type, self.route_id)

    @property
    def departure_point(self):
        return self.get_route_point(0)

    @property
    def arrival_point(self):
        return self.get_route_point(-1)

    @property
    def departure_time(self):
        return self.departure_point.departure_time

    @property
    def arrival_time(self):
        return self.arrival_point.arrival_time

    @property
    def travel_time(self):
        return minutes_to_time(
            self.calculate_travel_time(0, len(self.route_points) - 1)
        )

    def set_periodicity(self, periodicity, language):
        self.set_property("periodicity", language, periodicity)

    def get_periodicity(self, language):
        return self.get_property("periodicity", language)

    def add_route_point(self, route_point):
        self.route_points.append(route_point)

    def get_route_point(self, index):
        try:
            return self.route_points[index]
        except IndexError as e:
            raise AbsentRoutePointException(
                route_id=self.route_id,
                point_index=index
            )

    def calculate_travel_time(self, departure_point_idx, arrival_point_idx):
        minutes = 0
        previous_departure_time = None
        for index in range(departure_point_idx, arrival_point_idx):
            point = self.get_route_point(index)
            if previous_departure_time is not None:
                segment_time = calculate_raw_time_difference(
                    previous_departure_time,
                    point.arrival_time
                )
                minutes += segment_time + point.raw_stop_time
            previous_departure_time = point.departure_time
        if previous_departure_time:
            minutes += calculate_raw_time_difference(
                previous_departure_time,
                self.get_route_point(arrival_point_idx).arrival_time
            )
        return minutes


class RoutePoint(Entity):

    def __init__(self, agent_type, route_id, station_id):
        super().__init__()

        self.agent_type = agent_type
        self.route_id = route_id
        self.station_id = station_id

        self.arrival_time = None
        self.departure_time = None

    @staticmethod
    def get_domain_id(agent_type, route_id, station_id):
        return agent_type + route_id + '.' + station_id

    @property
    def domain_id(self):
        return self.get_domain_id(self.agent_type, self.route_id, self.station_id)

    @property
    def stop_time(self):
        if self.arrival_time and self.departure_time:
            return calculate_time_difference(
                self.arrival_time,
                self.departure_time
            )
        else:
            return ''

    @property
    def raw_stop_time(self):
        if self.arrival_time and self.departure_time:
            return calculate_raw_time_difference(
                self.arrival_time,
                self.departure_time
            )
        else:
            return 0


class Path(Entity):

    def __init__(self):
        super().__init__()

        self.path_items = []
        self.__raw_travel_time = 0

    @property
    def departure_station_id(self):
        return self.get_path_item(0).departure_point.station_id

    @property
    def arrival_station_id(self):
        return self.get_path_item(-1).arrival_point.station_id

    @property
    def departure_time(self):
        return self.get_path_item(0).departure_time

    @property
    def arrival_time(self):
        return self.get_path_item(-1).arrival_time

    @property
    def travel_time(self):
        return minutes_to_time(self.raw_travel_time)

    @property
    def raw_travel_time(self):
        return self.__raw_travel_time

    def __calculate_travel_time(self):
        minutes = 0
        previous_path_item = None
        for path_item in self.path_items:
            if previous_path_item is not None:
                minutes += calculate_raw_time_difference(
                    previous_path_item.arrival_time,
                    path_item.departure_time
                )
            minutes += path_item.raw_travel_time
            previous_path_item = path_item
        return minutes

    def add_path_item(self, path_item):
        if self.path_items and \
           self.path_items[-1].route.domain_id == path_item.route.domain_id:
            self.path_items[-1].arrival_point_idx = path_item.arrival_point_idx
        else:
            self.path_items.append(path_item)
        self.__raw_travel_time = self.__calculate_travel_time()

    def get_path_item(self, index):
        try:
            return self.path_items[index]
        except IndexError as e:
            raise AbsentPathItemException(
                item_index=index
            )


class PathItem(Entity):

    def __init__(self, route, departure_point_idx, arrival_point_idx):
        super().__init__()

        self.route = route
        self.departure_point_idx = departure_point_idx
        self.arrival_point_idx = arrival_point_idx

        self.__raw_travel_time = self.route.calculate_travel_time(
            self.departure_point_idx,
            self.arrival_point_idx
        )

    @property
    def departure_point(self):
        return self.route.get_route_point(self.departure_point_idx)

    @property
    def arrival_point(self):
        return self.route.get_route_point(self.arrival_point_idx)

    @property
    def departure_time(self):
        return self.departure_point.departure_time

    @property
    def arrival_time(self):
        return self.arrival_point.arrival_time

    @property
    def travel_time(self):
        return minutes_to_time(self.raw_travel_time)

    @property
    def raw_travel_time(self):
        return self.__raw_travel_time
