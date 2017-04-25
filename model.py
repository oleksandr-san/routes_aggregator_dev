import pickle


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
        self.segments = []

        self.route_number = None
        self.active_from_date = None
        self.active_to_date = None

    @staticmethod
    def get_domain_id(agent_type, route_id):
        return agent_type + route_id

    @property
    def domain_id(self):
        return self.get_domain_id(self.agent_type, self.route_id)

    def set_periodicity(self, periodicity, language):
        self.set_property("periodicity", language, periodicity)

    def get_periodicity(self, language):
        return self.get_property("periodicity", language)

    def add_segment(self, segment):
        self.segments.append(segment)


class Segment(Entity):

    def __init__(self, agent_type, route_id, segment_id):
        super().__init__()

        self.agent_type = agent_type
        self.route_id = route_id
        self.segment_id = segment_id

        self.departure_time = None
        self.departure_station_id = None
        self.arrival_time = None
        self.arrival_station_id = None

    @staticmethod
    def get_domain_id(agent_type, route_id, segment_id):
        return agent_type + route_id + '.' + segment_id

    @property
    def domain_id(self):
        return self.get_domain_id(self.agent_type, self.route_id, self.segment_id)

    @staticmethod
    def convert_to_minutes(time):
        result = 0
        try:
            components = time.split(':')
            if len(components):
                result = int(components[0]) * 60 + int(components[1])
        except Exception as e:
            result = 0
        return result

    @property
    def travel_time(self):
        return abs(self.convert_to_minutes(self.arrival_time)-self.convert_to_minutes(self.departure_time))
