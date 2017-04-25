from neo4j.v1 import GraphDatabase, basic_auth, CypherError, DatabaseError
from model import Entity, Station, Route, Segment


class DbAccessor:

    CREATE_NODE = r"CREATE (n:{label}  {{ {properties} }} ) RETURN n"
    CREATE_ROUTE_CONNECTION = \
        "MATCH (a:Route {{ domain_id: '{route_domain_id}' }}), " \
        "      (b:Station {{ domain_id: '{station_domain_id}' }}) " \
        "CREATE (a)-[:ROUTE_CONNECTION {{ {properties} }} ]->(b)"
    CREATE_TRANSITION = \
        "MATCH (a:Station {{ domain_id: '{from_domain_id}' }}), " \
        "      (b:Station {{ domain_id: '{to_domain_id}' }}) " \
        "CREATE (a)-[:TRANSITION {{ {properties} }} ]->(b)"

    CREATE_INDEX = "CREATE INDEX ON :{label}({property})"
    DELETE_RELATIONSHIP = "MATCH ()-[r { agent_type: $agent_type }]->() DELETE r"
    DELETE_NODE = "MATCH (n { agent_type: $agent_type }) DELETE n"

    MATCH_STATION_BY_DOMAIN_ID = "MATCH (n:Station) WHERE n.domain_id = $domain_id RETURN n"

    MATCH_BY_PARAMETER_STARTS_WITH = "MATCH (n:{label}) WHERE LOWER(n.{property_name}) " \
                                     "STARTS WITH LOWER($property_value) RETURN n LIMIT $limit"
    MATCH_BY_PARAMETER_STRICT = "MATCH (n:{label}) WHERE LOWER(n.{property_name}) " \
                                "= LOWER($property_value) RETURN n LIMIT $limit"
    MATCH_BY_PARAMETER_REGEX = "MATCH (n:{label}) WHERE n.{property_name} " \
                               "=~ $property_value RETURN n LIMIT $limit"

    MATCH_ROUTE_BY_DOMAIN_ID = "MATCH (n:Route) WHERE n.domain_id = $domain_id RETURN n"
    MATCH_TRANSITIONS_BY_ROUTE_ID = "MATCH (s1:Station)" \
                                    "-[r:TRANSITION { route_id: $route_id }]->" \
                                    "(s2: Station) RETURN DISTINCT " \
                                    "s1.station_id as departure_station_id, " \
                                    "r, s2.station_id as arrival_station_id " \
                                    "ORDER BY toInteger(r.segment_id)"

    MATCH_DIRECT_ROUTES = "MATCH (s1:Station)<-[r1:ROUTE_CONNECTION]-" \
                          "(route:Route)-[r2:ROUTE_CONNECTION]->(s2:Station) " \
                          "WHERE s1.domain_id in [{from_domain_ids}] and s2.domain_id in [{from_domain_ids}] " \
                          "and toInt(r1.segment_id) < toInt(r2.segment_id) " \
                          "return distinct route, r1.segment_id, r2.segment_id limit $limit"

    SEARCH_QUERY_MAP = {
        "STARTS_WITH": MATCH_BY_PARAMETER_STARTS_WITH,
        "STRICT": MATCH_BY_PARAMETER_STRICT,
        "REGEX": MATCH_BY_PARAMETER_REGEX
    }

    def __init__(self, credentials):
        self.driver = GraphDatabase.driver(
            'bolt://localhost',
            auth=basic_auth(credentials[0], credentials[1]))

        self.create_indices()

    @staticmethod
    def prepare_property(value):
        return value if not value is None else ''

    @staticmethod
    def prepare_properties(properties):
        prepared_properties = ('{key}: {value}'.format(
            key=item[0], value=repr(DbAccessor.prepare_property(item[1])))
            for item in properties.items())
        return ', '.join(prepared_properties)

    @staticmethod
    def set_properties(entity, properties):
        for item in properties.items():
            if not hasattr(entity, item[0]):
                entity.ensure_properties()[item[0]] = item[1]
            elif not isinstance(getattr(type(entity), item[0], None), property):
                setattr(entity, item[0], item[1])

    def execute(self, executor, default_value=None):
        result = default_value
        try:
            with self.driver.session() as session:
                with session.begin_transaction() as transaction:
                    result = executor(transaction)
        except (CypherError, DatabaseError) as e:
            pass
        except Exception as e:
            pass
        return result

    def create_indices(self):
        def indices_creator(transaction):
            indices = [
                ('Route', 'domain_id'),
                ('Route', 'route_number'),
                ('Station', 'domain_id'),
                ('Station', 'station_name_ua'),
                ('Station', 'station_name_ru'),
                ('Station', 'station_name_en'),
            ]
            for index in indices:
                transaction.run(self.CREATE_INDEX.format(label=index[0], property=index[1]))
        self.execute(indices_creator)

    def create_station(self, station, transaction):
        properties = {
            'domain_id': station.domain_id,
            'agent_type': station.agent_type,
            'station_id': station.station_id}

        properties.update(station.get_properties())
        station_query = self.CREATE_NODE.format(
            label='Station',
            properties=self.prepare_properties(properties))
        transaction.run(station_query)

    def create_route(self, route, transaction):
        properties = {
            'domain_id': route.domain_id,
            'agent_type': route.agent_type,
            'route_id': route.route_id,
            'route_number': route.route_number,
            'active_to_date': self.prepare_property(route.active_to_date),
            'active_from_date': self.prepare_property(route.active_from_date)}

        properties.update(route.get_properties())
        route_query = self.CREATE_NODE.format(
            label='Route',
            properties=self.prepare_properties(properties))
        transaction.run(route_query)

        built_route_connections = set()
        station_number = 0

        def create_route_connection(station_id):
            properties = {
                'agent_type': route.agent_type,
                'station_number': station_number}
            route_connection_query = self.CREATE_ROUTE_CONNECTION.format(
                station_domain_id=Station.get_domain_id(route.agent_type, station_id),
                route_domain_id=route.domain_id,
                properties=self.prepare_properties(properties))
            transaction.run(route_connection_query)

        for segment in route.segments:
            if not segment.departure_station_id in built_route_connections:
                create_route_connection(segment.departure_station_id)
                built_route_connections.add(segment.departure_station_id)
                station_number += 1
            if not segment.arrival_station_id in built_route_connections:
                create_route_connection(segment.arrival_station_id)
                built_route_connections.add(segment.arrival_station_id)
                station_number += 1

            self.create_segment(segment, transaction)

    def create_segment(self, segment, transaction):
        properties = {
            'domain_id': segment.domain_id,
            'agent_type': segment.agent_type,
            'route_id': segment.route_id,
            'segment_id': segment.segment_id,
            'departure_time': segment.departure_time,
            'arrival_time': segment.arrival_time}

        transition_query = self.CREATE_TRANSITION.format(
            from_domain_id=Station.get_domain_id(segment.agent_type, segment.departure_station_id),
            to_domain_id=Station.get_domain_id(segment.agent_type, segment.arrival_station_id),
            properties=self.prepare_properties(properties))
        transaction.run(transition_query)

    def extract_route(self, data_item, transaction):
        properties = data_item['n'].properties
        route = Route(properties['agent_type'], properties['route_id'])
        self.set_properties(route, properties)

        result = transaction.run(
            self.MATCH_TRANSITIONS_BY_ROUTE_ID,
            {'route_id': route.route_id})

        data = result.data()
        if data:
            for data_item in data:
                properties = data_item['r'].properties
                segment = Segment(properties['agent_type'],
                                  properties['route_id'],
                                  properties['segment_id'])
                self.set_properties(segment, properties)
                segment.departure_station_id = data_item['departure_station_id']
                segment.arrival_station_id = data_item['arrival_station_id']
                route.add_segment(segment)
        return route

    def extract_station(self, data_item):
        properties = data_item['n'].properties
        station = Station(properties['agent_type'], properties['station_id'])
        self.set_properties(station, properties)
        return station

    def get_station(self, domain_id):
        def station_getter(transaction):
            result = transaction.run(
                self.MATCH_STATION_BY_DOMAIN_ID,
                {'domain_id': domain_id})
            if result:
                data = result.data()
                return self.extract_station(data[0]) if data else None
            return None

        return self.execute(station_getter)

    def get_route(self, domain_id):
        def route_getter(transaction):
            result = transaction.run(
                self.MATCH_ROUTE_BY_DOMAIN_ID,
                {'domain_id': domain_id})
            if result:
                data = result.data()
                return self.extract_route(data[0], transaction) if data else None
            return None

        return self.execute(route_getter)

    def find_stations(self, station_name, language, search_mode, limit):
        def stations_getter(transaction):
            stations = []

            query_template = self.SEARCH_QUERY_MAP.get(search_mode.upper())
            if query_template:
                result = transaction.run(
                    query_template.format(
                        label='Station',
                        property_name=Entity.prepare_property('station_name', language)),
                    {'property_value': station_name, 'limit': limit})
                data = result.data()
                if data:
                    stations.extend(map(lambda data_item: self.extract_station(data_item), data))
            return stations

        return self.execute(stations_getter, [])

    def find_routes(self, route_number, search_mode, limit):
        def routes_getter(transaction):
            routes = []

            query_template = self.SEARCH_QUERY_MAP.get(search_mode.upper())
            if query_template:
                result = transaction.run(
                    query_template.format(
                        label='Route',
                        property_name='route_number'),
                    {'property_value': route_number, 'limit': limit})
                data = result.data()
                if data:
                    routes.extend(
                        map(lambda data_item: self.extract_route(data_item, transaction), data)
                    )
            return routes

        return self.execute(routes_getter, [])

    def build_model(self, model):
        def model_builder(transaction):
            self.remove_model(model.agent_type, transaction)
            for station in model.stations.values():
                self.create_station(station, transaction)
            for route in model.routes.values():
                self.create_route(route, transaction)

        self.execute(model_builder)

    def remove_model(self, agent_type, transaction):
        transaction.run(self.DELETE_RELATIONSHIP, {'agent_type': agent_type})
        transaction.run(self.DELETE_NODE, {'agent_type': agent_type})
