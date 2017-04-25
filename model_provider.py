import boto3
import os
import requests
import time
from lxml import html
from model import ModelAccessor, Station, Route, Segment
from botocore.client import Config


class UZSubtrainAgent:

    def __init__(self):
        self.session = requests.session()
        self.agent_type = "uzs"
        self.language_map = {"ua": "", "ru": "_ru", "en": "_en"}

    def build_model(self, model):
        model.agent_type = self.agent_type
        self.build_stations(model)
        self.build_routes(model)
        self.build_segments(model)

    def build_stations(self, model):

        station_schedule_url = 'http://swrailway.gov.ua/timetable/eltrain/?geo2_list=1&lng={language}'
        station_element_xpath = '/html/body/table/tr[2]/td/table/tr[3]/td[4]/' \
                                'table/tr/td/table/tr[2]/td/center/li/table[2]/tr/td/ul/li/a';

        for item in self.language_map.items():

            language = item[0]
            response = self.session.get(station_schedule_url.format(language=item[1]))

            if response.ok:
                tree = html.fromstring(response.text)
                for element in tree.xpath(station_element_xpath):
                    href = element.get('href')
                    if href and href.startswith('?sid'):
                        station_id = href[5:href.find('&')]
                        station_name = ''
                        for text in element.itertext():
                            station_name = text
                            break

                        station = model.find_station(station_id)
                        if not station:
                            station = Station(self.agent_type, station_id)
                            model.add_station(station)

                        station.set_station_name(station_name, language)

    def build_routes(self, model):
        station_table_row_xpath = "/html/body/table/tr[2]/td/table/tr[3]/td[4]/table/tr/td/" \
                                  "table/tr[2]/td/center/table/tr[@class=\'on\' or @class=\'onx\']"
        station_table_url = "http://swrailway.gov.ua/timetable/eltrain/?sid={station_id}&lng={language}"

        print('Station building session - {} stations to build'.format(len(model.stations.values())))
        for i, station in enumerate(model.stations.values()):

            station_id = station.station_id
            print('Building station #{} from {}'.format(i + 1, len(model.stations.values())))
            time.sleep(0.1)

            for item in self.language_map.items():

                language = item[0]
                response = self.session.get(station_table_url.format(
                    station_id=station_id, language=item[1]))

                if response.ok:
                    tree = html.fromstring(response.text)
                    for index, element in enumerate(tree.xpath(station_table_row_xpath)):
                        links = element.xpath('./td/a[@class=\'et\']')

                        href = links[0].get('href') if len(links) else ''
                        if href and href.startswith('.?tid'):
                            route_id = href[6:href.find('&')]

                            children = element.getchildren()
                            route = model.find_route(route_id)
                            if not route:
                                route = Route(self.agent_type, route_id)
                                model.add_route(route)
                                if len(children) >= 6:
                                    route.route_number = links[0].text.strip(' /\\')
                                    route.active_from_date = children[5].text
                                    route.active_to_date = children[6].text
                            if len(children) >= 6:
                                route.set_periodicity(children[1].text.strip(' /\\'), language)
                        elif index == 0:
                            station = model.find_station(station_id)
                            if station is not None:
                                location_parameters = links[0].text.strip('()').split('/')
                                if len(location_parameters) > 1:
                                    station.set_state_name(location_parameters[0].strip(), language)
                                    station.set_country_name(location_parameters[1].strip(), language)

    def build_segments(self, model):
        segment_table_row_xpath = "/html/body/table/tr[2]/td/table/tr[3]/td[4]/table/tr/td/" \
                                  "table/tr[2]/td/center/table/tr/td/table/tr[@class=\'on\' or @class=\'onx\']"
        segment_table_url = "http://swrailway.gov.ua/timetable/eltrain/?tid={route_id}"

        print('Routes building session - {} stations to build'.format(len(model.routes.values())))
        for i, route in enumerate(model.routes.values()):
            response = self.session.get(segment_table_url.format(route_id=route.route_id))

            print('Building route #{} from {}'.format(i + 1, len(model.routes.values())))
            time.sleep(0.1)

            if response.ok:
                tree = html.fromstring(response.text)
                segment_rows = tree.xpath(segment_table_row_xpath)
                if len(segment_rows) > 2:

                    last_station_id = None
                    last_departure_time = None

                    for segment_row in segment_rows[2:]:
                        children = segment_row.getchildren()
                        if len(children) > 3:
                            links = segment_row.xpath('./td/a[@class=\'et\']')
                            href = links[0].get('href') if len(links) else ''
                            if href and href.startswith('.?sid'):
                                station_id = href[6:href.find('&')]
                                arrival_time = children[2].text
                                departure_time = children[3].text

                                if last_station_id and last_departure_time:
                                    segment = Segment(self.agent_type, route.route_id, str(len(route.segments)))
                                    segment.departure_station_id = last_station_id
                                    segment.departure_time = last_departure_time
                                    segment.arrival_station_id = station_id
                                    segment.arrival_time = arrival_time

                                    route.add_segment(segment)

                                last_station_id = station_id
                                last_departure_time = departure_time


class UZAgent:

    def __init__(self):
        self.session = requests.session()
        self.agent_type = "uz"
        self.language_map = {"ua": "", "en": "en"}

    def build_model(self, model):
        model.agent_type = self.agent_type
        self.build_stations(model)

    def build_stations(self, model):

        station_schedule_url = 'http://www.uz.gov.ua/{language}/passengers/timetable/' \
                               '?station={station_id}&by_station=1'
        station_name_xpath = '//*[@id="cpn-timetable"]/div[1]/h3'
        route_row_xpath = '//*[@id="cpn-timetable"]/table/tbody/tr/td/a'

        route_page_url = 'http://www.uz.gov.ua/{language}/passengers/timetable/' \
                         '?ntrain={route_id}&by_id=1'
        route_information_xpath = '//*[@id="cpn-timetable"]/table[1]/tbody/tr'
        route_segment_xpath = '//*[@id="cpn-timetable"]/table[2]/tbody/tr'

        station_name_offset_map = {"ua": 19, "en": 25}
        stations_to_build = set()
        routes_to_build = set()

        stations_to_build.add('22000')

        while stations_to_build or routes_to_build:

            print('Station building session - {} stations to build'.format(len(stations_to_build)))
            for i, station_id in enumerate(stations_to_build):

                print('Building station #{} from {}'.format(i+1, len(stations_to_build)))
                time.sleep(0.1)
                for item in self.language_map.items():

                    language = item[0]
                    response = self.session.get(
                        station_schedule_url.format(language=item[1], station_id=station_id)
                    )

                    if response.ok:
                        tree = html.fromstring(response.text)
                        station_name_elements = tree.xpath(station_name_xpath)

                        if len(station_name_elements) == 1:
                            station = model.find_station(station_id)
                            if not station:
                                station = Station(self.agent_type, station_id)
                                model.add_station(station)

                            station_name_text = station_name_elements[0].text[station_name_offset_map[language]:]
                            last_bracket_idx = station_name_text.rfind('(')
                            if last_bracket_idx != -1:
                                station.set_station_name(station_name_text[:last_bracket_idx - 1], language)
                                station.set_country_name(station_name_text[last_bracket_idx:].strip('()'), language)

                            for route_element in tree.xpath(route_row_xpath):
                                href = route_element.get('href')
                                if href and href.startswith('?ntrain='):
                                    route_id = href[8:href.find('&')]

                                    route = model.find_route(route_id)
                                    if not route:
                                        route = Route(self.agent_type, route_id)
                                        model.add_route(route)
                                        routes_to_build.add(route_id)
            stations_to_build.clear()

            print('Routes building session - {} routes to build'.format(len(routes_to_build)))
            for i, route_id in enumerate(routes_to_build):

                print('Building route #{} from {} '.format(i+1, len(routes_to_build)))
                route = model.find_route(route_id)
                if route is None:
                    continue

                time.sleep(0.1)
                for item in self.language_map.items():
                    language = item[0]
                    response = self.session.get(
                        route_page_url.format(language=item[1], route_id=route.route_id)
                    )

                    if response.ok:
                        tree = html.fromstring(response.text)
                        route_info_elements = tree.xpath(route_information_xpath)
                        if len(route_info_elements):
                            children = route_info_elements[0].getchildren()
                            if children:
                                if not route.route_number:
                                    route_number_components = children[1].text.split()
                                    if route_number_components:
                                        route.route_number = route_number_components[0].strip()
                                route.set_periodicity(children[2].text.strip(), language)

                        if len(route.segments):
                            continue

                        last_station_id = None
                        last_departure_time = None

                        for segment_row in tree.xpath(route_segment_xpath):
                            children = segment_row.getchildren()
                            if len(children) > 2:
                                links = segment_row.xpath('./td/a')
                                href = links[0].get('href') if len(links) else ''
                                if href and href.startswith('?station'):
                                    station_id = href[9:href.find('&')]
                                    arrival_time = children[1].text
                                    departure_time = children[2].text

                                    station = model.find_station(station_id)
                                    if station is None:
                                        stations_to_build.add(station_id)

                                    if last_station_id and last_departure_time:
                                        segment = Segment(self.agent_type, route.route_id, str(len(route.segments)))
                                        segment.departure_station_id = last_station_id
                                        segment.departure_time = last_departure_time
                                        segment.arrival_station_id = station_id
                                        segment.arrival_time = arrival_time

                                        route.add_segment(segment)

                                    last_station_id = station_id
                                    last_departure_time = departure_time

            routes_to_build.clear()


class ModelProvider:

    def __init__(self, credentials):
        self.agents = {'uz': UZAgent(), 'uzs': UZSubtrainAgent()}
        self.credentials = credentials

    def build_model(self, agent_type):
        model = ModelAccessor()

        agent = self.agents.get(agent_type)
        if not agent is None:
            agent.build_model(model)

        self.save_model(model, time.strftime("archive/%d.%m.%Y %H:%M"))
        self.save_model(model, "current")
        return model

    def __get_storage_client(self):
        try:
            client = boto3.client(
                's3',
                aws_access_key_id=self.credentials[0],
                aws_secret_access_key=self.credentials[1],
                config=Config(signature_version='s3v4'))
        except Exception as e:
            client = None
        return client

    @staticmethod
    def __prepare_object_name(agent_type, object_name):
        if not object_name.endswith('/'):
            object_name += '/'
        object_name += agent_type + '.data'
        return object_name

    def save_model(self, model, object_name):
        try:
            client = self.__get_storage_client()
            if client:
                with open('temp.data', 'wb') as fileobj:
                    model.save_binary(fileobj)
                with open('temp.data', 'rb') as fileobj:
                    client.upload_fileobj(
                        fileobj,
                        'routes-aggregator',
                        self.__prepare_object_name(model.agent_type, object_name))
                os.remove('temp.data')
        except Exception as e:
            pass

    def load_model(self, agent_type, object_name):
        try:
            model = ModelAccessor()
            client = self.__get_storage_client()
            if client:
                with open('temp.data', 'wb') as fileobj:
                    client.download_fileobj(
                        'routes-aggregator',
                        self.__prepare_object_name(agent_type, object_name),
                        fileobj)
                with open('temp.data', 'rb') as fileobj:
                    model.restore_binary(fileobj)
                os.remove('temp.data')
        except Exception as e:
            model = None
        return model
