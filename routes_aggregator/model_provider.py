import os
import time

import boto3
import requests
from botocore.client import Config
from lxml import html

from routes_aggregator.model import ModelAccessor, Station, Route, RoutePoint


class BaseAgent:

    def __init__(self, agent_type, logger):
        self.session = requests.session()

        self.agent_type = agent_type
        self.logger = logger

    @staticmethod
    def prepare_time(time):
        if not time or len(time) == 1:
            return ''
        else:
            return time


class UZSubtrainAgent(BaseAgent):

    def __init__(self, agent_type, logger):
        super().__init__(agent_type, logger)

        self.language_map = {"ua": "", "ru": "_ru", "en": "_en"}

    def build_model(self, model):
        model.agent_type = self.agent_type
        self.build_stations(model)
        self.build_routes(model)

    def build_stations(self, model):

        station_element_xpath = "/html/body/table/tr[2]/td/table/tr[3]/td[4]/" \
                                "table/tr/td/table/tr[2]/td/center/li/table[2]/tr/td/ul/li/a"
        station_table_row_xpath = "/html/body/table/tr[2]/td/table/tr[3]/td[4]/table/tr/td/" \
                                  "table/tr[2]/td/center/table/tr[@class=\'on\' or @class=\'onx\']"

        station_schedule_url = 'http://swrailway.gov.ua/timetable/eltrain/?geo2_list=1&lng={language}'
        station_table_url = "http://swrailway.gov.ua/timetable/eltrain/?sid={station_id}&lng={language}"

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

        self.logger.info('Station building session - {} stations to build'.format(len(model.stations.values())))
        for i, station in enumerate(model.stations.values()):

            station_id = station.station_id
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

    def build_routes(self, model):
        route_table_row_xpath = "/html/body/table/tr[2]/td/table/tr[3]/td[4]/table/tr/td/" \
                                  "table/tr[2]/td/center/table/tr/td/table/tr[@class=\'on\' or @class=\'onx\']"
        route_table_url = "http://swrailway.gov.ua/timetable/eltrain/?tid={route_id}"

        self.logger.info('Routes building session - {} routes to build'.format(len(model.routes.values())))
        for i, route in enumerate(model.routes.values()):
            response = self.session.get(route_table_url.format(route_id=route.route_id))
            time.sleep(0.1)

            if response.ok:
                tree = html.fromstring(response.text)
                route_point_rows = tree.xpath(route_table_row_xpath)
                if len(route_point_rows) > 2:

                    for route_point_row in route_point_rows[2:]:
                        children = route_point_row.getchildren()
                        if len(children) > 3:
                            links = route_point_row.xpath('./td/a[@class=\'et\']')
                            href = links[0].get('href') if len(links) else ''
                            if href and href.startswith('.?sid'):
                                station_id = href[6:href.find('&')]

                                route_point = RoutePoint(self.agent_type, route.route_id, station_id)
                                route_point.arrival_time = self.prepare_time(children[2].text)
                                route_point.departure_time = self.prepare_time(children[3].text)
                                route.add_route_point(route_point)


class UZAgent(BaseAgent):

    def __init__(self, agent_type, logger):
        super().__init__(agent_type, logger)

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
        route_point_xpath = '//*[@id="cpn-timetable"]/table[2]/tbody/tr'

        station_name_offset_map = {"ua": 19, "en": 25}
        stations_to_build = set()
        routes_to_build = set()

        stations_to_build.add('22000')

        while stations_to_build or routes_to_build:

            self.logger.info('Station building session - {} stations to build'.format(len(stations_to_build)))
            for i, station_id in enumerate(stations_to_build):

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

            self.logger.info('Routes building session - {} routes to build'.format(len(routes_to_build)))
            for i, route_id in enumerate(routes_to_build):

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

                        if len(route.route_points):
                            continue

                        for route_point_row in tree.xpath(route_point_xpath):
                            children = route_point_row.getchildren()
                            if len(children) > 2:
                                links = route_point_row.xpath('./td/a')
                                href = links[0].get('href') if len(links) else ''
                                if href and href.startswith('?station'):
                                    station_id = href[9:href.find('&')]

                                    route_point = RoutePoint(self.agent_type, route.route_id, station_id)
                                    route_point.arrival_time = self.prepare_time(children[1].text)
                                    route_point.departure_time = self.prepare_time(children[2].text)
                                    route.add_route_point(route_point)

                                    station = model.find_station(station_id)
                                    if station is None:
                                        stations_to_build.add(station_id)

            routes_to_build.clear()


class ModelProvider:

    def __init__(self, storage_adapter, logger):
        self.agent_types = {'uz': UZAgent, 'uzs': UZSubtrainAgent}
        self.storage_adapter = storage_adapter
        self.logger = logger

    def build_model(self, agent_type):
        model = ModelAccessor()

        model_builder = self.agent_types.get(agent_type)
        if model_builder:
            model_builder(agent_type, self.logger).build_model(model)

        #self.save_model(model, time.strftime("archive/%d.%m.%Y %H:%M"))
        self.save_model(model, "current")
        return model

    def save_model(self, model, object_name):
        self.storage_adapter.save_model(model, object_name)

    def load_model(self, agent_type, object_name):
        return self.storage_adapter.load_model(agent_type, object_name)
