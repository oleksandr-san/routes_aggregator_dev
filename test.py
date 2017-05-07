import argparse
import itertools

from routes_aggregator.service import Service
from routes_aggregator.model import *
from routes_aggregator.utils import *


def travel_time_test():
    print(calculate_raw_time_difference('11:10', '11:11'))
    print(calculate_time_difference('00:04', '23:54'))


def language_property_test():
    station = Station('test', '123')
    station.set_station_name('Test_UA', 'ua')
    station.set_station_name('Test_EN', 'en')
    print(Entity.extract_property(station.get_station_name, 'ru'))


def service_test():

    parser = argparse.ArgumentParser(description='Routes Aggregator API')
    parser.add_argument('config_path', help='Path to configuration file')
    args = parser.parse_args()

    Service(config_path=args.config_path)

    #Service().request_model_update('uz', False)
    #Service().request_model_update('uzs', False)

    r = Service().find_paths([['uz22100'], ['uz22430'], ['uz24110']], "SIMPLE",
                             max_transitions_count=4, limit=10)

    r = Service().find_stations(['Kra', 'Ode'], 'starts_with', 10)
    r = Service().find_routes(['68', '10'], [], 'starts_with', 10)
    Service().get_station('uz24110')
    Service().get_route('uz55657')
    print(r)

language_property_test()
travel_time_test()
service_test()
