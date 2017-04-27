import argparse

from routes_aggregator.service import Service
from routes_aggregator.model import *
from routes_aggregator.utils import *

def travel_time_test():
    #r = Route('test', '1')
    #.add_route_point(RoutePoint('test', r.domain_id, '2'))
    print(calculate_raw_time_difference('11:10', '11:11'))
    print(calculate_time_difference('00:04', '23:54'))


def service_test():

    parser = argparse.ArgumentParser(description='Routes Aggregator API')
    parser.add_argument('config_path', help='Path to configuration file')
    args = parser.parse_args()

    Service(config_path=args.config_path)

    #Service().request_model_update('uz', False)
    #Service().request_model_update('uzs', False)
    r = Service().find_paths(['uz22100'], ['uz24110'], transfers_count=1, limit=10)
    print(r)

travel_time_test()
service_test()
