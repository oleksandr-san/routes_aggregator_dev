import argparse

from routes_aggregator.service import Service

parser = argparse.ArgumentParser(description='Routes Aggregator API')
parser.add_argument('config_path', help='Path to configuration file')
args = parser.parse_args()

Service(config_path=args.config_path)

Service().request_model_update('uz', False)
Service().request_model_update('uzs', False)
r = Service().find_paths(['uzs2528', 'uz22100'], ['uzs1883', 'uz24110'], transfers_count=1, limit=10)
print(r)
