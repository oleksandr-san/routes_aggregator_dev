from routes_aggregator.db_accessor import DbAccessor
from routes_aggregator.model_provider import ModelProvider

credentials = ('AKIAIVFHGEK5NRLALIWQ', 'k89QjR2GookyZO2An59CrIURiGpkxg/ErwYVAgQy')
provider = ModelProvider(credentials)
model = provider.build_model('uzs')

#model = provider.load_model('uz', 'current')

#model = ModelAccessor()
#with open('uzs_last.data', 'rb') as fileobj:
#    model.restore_binary(fileobj)

#provider.save_model(model, 'current')
#m = provider.load_model('uz', 'current')

#model = provider.build_model('uz')
#with open('uz_last.data', 'wb') as fileobj:
#   model.save_binary(fileobj)
# model = provider.build_model('uzs')


db = DbAccessor(('neo4j', '1234'))
db.build_model(model)
#s = db.get_station("uz22000")
#s = db.find_stations("Крам", "ua", "STARTS_WITH", 10)
#r = db.get_route('uz55101')
#r = db.find_routes_by_route_number('369', 'strict', 10)
#r = db.find_routes_by_station_ids(['uz24110', 'uz22100'], 1000)
#r = db.find_direct_routes(['uzs2528', 'uz22100'], ['uzs1883', 'uz24110'], 10)

#print(r)