from model import ModelAccessor, Station, Route, Segment
from db_accessor import DbAccessor
from model_provider import ModelProvider

import time

credentials = ('AKIAIVFHGEK5NRLALIWQ', 'k89QjR2GookyZO2An59CrIURiGpkxg/ErwYVAgQy')
provider = ModelProvider(credentials)
#provider.build_model('uzs')

#model = provider.load_model('uzs', 'current')

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
#db.build_model(model)
#s = db.get_station("uz22000")
#s = db.find_stations("Крам", "ua", "STARTS_WITH", 10)
#r = db.get_route('uz55101')
#r = db.find_routes('369', 'strict', 10)

print(r)