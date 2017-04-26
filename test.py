from routes_aggregator.service import Service

Service(
    db_user='neo4j', db_password='1234',
)
#Service().request_model_update('uz', False)
r = Service().find_paths(['uzs2528', 'uz22100'], ['uzs1883', 'uz24110'], transfers_count=1, limit=10)
print(r)
