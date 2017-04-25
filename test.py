from routes_aggregator.service import Service

r = Service(
    db_user='neo4j', db_password='1234',
    aws_access_key_id='AKIAIVFHGEK5NRLALIWQ',
    aws_secret_access_key='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'
).find_paths(
    ['uzs2528', 'uz22100'],
    ['uzs1883', 'uz24110'],
    transfers_count=1,
    limit=10
)
print(r)
