api_key = 'YCFU7JWLI2RAWJ5B'

api_secret='NqfwYiPHOlJii5KC7D5DY1chToIgJN032tw50WeHMC86s0HNStNVpb2voik0W9J7'

Bootstrap_server='pkc-921jm.us-east-2.aws.confluent.cloud:9092'
kafka_topic = "topic_m"


kafka_server_config = {
    'bootstrap.servers': Bootstrap_server,
    # 'group.id': 'group_id',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': api_key,
    'sasl.password': api_secret,
    'session.timeout.ms': '45000'
}
