This repo is created for the basic demo of kafka producer and consumer in java. Can be considered as reference if required.

This is a java application based on Spring framework.

Files
1. Kafka Constants Interface
2. Main Application Class
3. Controllers
  1. Kafka Rest Controller
    - GET: /api/v1/message-producer?message=This is message
    - GET: /api/v1/message-consumer
  2. URL Check controller
    This is a REST controller created for site status, whether the given site url is up or down.
    GET: /check?url=https://www.google.com
4. Services
  1. KafkaConsumerCreator
  2. KafkaProducerCreator

