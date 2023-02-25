# Guia

```
- run docker-compose up -d - UP Kafka server
- run kafka_producer.py - There is a data faker producer in python and submit in a Kafka Topic
- run kafka_consumer.py - There is a consumer from Kafka Topic

- Show created "user-tracker" Topic
docker exec -t kafka_broker kafka-topics --bootstrap-server localhost:9092 --list

- Show Topic "user-tracker" data from beginning in docker
docker exec -it kafka_broker kafka-console-consumer --bootstrap-server localhost:9092 --topic user-tracker --from-beginning


- Teste Create Topic
docker exec -it kafka_broker kafka-topics --bootstrap-server localhost:9092 --topic first_topic --create

- Teste Producer in Topic
docker exec -it kafka_broker kafka-console-producer --bootstrap-server localhost:9092 --topic first_topic

- Teste Read from Topic
docker exec -it kafka_broker kafka-console-consumer --bootstrap-server localhost:9092 --topic first_topic --from-beginning

Finish Containers
- run docker-compose down
```