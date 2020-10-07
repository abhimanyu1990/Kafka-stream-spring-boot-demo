# Kafka-stream-spring-boot-demo
This is demo project to showcase implementation of Kafka stream using Spring boot. This is basic implementation to show how we can use KafkaStream processing to process the IoT data before it will be consumed by consumer. In this case, I am trying to classify tempreature into HIGH, MODERATE, LOW, EXTREMELY_LOW before it will be consume by consumer.



# POC Detail

1. Implemented Producer API  and Consumer API
2. Implemented KafkaStream and Read the data  { city name and tempreature } from one topic ( test )
3. KafkaStream will modify the data with additional parameter -"Tempreature Class = {  HIGH, MODERATE, LOW, EXTREMELY_LOW }"
 Tempreature above 35 degree celcius is high
 Tempreature between 25 and 35 degree celcius is moderate
 Teampreature between 15 and 25 is normal
 Tempreature between 5-15 is low 
 Tempreature below 5 is extremly low
 4. KafkaStream push the modified data to second topic "consume_test"  which will be consume by consumer

