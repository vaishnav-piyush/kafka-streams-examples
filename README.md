
## CONSUME KEY VALUE
$CONFLUENT_HOME/bin/kafka-console-consumer --bootstrap-server localhost:9092 --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --topic favourite-color-output --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
rkvkt user-purchase-enriched-left-join --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer


$CONFLUENT_HOME/bin/kafka-console-consumer --bootstrap-server localhost:9092 --from-beginning --topic favourite-color-output

## List Topics
$CONFLUENT_HOME/bin/kafka-topics --bootstrap-server localhost:9092 --list

## CREATE TOPIC
$CONFLUENT_HOME/bin/kafka-topics --bootstrap-server localhost:9092 --create --topic favourite-color-output

## CREATE LOG COMPACTED TOPIC
$CONFLUENT_HOME/bin/kafka-topics --bootstrap-server localhost:9092 --create --partitions 1 --replication-factor 1 --config cleanup.policy=compact --config min.cleanable.dirty.ratio=.005 --config segment.ms=10000 --topic favourite-color-by-user
mklckt bank-balance 

## DESCRIBE TOPIC
$CONFLUENT_HOME/bin/kafka-topics --bootstrap-server localhost:9092 --describe --topic favourite-color-by-user
dsckt favourite-color-by-user 

## DELETE TOPIC
$CONFLUENT_HOME/bin/kafka-topics --bootstrap-server localhost:9092 --delete --topic favourite-color-output 

## PRODUCE WTH KEY VALUE
$CONFLUENT_HOME/bin/kafka-console-producer --broker-list localhost:9092 --property "parse.key=true" --property "key.separator=," --topic favourite-color-input 
wkvkt favourite-color-input

## PRODUCE 
$CONFLUENT_HOME/bin/kafka-console-producer --broker-list localhost:9092 --topic favourite-color-input 