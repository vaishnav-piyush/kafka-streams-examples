package com.piyush.kafka.streams;

import com.piyush.kafka.EmbeddedSingleNodeKafkaCluster;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.test.TestUtils;
import org.awaitility.Awaitility;
import org.junit.*;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class UserPurchaseJoinerIntegrationTest {

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    private KafkaProducer<String, String> userDataProducer = createUserProducer();
    private KafkaProducer<String, String> userPurchaseProducer = createPurchaseProducer();
    private KafkaConsumer<String, String> innerJoinConsumer = createInnerJoinConsumer();
    private KafkaConsumer<String, String> leftJoinConsumer = createLeftJoinConsumer();
    private UserPurchaseJoiner userPurchaseJoiner;

    private static KafkaProducer<String, String> createUserProducer() {
        Properties props = defaultProducerProperties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "user-data-producer");

        return new KafkaProducer<>(props);
    }

    private static KafkaProducer<String, String> createPurchaseProducer() {

        Properties props = defaultProducerProperties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "user-purchase-producer");

        return new KafkaProducer<>(props);
    }

    private static Properties defaultProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
//        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        return props;
    }

    private static Properties defaultConsumerProperties() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return props;
    }

    private static KafkaConsumer<String, String> createInnerJoinConsumer() {
        Properties props = defaultConsumerProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "inner-join-consumer");

        return new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
    }

    private static KafkaConsumer<String, String> createLeftJoinConsumer() {
        Properties props = defaultConsumerProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "left-join-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
    }

    @BeforeClass
    @SneakyThrows
    public static void startKafkaClusterAndCreateTopics() {
//        CLUSTER.start();
        CLUSTER.createTopic(UserPurchaseJoiner.USER_TOPIC);
        CLUSTER.createTopic(UserPurchaseJoiner.PURCHASE_TOPIC);
        CLUSTER.createTopic(UserPurchaseJoiner.PURCHASE_ENRICH_INNER_JOIN);
        CLUSTER.createTopic(UserPurchaseJoiner.PURCHASE_ENRICH_LEFT_JOIN);
    }

    @AfterClass
    public static void stopKafkaCluster() {
        CLUSTER.stop();
    }

    @Before
    public void createStreams() {
        userPurchaseJoiner = new UserPurchaseJoiner(CLUSTER.bootstrapServers());
        userPurchaseJoiner.getStreams().cleanUp();
        userPurchaseJoiner.getStreams().start();
    }

    @After
    public void stopStreams() {
        userPurchaseJoiner.getStreams().close();
        innerJoinConsumer.close();
        leftJoinConsumer.close();
    }

    @Test
    public void innerJoinShouldHaveRecord_WhenUserDataAvailable() {
        userDataProducer.send(new ProducerRecord<>(UserPurchaseJoiner.USER_TOPIC, "piyush", "vaishnav"));
        userDataProducer.flush();
        userPurchaseProducer.send(new ProducerRecord<>(UserPurchaseJoiner.PURCHASE_TOPIC, "piyush", "kafka-streams course"));
        userPurchaseProducer.flush();
        innerJoinConsumer.subscribe(Collections.singleton(UserPurchaseJoiner.PURCHASE_ENRICH_INNER_JOIN));
        Map<String, String> actuals = new ConcurrentHashMap<>();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(
                () -> {
                    ConsumerRecords<String, String> records = innerJoinConsumer.poll(Duration.ofSeconds(1));
                    records.forEach(record -> actuals.put(record.key(), record.value()));
                    return !records.isEmpty();
                }
        );

        assert("Item: kafka-streams course, Lname: vaishnav".equals(actuals.get("piyush")));


    }
}
