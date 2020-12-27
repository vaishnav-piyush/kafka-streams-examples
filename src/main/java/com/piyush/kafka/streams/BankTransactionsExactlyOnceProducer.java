package com.piyush.kafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class BankTransactionsExactlyOnceProducer {

    private static final String BANK_ACCOUNT_TRANSACTIONS_TOPIC = "bank-account-transactions";
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private Producer<String, String> producer;

    public BankTransactionsExactlyOnceProducer() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "bank-account-transactions-dummy-producer");
        config.put(ProducerConfig.RETRIES_CONFIG, "3");

        producer = new KafkaProducer<String, String>(config);
    }

    @SneakyThrows
    public void produce(final String user, final float balance) {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode transaction = objectMapper.createObjectNode();
        transaction.put("user", user);
        transaction.put("balance", balance);
        transaction.put("time", dateTimeFormatter.format(LocalDateTime.now()));

        final String stringValue = objectMapper.writeValueAsString(transaction);
        final ProducerRecord<String, String> record =
                new ProducerRecord<>(BANK_ACCOUNT_TRANSACTIONS_TOPIC, user, stringValue);
        producer.send(record,
                (recordMetadata, e) -> {
                    if(e != null) {
                        e.printStackTrace();
                    } else {
                            System.out.println("key/value " + user + "/" + stringValue +
                                    "\twritten to topic[partition] " + recordMetadata.topic() + "[" + recordMetadata.partition() + "] at offset " +
                                    recordMetadata.offset());
                    }
                } );
    }

    @SneakyThrows
    public void produceTestData() {
        final List<String> users = Arrays.asList("piyush", "swathi", "vihaan");
        Random random = new Random();
        int count = 100;
        while(count-- > 0) {
            produce(users.get(random.nextInt(3)), random.nextInt(100000));
        }
    }

    public void shutDown() {
        producer.close();
    }

    public static void main(String[] args) {
        BankTransactionsExactlyOnceProducer producerApp = new BankTransactionsExactlyOnceProducer();
        producerApp.produceTestData();

        Runtime.getRuntime().addShutdownHook(new Thread(producerApp::shutDown));
        producerApp.shutDown();
    }

}
