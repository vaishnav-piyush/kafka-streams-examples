package com.piyush.kafka.streams;

import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class UserPurchasesProducer {

    private static final String USER_TOPIC = "user-data";
    private static final String PURCHASE_TOPIC = "user-purchase";
    private final Producer<String, String> userDataProducer;
    private final Producer<String, String> purchaseProducer;

    public UserPurchasesProducer() {
        Properties config1 = new Properties();
        config1.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config1.put(ProducerConfig.ACKS_CONFIG, "all");
        config1.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config1.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config1.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        config1.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        config1.put(ProducerConfig.CLIENT_ID_CONFIG, "user-purchase-producer");
        config1.put(ProducerConfig.RETRIES_CONFIG, "3");

        userDataProducer = new KafkaProducer<String, String>(config1);
        purchaseProducer = new KafkaProducer<String, String>(config1);
    }

    @SneakyThrows
    private void produceUserData(String fname, String lname) {
        final ProducerRecord<String, String> user = new ProducerRecord<>(USER_TOPIC, fname, lname);
        userDataProducer.send(user).get();
    }

    @SneakyThrows
    private void producePurchaseData(String fname, String item) {
        final ProducerRecord<String, String> purchase = new ProducerRecord<>(PURCHASE_TOPIC, fname, item);
        purchaseProducer.send(purchase).get();
    }


    public static void main(String[] args) throws InterruptedException {
        UserPurchasesProducer producer = new UserPurchasesProducer();

        // 1
        producer.produceUserData("piyush", "vaishnav");
        producer.producePurchaseData("piyush", "kafka-streams course");

        // 2
        producer.producePurchaseData("swathi", "ux course");

        // 3
        producer.produceUserData("piyush", "pandu");
        producer.producePurchaseData("piyush", "kafka schema registry course");

        // 4
        producer.producePurchaseData("vihaan", "milk bottle");
        producer.produceUserData("vihaan", "vaishnav");
        producer.produceUserData("vihaan", null);
        producer.producePurchaseData("vihaan", "toys");

        // 5
        producer.produceUserData("nidhi", "jain");
        producer.produceUserData("nidhi", null);
        producer.producePurchaseData("nidhi", "jewellery");

        Thread.sleep(1000);

    }

}
