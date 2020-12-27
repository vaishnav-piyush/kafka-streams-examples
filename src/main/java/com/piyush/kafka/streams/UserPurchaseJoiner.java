package com.piyush.kafka.streams;

import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

@Data
public class UserPurchaseJoiner {

    public static final String USER_TOPIC = "user-data";
    public static final String PURCHASE_TOPIC = "user-purchase";
    public static final String PURCHASE_ENRICH_INNER_JOIN = "user-purchase-enriched-inner-join";
    public static final String PURCHASE_ENRICH_LEFT_JOIN = "user-purchase-enriched-left-join";
    private static final String APPLICATION_ID = "user-purchase";

    private final KafkaStreams streams;

    public UserPurchaseJoiner(String connectionString) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, connectionString);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5);
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        GlobalKTable<String, String> userData = builder.globalTable(USER_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> purchaseStream = builder.stream(PURCHASE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        purchaseStream.join(userData,
                (fname, item) -> fname,
                (item, lname) -> String.format("Item: %s, Lname: %s", item, lname))
                .to(PURCHASE_ENRICH_INNER_JOIN, Produced.with(Serdes.String(), Serdes.String()));

        purchaseStream.leftJoin(userData,
                (fname, item) -> fname,
                (item, lname) -> String.format("Item: %s, Lname: %s", item, lname))
                .to(PURCHASE_ENRICH_LEFT_JOIN, Produced.with(Serdes.String(), Serdes.String()));

        streams = new KafkaStreams(builder.build(), config);

    }

    public static void main(String[] args) {
        UserPurchaseJoiner userPurchaseJoiner = new UserPurchaseJoiner("localhost:9092");
        userPurchaseJoiner.streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(userPurchaseJoiner.streams::close));
    }
}
