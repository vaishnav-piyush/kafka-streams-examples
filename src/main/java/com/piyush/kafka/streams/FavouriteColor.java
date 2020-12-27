package com.piyush.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import sun.tools.tree.GreaterOrEqualExpression;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FavouriteColor {

    private static final String INPUT_TOPIC = "favourite-color-input";
    private static final String OUTPUT_TOPIC = "favourite-color-output";
    private static final String STORE_NAME = "favourite-color-by-user";
    private static final String AGGREGATE_NAME = "color-count";

    public static enum COLORS_IN_SCOPE {
        RED,
        GREEN,
        BLUE
    }

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5);
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();

        // 1. get KStream (null key-> user,fav-color)
        KStream<String, String> textLines = builder.stream(INPUT_TOPIC);
        KTable<String, Long> favColorCount = textLines
                // 2. filter bad values
                .filter((nullKey, value) -> value.contains(","))
                // 3. map to (user, fav-color)
                .map((nullkey, value) -> {
                    String[] userColor = value.split(",");
                    return KeyValue.pair(userColor[0].toLowerCase(), userColor[1].toLowerCase());
                })
                // 4. filter only RGB value
                .filter((user, color) -> {
                    System.out.println(String.format("Received (%s,%s)", user, color));
                    String colorUC = color.toUpperCase();
                    return Arrays.asList(COLORS_IN_SCOPE.values()).stream().anyMatch(color1 ->
                            color1.name().equals(colorUC));
                })
                // 5. convert to KTable
                .toTable(Materialized.as(STORE_NAME))
                // 6. group by color
                .groupBy((user, color) -> KeyValue.pair(color, color))
                // 7. get count for each color
                .count(Named.as(AGGREGATE_NAME));
        // 8. write to output topic
        favColorCount.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        // start Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        System.out.println(streams.toString());
    }


}
