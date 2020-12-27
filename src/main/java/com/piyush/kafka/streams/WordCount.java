package com.piyush.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCount {

    private final static String INPUT_TOPIC = "word-count-input";
    private final static String OUTPUT_TOPIC = "word-count-output";

    public static void main(String[] args) {
        Properties  config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // 1 Stream from Kafka input topic
        KStream<String, String> wordCountInput = builder.stream(INPUT_TOPIC);
        // 2 map values to lower case
        KTable<String, Long> wordCount = wordCountInput.mapValues(textLine -> textLine.toLowerCase())
        // 3 flatmap to split by space
        .flatMapValues(lowerCaseString -> Arrays.asList(lowerCaseString.split(" ")))
        // 4 select value as the new key
        .selectKey((ignoredKey, word) -> word)
        // 5 group by key
        .groupByKey()
        // 6 Aggregate by count
        .count(Named.as("word-count"));

        // 7 Output to kafka topic
        wordCount.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        // start Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        System.out.println(streams.toString());
    }
}
