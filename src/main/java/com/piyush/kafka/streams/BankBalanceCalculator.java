package com.piyush.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class BankBalanceCalculator {
    private static final String BANK_TRANSACTIONS_TOPIC = "bank-account-transactions";
    private static final String BANK_BALANCE_TOPIC = "bank-balance";
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    KafkaStreams bankBalanceStreams;

    @SneakyThrows
    BankBalanceCalculator() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-calc");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5);
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Serde<JsonNode> jsonSerde = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer());


        ObjectMapper objectMapper = new ObjectMapper();

        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("count", 0);
        initialBalance.put("balance", 0);
        initialBalance.put("time", "2020-12-01 00:00:00.000");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, JsonNode> bankTransactions = builder.stream(BANK_TRANSACTIONS_TOPIC, Consumed.with(Serdes.String(), jsonSerde));
        KTable<String, JsonNode> bankBalance = bankTransactions
                .groupByKey()
                .aggregate(
                        () -> initialBalance,
                        (user, lastTransaction, currentBalance) -> (ObjectNode) computeBalance(lastTransaction, currentBalance),
                        Materialized.<String, JsonNode>as(Stores.inMemoryKeyValueStore("bank-balance"))
                .withKeySerde(Serdes.String())
                .withValueSerde(jsonSerde));
        bankBalance.toStream().to(BANK_BALANCE_TOPIC, Produced.with(Serdes.String(), jsonSerde));

        bankBalanceStreams = new KafkaStreams(builder.build(), config);
        bankBalanceStreams.cleanUp();
        bankBalanceStreams.start();
        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(bankBalanceStreams::close));

        System.out.println(bankBalanceStreams.toString());
    }

    private JsonNode computeBalance(JsonNode transaction, JsonNode balance) {
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", balance.get("count").asInt()+1);
        newBalance.put("balance", balance.get("balance").asDouble() + transaction.get("balance").asDouble());
        LocalDateTime lastUpdateTime =  LocalDateTime.from(dateTimeFormatter.parse(balance.get("time").asText()));
        LocalDateTime transactionTime = LocalDateTime.from(dateTimeFormatter.parse(transaction.get("time").asText()));
        LocalDateTime newBalanceTime = lastUpdateTime.compareTo(transactionTime) > 0 ? lastUpdateTime : transactionTime;
        newBalance.put("time", dateTimeFormatter.format(newBalanceTime));
        return newBalance;
    }

    public static void main(String[] args) {
        BankBalanceCalculator bankBalanceCalculator = new BankBalanceCalculator();
    }


}
