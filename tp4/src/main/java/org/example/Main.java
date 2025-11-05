package org.example;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
// Configurer l'application Kafka Streams
        Properties props = new Properties();
        props.put("application.id", "kafka-streams-app");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
// Construire le flux
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> sourceStream = builder.stream("text-input");

        final List<String> forbiddenWords = Arrays.asList("HACK", "SPAM", "XXX");
        Predicate<String, String> isInvalid = (key, value) -> {
            if (value == null) {
                return true;
            }
            String trimmedValue = value.trim();
            if (trimmedValue.isEmpty()) {
                return true;
            }
            if (value.length() > 100) {
                return true;
            }
            String upperValue = trimmedValue.toUpperCase();
            for (String word : forbiddenWords) {
                if (upperValue.contains(word)) {
                    return true;
                }
            }
            return false;
        };
        Predicate<String, String> isValid = (key, value) -> true;


        KStream<String, String>[] branchedStreams = sourceStream.branch(isInvalid, isValid);

        branchedStreams[0].to("text-dead-letter");
// Transformation
        KStream<String, String> validAndCleanedStream = branchedStreams[1].mapValues(value -> value.trim())
                .mapValues(value -> value.replaceAll("\\s+", " "))
                .mapValues(value -> value.toUpperCase()+"-TEST");
        validAndCleanedStream.to("text-clean");

// Démarrer l'application Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
// Ajouter un hook pour arrêter proprement
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}