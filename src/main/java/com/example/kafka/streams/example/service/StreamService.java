package com.example.kafka.streams.example.service;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Titan on 22.10.2017.
 */
@Service
public class StreamService {

    public void serve() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.EXACTLY_ONCE, true);

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> textLines = builder.stream("TextLinesTopic", "TextLinesTopic2");

        textLines = textLines.peek(new ForeachAction<Object, Object>() {
            @Override
            public void apply(Object o, Object o2) {
                System.out.println(o + " " + o2);
            }
        });

        KStream<String, String> stringStringKStream = textLines
                .map((s, s2) -> {
                    String[] split = s2.split("\\s+");

                    System.out.println("running mapper");
                    System.out.println(Arrays.toString(split));

                    return new KeyValue<>(split[0], s2);
                })


                .flatMapValues((String textLine) -> {
                    List<String> strings = Arrays.asList(textLine.toLowerCase().split("\\s+"));
                    System.out.println("running value mapper");
                    System.out.println(strings);
                    return strings;
                });

        KGroupedStream<String, String> stringStringKGroupedStream = stringStringKStream
                .groupByKey();
        KTable<Windowed<String>, String> aggregate1 = stringStringKGroupedStream
                .aggregate(
                        () -> {
                            System.out.println("running initializer");
                            return "";
                        },
                        (key, value, aggregate) -> {
                            System.out.println("running aggregator: " + "\t" + key + "\t" + value + "\t" + aggregate);
                            return aggregate + "\t" + value;
                        },
                        TimeWindows.of(5000),
                        Serdes.String()
                );

        KStream<Windowed<String>, String> windowedStringKStream = aggregate1.toStream();

                windowedStringKStream.to(new Serde<Windowed<String>>() {
                    @Override
                    public void configure(Map<String, ?> map, boolean b) {

                    }

                    @Override
                    public void close() {

                    }

                    @Override
                    public Serializer<Windowed<String>> serializer() {
                        return new Serializer<Windowed<String>>() {
                            @Override
                            public void configure(Map<String, ?> map, boolean b) {

                            }

                            @Override
                            public byte[] serialize(String s, Windowed<String> stringWindowed) {
                                return stringWindowed.key().getBytes();
                            }

                            @Override
                            public void close() {

                            }
                        };
                    }

                    @Override
                    public Deserializer<Windowed<String>> deserializer() {
                        return new Deserializer<Windowed<String>>() {
                            @Override
                            public void configure(Map<String, ?> map, boolean b) {

                            }

                            @Override
                            public Windowed<String> deserialize(String s, byte[] bytes) {
                                return null;
                            }

                            @Override
                            public void close() {

                            }
                        };
                    }
                }, Serdes.String(), "WordsWithCountsTopic");

//        KTable<String, Long> wordCounts = stringStringKStream
//                .groupBy((key, word) -> word)
//                .count("Counts");
//
//        wordCounts.to(Serdes.String(), Serdes.Long(), "WordsWithCountsTopic");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
