package mnix.kafka.wordcount;

import mnix.kafka.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

public class Streams {
    public static void main(String[] args) {
        Topology topology = topology();
        KafkaStreams streams = new KafkaStreams(topology, config());
        streams.cleanUp();
        streams.start();
        System.out.println(topology.describe().toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    public static Topology topology(){
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(WordCountConfig.INPUT_TOPIC);
        stream.flatMapValues(value -> Arrays.asList(Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS).split(value.toLowerCase())))
                .selectKey((key, value) -> value)
                .groupByKey()
                .count()
                .toStream()
                .to(WordCountConfig.OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

    private static Properties config() {
        Properties config = Config.config();
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-counter");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        return config;
    }
}
