package mnix.kafka.bankbalance;

import mnix.kafka.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

import static mnix.kafka.GsonFactory.GSON;
import static mnix.kafka.bankbalance.BankBalanceConfig.INPUT_TOPIC;
import static mnix.kafka.bankbalance.BankBalanceConfig.OUTPUT_TOPIC;

public class Streams {
    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        stream.mapValues((key, value) -> GSON.fromJson(value, BalanceChange.class))
                .selectKey((key, value) -> value.getName())
                .groupByKey()
                .reduce(BalanceChange::merge)
                .mapValues((readOnlyKey, value) -> GSON.toJson(value))
                .toStream()
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config());
        streams.cleanUp();
        streams.start();
        System.out.println(topology.describe().toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Properties config() {
        Properties config = Config.config();
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, BalanceChangeSerde.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        return config;
    }
}
