package mnix.kafka.favoritecolor;

import mnix.kafka.Config;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static mnix.kafka.favoritecolor.FavoriteColorConfig.OUTPUT_TOPIC;

public class Consumer {
    public static void main(String[] args) {
        KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(config());
        consumer.subscribe(List.of(OUTPUT_TOPIC));
        while(true) {
            ConsumerRecords<String, Long> poll = consumer.poll(Duration.ofSeconds(2));
            System.out.println(poll.count());
            Iterable<ConsumerRecord<String, Long>> records = poll.records(OUTPUT_TOPIC);
            records.forEach(v -> {
                System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
                        v.key(), v.value(),
                        v.partition(), v.offset());
            });
//            consumer.commitAsync();
        }
//        consumer.close(Duration.ofSeconds(5));
    }

    private static Properties config() {
        Properties config = Config.config();
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(CommonClientConfigs.CLIENT_ID_CONFIG, "colors-consumer");
        config.put(CommonClientConfigs.GROUP_ID_CONFIG, "colors-consumer");
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        return config;
    }
}
