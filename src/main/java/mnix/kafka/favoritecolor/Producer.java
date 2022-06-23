package mnix.kafka.favoritecolor;

import mnix.kafka.Config;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static mnix.kafka.favoritecolor.FavoriteColorConfig.INPUT_TOPIC;

public class Producer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer = new KafkaProducer<>(config());
        List<List<String>> values = List.of(
                List.of("stephane", "blue"),
                List.of("john", "green"),
                List.of("stephane", "red"),
                List.of("alice", "red")
        );
        for (List<String> v : values) {
            ProducerRecord<String, String> record = new ProducerRecord<>(INPUT_TOPIC, v.get(0), v.get(1));
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata recordMetadata = future.get();
        }
    }

    private static Properties config() {
        Properties config = Config.config();
        config.put(CommonClientConfigs.CLIENT_ID_CONFIG, "color-producer");
        config.put(CommonClientConfigs.GROUP_ID_CONFIG, "color-producer");
        return config;
    }
}
