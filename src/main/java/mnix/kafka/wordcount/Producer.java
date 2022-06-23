package mnix.kafka.wordcount;

import mnix.kafka.Config;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Producer {
    public static void main(String[] args) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(config());
        List<String> messages = List.of(
                "witam",
                "elo elo",
                "witam elo",
                "siema elo"
        );
        messages.forEach(message -> {
            System.out.println(message);
            ProducerRecord<String, String> record = new ProducerRecord<>(WordCountConfig.INPUT_TOPIC, null, message);
            Future<RecordMetadata> future = producer.send(record);
            try {
                RecordMetadata recordMetadata = future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static Properties config() {
        Properties config = Config.config();
        config.put(CommonClientConfigs.CLIENT_ID_CONFIG, "word-count-producer");
        return config;
    }
}
