package mnix.kafka.bankbalance;

import mnix.kafka.Config;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static mnix.kafka.GsonFactory.GSON;
import static mnix.kafka.bankbalance.BankBalanceConfig.INPUT_TOPIC;

public class Producer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer = new KafkaProducer<>(config());
        balanceChanges().forEach(balanceChange -> {;
            String value = GSON.toJson(balanceChange);
            System.out.println(value);
            ProducerRecord<String, String> record = new ProducerRecord<>(INPUT_TOPIC, null, value);
            Future<RecordMetadata> future = producer.send(record);
            try {
                RecordMetadata recordMetadata = future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static Stream<BalanceChange> balanceChanges() {
        Random random = new Random();
        List<String> names = List.of("Adam", "Marcin", "Paweł", "Aga", "Anna", "Robert");
        AtomicReference<Instant> instant = new AtomicReference<>(Instant.parse("2000-01-01T00:00:00Z"));
        return Stream.generate(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            instant.set(instant.get().plusSeconds(random.nextLong(100, 500000)));
            return new BalanceChange(
                    names.get(random.nextInt(0, names.size())),
                    random.nextLong(-500, 500),
                    instant.get()
            );
        });
    }

    private static Properties config() {
        Properties config = Config.config();
        config.put(CommonClientConfigs.CLIENT_ID_CONFIG, "balance-producer");
        config.put(CommonClientConfigs.GROUP_ID_CONFIG, "balance-producer");
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, "3");
        config.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        return config;
    }
}
