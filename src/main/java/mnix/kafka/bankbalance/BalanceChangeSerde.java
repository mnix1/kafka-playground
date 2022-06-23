package mnix.kafka.bankbalance;


import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static mnix.kafka.GsonFactory.GSON;

public class BalanceChangeSerde implements Serde<BalanceChange> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serde.super.configure(configs, isKey);
    }

    @Override
    public void close() {
        Serde.super.close();
    }

    @Override
    public Serializer<BalanceChange> serializer() {
        return (topic, data) -> GSON.toJson(data).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Deserializer<BalanceChange> deserializer() {
        return (topic, data) -> GSON.fromJson(new String(data), BalanceChange.class);
    }
}
