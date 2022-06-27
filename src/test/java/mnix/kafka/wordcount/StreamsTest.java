package mnix.kafka.wordcount;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class StreamsTest {
    TopologyTestDriver topologyTestDriver;
    TestInputTopic<String, String> inputTopic;
    TestOutputTopic<String, Long> outputTopic;

    @BeforeEach
    void setUp() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:1");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        topologyTestDriver = new TopologyTestDriver(Streams.topology(), config);
        inputTopic = topologyTestDriver.createInputTopic(WordCountConfig.INPUT_TOPIC, new StringSerializer(), new StringSerializer());
        outputTopic = topologyTestDriver.createOutputTopic(WordCountConfig.OUTPUT_TOPIC, new StringDeserializer(), new LongDeserializer());
    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }

    @Test
    void test() {
        //given
        inputTopic.pipeInput("elo elo witam");
        inputTopic.pipeInput("siema elo witam");
        //then
        Map<String, Long> records = outputTopic.readKeyValuesToMap();
        assertThat(records.get("elo")).isEqualTo(3L);
        assertThat(records.get("witam")).isEqualTo(2L);
        assertThat(records.get("siema")).isEqualTo(1L);
    }
}