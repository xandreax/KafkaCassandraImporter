package esteco.ubies.importer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;

import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerFactory {
    private static Consumer<Long, Document> consumer;

    public KafkaConsumerFactory(String bootStrapServers, String consumerGroupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.LongDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "esteco.ubies.importer.bsonserializators.BsonDeserializer");
        consumer = new KafkaConsumer< >(props);
    }

    public static Consumer <Long, Document> subscribe(String topicName) {
        consumer.subscribe(Collections.singletonList(topicName));
        return consumer;
    }
}
