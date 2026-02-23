package kafka

@GrabConfig(systemClassLoader=true)
@Grab(group='org.apache.kafka', module='kafka-clients', version='3.6.1')
@Grab(group='org.slf4j', module='slf4j-api', version='1.7.36')
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import com.kms.katalon.core.annotation.Keyword
import com.kms.katalon.core.util.KeywordUtil

import java.time.Duration
import java.util.Properties

public class KafkaKeyword {

    private KafkaConsumer<String, String> consumer

    /**
     * Initialize Kafka Consumer
     * @param bootstrapServers e.g. "localhost:9092"
     * @param groupId e.g. "katalon-consumer-group"
     * @param topic topic to subscribe
     */
    @Keyword
    def void initConsumer(String bootstrapServers, String groupId, String topic) {
        Properties props = new Properties()
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")

        consumer = new KafkaConsumer<>(props)
        consumer.subscribe(Collections.singletonList(topic))
        KeywordUtil.logInfo("Subscribed to topic: " + topic)
    }

    /**
     * Poll for messages
     * @param timeoutMillis timeout in milliseconds
     * @return List of message values
     */
    @Keyword
    def List<String> consumeMessages(int timeoutMillis) {
        if (consumer == null) {
            KeywordUtil.markFailed("Consumer not initialized. Call initConsumer first.")
            return []
        }

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(timeoutMillis))
        List<String> messages = new ArrayList<>()

        for (ConsumerRecord<String, String> record : records) {
            messages.add(record.value())
            KeywordUtil.logInfo("Consumed message: " + record.value())
        }

        return messages
    }

    /**
     * Close the consumer
     */
    @Keyword
    def void closeConsumer() {
        if (consumer != null) {
            consumer.close()
            KeywordUtil.logInfo("Kafka Consumer closed.")
        }
    }
}
