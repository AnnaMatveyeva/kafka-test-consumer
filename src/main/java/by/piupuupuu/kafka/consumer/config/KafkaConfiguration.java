package by.piupuupuu.kafka.consumer.config;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

	@Value("${kafka.bootstrap-servers}")
	private String kafkaServer;
	private String topic = "pickup-points";

	@Bean
	public KafkaReceiver<String, String> kafkaReceiver() {

		return KafkaReceiver.create(receiverOptions());
	}

	@Bean
	public ReceiverOptions<String, String> receiverOptions() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-test-consumer");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-test-group");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		ReceiverOptions<String, String> receiverOptions = ReceiverOptions.create(props);
		receiverOptions.withKeyDeserializer(new StringDeserializer());
		receiverOptions.withValueDeserializer(new StringDeserializer());

		return receiverOptions.subscription(Collections.singleton(topic));
	}
	@Bean

	Flux<ReceiverRecord<String, String>> reactiveKafkaReceiver(ReceiverOptions<String, String> kafkaReceiverOptions) {
		return KafkaReceiver.create(kafkaReceiverOptions).receive();
	}
}
