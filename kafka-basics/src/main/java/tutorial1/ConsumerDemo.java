package tutorial1;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
	
	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
		
		String bootstrapServers = "localhost:9092";
		String groudId = "my-fourth-application";
		String topic = "first-topic";
		
		// create consumer properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groudId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); //earliest, latest, none
		
		// create Consumer
		KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
		
		// subscribe consumer to our topic(s)
		consumer.subscribe(Collections.singleton(topic)); // subscribe 1 topic
		consumer.subscribe(Arrays.asList(topic));
		
		// poll for new data
		while (true){
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.X
			
			for (ConsumerRecord<String, String> record : records) {
					logger.info("key: " + record.key() + ", Value: " + record.value() );
				logger.info("Partition: " + record.partition());
				logger.info("Offset: " + record.offset());
			}
		}
		
		
	}
}
