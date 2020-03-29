package tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
	
	public static void main(String[] args) {
		String bootstrapServers = "localhost:9092";
		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// create the producer <key,value>
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		// create a ProducerRecord <topic, value>
		ProducerRecord<String,String> record = new ProducerRecord<String, String>("first-topic","hello world");
		// send data - This is asynchronous
		producer.send(record);
		//flush data
		producer.flush();
		//close producer
		producer.close();
	}
}
