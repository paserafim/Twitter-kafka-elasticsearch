package tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {
	
	public static void main(String[] args) {
		
		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
		
		String bootstrapServers = "localhost:9092";
		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// create the producer <key,value>
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		
		for (int i=0; i<10;i++){
			// create a ProducerRecord <topic, value>
			final ProducerRecord<String,String> record = new ProducerRecord<String, String>("first-topic","hello world " + Integer.toString(i));
			// send data - This is asynchronous
			producer.send(record, new Callback() {
				public void onCompletion(RecordMetadata recordMetadata, Exception e) {
					//Execute every time a record is successfully send or exception is thrown
					if(e == null){
						// The record was successfully sent
						logger.info("\n Received new metadata: \n" +
								"Topic: " + recordMetadata.topic() + "\n" +
								"Partition: " + recordMetadata.partition() + "\n" +
								"Offset: " + recordMetadata.offset() + "\n" +
								"Timestamp: " + recordMetadata.timestamp());
					} else {
						logger.error("Error while producing", e);
					}
				}
			});
		}

		//flush data
		producer.flush();
		//close producer
		producer.close();
	}
}
