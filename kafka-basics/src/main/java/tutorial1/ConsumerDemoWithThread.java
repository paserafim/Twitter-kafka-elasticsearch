package tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

// Lesson 49

public class ConsumerDemoWithThread {
	
	public static void main(String[] args) {
		new ConsumerDemoWithThread().run();
	}
	
	private ConsumerDemoWithThread(){
	
	}
	
	private void run(){
		Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
		
		String bootstrapServers = "localhost:9092";
		String groudId = "my-sixth-application";
		String topic = "first-topic";
		
		// latch for dealing with multiple threads
		CountDownLatch latch = new CountDownLatch(1);
		
		logger.info("Creating the consumer thread");
		// create the consumer runnable
		Runnable myConsumerRunnable = new ConsumerRunnable(latch,bootstrapServers,groudId,topic);
		
		Thread myThread = new Thread(myConsumerRunnable);
		myThread.start();
		
		// Shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Caught shutdown hook");
			((ConsumerRunnable) myConsumerRunnable).shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			logger.info("Application has exited");
		}));
		
		try{
		latch.await();
		} catch (InterruptedException e){
			logger.info("Application got interrupted",e);
		} finally {
			logger.info("Application is closing.");
		}
		
	}
	
	
	public class ConsumerRunnable implements Runnable{
		
		private CountDownLatch latch;
		private KafkaConsumer<String,String> consumer;
		Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
		
		ConsumerRunnable(CountDownLatch latch,String bootstrapServers,String groupId, String topic){
			this.latch = latch;
			
			// create consumer properties
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); //earliest, latest, none
			
			// create Consumer
			KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
			
			// subscribe consumer to our topic(s)
			consumer.subscribe(Collections.singleton(topic)); // subscribe 1 topic
			consumer.subscribe(Arrays.asList(topic));
			
		}
		
		@Override
		public void run() {
			// poll for new data
			try {
				while (true){
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.X
					
					for (ConsumerRecord<String, String> record : records) {
						logger.info("key: " + record.key() + ", Value: " + record.value() );
						logger.info("Partition: " + record.partition());
						logger.info("Offset: " + record.offset());
					}
				}
			}catch (WakeupException e){
				logger.info("Received shutdown signal.");
			} finally {
				consumer.close();
				//tell the main code we're done with the consumer
				latch.countDown();
			}
		}
		
		public void shutdown(){
			// The wakeup() is a special method to interrupt consumer.poll()
			// it will throw the exception WakeUpException
			consumer.wakeup();
		}
	}
}
