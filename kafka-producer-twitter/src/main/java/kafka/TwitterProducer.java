package kafka;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
	private String consumerApiKey = "zEMJl1JRL1H8M8vDqbsrUv40r";
	private String consumerApiSecret = "vajrC5eOse6padPNPiteTk9mygN27JtENVaD8HPgkHlhM68Y00";
	private String token = "699653-gy6hEogE1bL62osGyUjC3znA01bU1d7to6EaybpzuVX";
	private String secret = "o0foRkZ4iglyWQAkYFnyGQMSCec8wpKAwFSqJaOoMAuYO";
	
	List<String> terms = Lists.newArrayList("kafka", "usa", "politics", "covid");
	
	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
	
	public TwitterProducer(){}
	
	public static void main(String[] args) {
		new TwitterProducer().run();
	}
	
	public void run(){
		logger.info("Setup");
		/* Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		// create a twitter client
		Client client = createTwitterClient(msgQueue);
		client.connect();
		
		// create a kafka producer
		KafkaProducer<String,String> producer = createKafkaProducer();
		
		//add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			logger.info("stopping application...");
			logger.info("shutting down client from Twitter...");
			client.stop();
			logger.info("closing producer");
			producer.close();
			logger.info("done");
		}));
		
		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
			if(msg != null){
				logger.info(msg);
				producer.send(new ProducerRecord<>("twitter_tweets",null,msg), new Callback(){
					
					@Override
					public void onCompletion(RecordMetadata recordMetadata, Exception e) {
						if(e != null){
							logger.error("Something bad happen", e);
						}
					}
				});
			}
			logger.info("End of application");
		}
		//loop to send tweets to kafka
	}
	
	public Client createTwitterClient(BlockingQueue<String> msgQueue){
		
		/* Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		// List<Long> followings = Lists.newArrayList(1234L, 566788L);
		
		// hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerApiKey, consumerApiSecret, token, secret);
		
		ClientBuilder builder = new ClientBuilder()
				.name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts)
				.authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));
				// .eventMessageQueue(eventQueue);     // optional: use this if you want to process client events
		
		Client hosebirdClient = builder.build();
		return hosebirdClient;
		// Attempts to establish a connection.
		// hosebirdClient.connect();
	}
	
	public KafkaProducer<String,String> createKafkaProducer(){
		String bootstrapServers = "localhost:9092";
		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// safe producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
		//kafka 2.0 >= 1.1 so we can keep this as 5. Use 1 otherwise.
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
		
		// High throughput producer (at the expense of a bit of latency and CPU usage)
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32 * 1024)); // 32kb batch size
		
		// create the producer <key,value>
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		return producer;
	}
	
	
}
