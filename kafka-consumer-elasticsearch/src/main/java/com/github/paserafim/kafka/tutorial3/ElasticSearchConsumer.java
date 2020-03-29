package com.github.paserafim.kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {
	
	public static RestHighLevelClient createClient(){
		//Replace with our own credentials
		String hostname = "elastic-testing-3302064911.eu-central-1.bonsaisearch.net";
		String username = "q8na5thbak";
		String password = "izwpuk8x2k";
		
		// don't do if you run a local Elastic Search
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,password));
		
		RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
				.setHttpClientConfigCallback(httpAsyncClientBuilder ->
						httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
		
		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
		
	}
	
	public static KafkaConsumer<String,String> createConsumer(String topic){
		
		String bootstrapServers = "localhost:9092";
		String groudId = "kafka-demo-elasticsearch";
		//String topic = "twitter-tweets";
		
		// create consumer properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groudId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); //earliest, latest, none
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false"); //disable auto commit of offsets
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");
		
		// create Consumer
		KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(properties);
		consumer.subscribe(Collections.singleton(topic));
		
		return consumer;
	}
	
	public static void main(String[] args) throws IOException {
		
		Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
		RestHighLevelClient client = createClient();
		
		KafkaConsumer<String,String> consumer = createConsumer("twitter_tweets");
		
		// poll for new data
		while (true){
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.X
			
			Integer recordCount = records.count();
			logger.info("Received " + recordCount + " records");
			
			BulkRequest bulkRequest = new BulkRequest();
			
			for (ConsumerRecord<String, String> record : records) {
				//Where we insert data in Elastic Search
				
				// 2 strategies create ids
				// -- kafka generic ID --
				//String id = record.topic() + "_" + record.partition() + "_" + record.offset();
				//Twitter feed specific id - get id_str from the tweet
				
				try {
					String id = extractFromTweet(record.value());
					IndexRequest indexRequest = new IndexRequest(
							"twitter",
							"tweets",
							id //this is to make our consumer idempotent - avoid duplicates
					).source(record.value(), XContentType.JSON);
					
					bulkRequest.add(indexRequest);
				}catch (NullPointerException e){
					logger.warn("skipping bad data (without id_str) " + record.value());
				}
				
				//IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
				//String id1 = indexResponse.getId();
				//logger.info(id1);

			}
			if(recordCount > 0){
				BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
				
				logger.info("Commiting offsets...");
				consumer.commitAsync();
				logger.info("Offsets have been committed");
				try {
					Thread.sleep(10); // introduced small delay
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			

		}
		
		//close the client gracefully
		//client.close();
		
	}
	
	private static JsonParser jsonParser = new JsonParser();
	
	private static String extractFromTweet(String tweetJson){
		// Get a library for parse JSON - GSON (Google json)
		
		//Course video
		return jsonParser.parse(tweetJson)
				.getAsJsonObject()
				.get("id_str")
				.getAsString();
		
		// More recent
		/*
		GsonBuilder builder = new GsonBuilder();
		Gson gson = builder.create();
		
		JsonElement jsonElement = gson.fromJson(tweetJson,JsonElement.class)
				.getAsJsonObject().get("id_str");
		
		return jsonElement.getAsString();
		*/
	}
	
}
