package com.github.abhishek;

//import com.google.gson.JsonParser;
import com.fasterxml.jackson.core.*;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static RestHighLevelClient createClient() {

        //////////////////////////
        /////////// IF YOU USE LOCAL ELASTICSEARCH
        //////////////////////////

        //  String hostname = "localhost";
        //  RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,9200,"http"));


        //////////////////////////
        /////////// IF YOU USE BONSAI / HOSTED ELASTICSEARCH
        //////////////////////////

        // replace with your own credentials
        String hostname = ""; // localhost or bonsai url
        String username = ""; // needed only for bonsai
        String password = ""; // needed only for bonsai

        // credentials provider help supply username and password
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        //Rest Client Builder
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    // we tell the client to take the credentials from above and use it
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        // Insert data into ElasticSearch
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    //Creating the KafkaConsumer,argument is set to topic for parameterising
    public static KafkaConsumer<String, String> createConsumer(String topic){
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "myGroupElasticSearch";


        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        //Deserialization is done when kafka sends bytes to consumers,consumers have to take these bytes & create a string from it !
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //Auto-Offset is set to earliest to read data from very beginning
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //can have latest,none,earliest
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //to disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); //to get & write 100 records at a time to ElasticSearch


        //create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //subscribe the consumer
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    //to get ID from JSON data as String we use JsonParser
    private static  JsonParser jsonParser = new JsonParser();

    private static String extractIdFromTweet(String tweetJson) {
        //gson library
        return jsonParser.parse(tweetJson)
        .getAsJsonObject()
        .get("id_str")
        .getAsString();
    }
    public static void main(String[] args) throws IOException {


        //create a logger
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        RestHighLevelClient client = createClient();

        //Pass the topic to KafkaConsumer
        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        //poll for new data
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            //this is to specify how many records we received
            Integer recordCount = records.count();
            logger.info("Received" + recordCount + "records");

            BulkRequest bulkRequest = new BulkRequest();

            //loop over the records
            for (ConsumerRecord<String, String> record : records) {

                // 2 strategies,one is Kafka Generic ID
                //this id is unique because each msg in Kafka has partition,topic and offset
                /** String id  = record.topic() + "_" + record.partition() + "_" +  record.offset(); */

                try {
                    //twitter feed specific id
                    String id = extractIdFromTweet(record.value());

                    //here we insert data to ElasticSearch
                    //Insert Data into ElasticSearch,takes 3 parameters index,type and ID,the index will fail if index twitter doesn't exist!,xcontent
                    //is used to say we are indexing JSON Document
                    @Deprecated
                    IndexRequest indexRequest = new IndexRequest("twitter", //index tweet,comparatbly slow
                            "tweets",
                            id //this is to make our consumer idempotent
                    ).source(record.value(),//source of the value is from the consumer that's recorded here
                            XContentType.JSON);//type is JSON

                    //we loop through all the records and load to bulkRequest
                    bulkRequest.add(indexRequest); //we add to bulkRequest(takes no time)


                    //to get the response out of the request
                    /** IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                     // to get the indexResponse as an ID every time we insert data to ElasticSearch
                     String id = indexResponse.getId();
                     //log the ID(Response)
                     logger.info(indexResponse.getId());
                     try {
                     //after 1000ms the data insertion will stop throwing an exception
                     Thread.sleep(10);//introduce a small delay
                     } catch (InterruptedException e) {
                     e.printStackTrace();
                     }*/
                } catch (NullPointerException e){ //whenever a tweet doesn't have an id i.e str_id
                    logger.warn("Skipping Bad Data" + record.value());
                }
            }
            //if only we receive any record
            if (recordCount > 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing the Offset!!!!");
                consumer.commitSync(); //consumer offset commit strategies (enable.auto.commit->false)
                logger.info("Offsets have been committed....");
                //to sleep a bit
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        //close the client gracefully
        //client.close();
    }
}

