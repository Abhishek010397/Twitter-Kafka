package co.github.abhishek.tutorial4;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamFilterTweets {
    public static void main(String[] args) {

        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "Kafka-Streams");
        //by default we will have strings as key
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        //default strings as value
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create the topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
             //input topic, from this we can run some operations
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String, String> filteredStream = inputTopic.filter(
                //Java8 Style
                /** filter a user having 10000 followers */
                (k, jsonTweet) -> extractUserFollowersInTweet(jsonTweet) > 10000
        );
        //filtered stream will be taken care by new topic
        filteredStream.to("Filtered_tweets");

        // build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        //start the application
        kafkaStreams.start();
    }
    @Deprecated
    private static JsonParser jsonParser = new JsonParser();

    @Deprecated
    private static Integer extractUserFollowersInTweet(String tweetJson) {
        //gson library
        try {
            return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();

        }catch (NullPointerException e){
            return 0;
        }
    }
}
