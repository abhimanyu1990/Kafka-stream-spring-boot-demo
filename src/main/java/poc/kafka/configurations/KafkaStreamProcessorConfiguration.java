package poc.kafka.configurations;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;


import poc.kafka.entity.Message;
import poc.kafka.entity.Tempreature;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamProcessorConfiguration {
	
	@Value("${kafka.topic.name}")
    private String inputTopic;
	
	@Value("${kafka.bootstrap-servers}")
	private String bootstrapServers;
	 
	@Value("${kafka.topic.secondTopicName}")
	private String secondTopicName;
	
	 @Value("${kafka.consumer.application-id}")
	 private String applicationId;
	
	/*
	 * Setting up KafkaStreams configuration by providing Kafka broker server details, Key and Value class serialization detail,
	 * Exception handler class detail, and providing application Id to it
	 * 
	 */
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs(KafkaProperties kafkaProperties) {
        Map<String, Object> config = new HashMap<>();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,applicationId);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, new JsonSerde<>(Message.class).getClass());
        config.put(JsonDeserializer.KEY_DEFAULT_TYPE, String.class);
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Message.class);
        config.put("default.deserialization.exception.handler", LogAndContinueExceptionHandler.class);
        return new KafkaStreamsConfiguration(config);
    }
    

    /*
     *  Procerssing the stream receive from "Input Topic" , identifying the tempreature class and adding it out topic -"secondTopicName" which will be
     *  further consume by consumer 
     */
    @Bean
    public KStream<String, Message> kStreamJson(StreamsBuilder builder) {
    	KStream<String, Message> stream = builder.stream(inputTopic, Consumed.with(Serdes.String(), new JsonSerde<>(Message.class)));
    	
    	stream.foreach((k,v)->{
    		double temp = v.getTempreature();
    		if(temp > 35) {
    			v.setTempClass(Tempreature.HIGH);
    		}
    		
    		if(temp < 35  && temp >=25){
    			v.setTempClass(Tempreature.MODERATE);
    		}
    		
    		if(temp < 25  && temp >=15){
    			v.setTempClass(Tempreature.LOW);
    		}
    		
    		if(temp < 15 ){
    			v.setTempClass(Tempreature.EXTREMELY_LOW);
    		}
    	});
        
    	stream.to(secondTopicName,Produced.with(Serdes.String(), new JsonSerde<>(Message.class)));
        return stream;
    }
    

}
