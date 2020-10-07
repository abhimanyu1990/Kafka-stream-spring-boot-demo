package poc.kafka.configurations;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import poc.kafka.entity.Message;

@Configuration
public class KafkaProducerConfiguration {
	    @Value("${kafka.bootstrap-servers}")
		private String bootstrapServers;
	    
	    @Bean
	    public Map<String,Object> producerConfigs(){
	    	HashMap<String,Object> kafkaProducerProperties = new HashMap<>();
	    	
	    	/*
	    	 * Kafka bootstrap servers are comma separated host and port address of the brokers in a "bootstrap" 
	    	 * Kafka cluster that a Kafka client connects to initially to bootstrap itself.
	    	 */
	    	kafkaProducerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	    	
	    	/*
	    	 *  Kafka stores and transports byte arrays in its queue. The String and Byte array serializers are provided by 
	    	 *  Kafka by default, but if a developer is using them for objects which are not Strings or byte arrays, then 
	    	 *  Java’s default serializer for your objects will be used.If the JVM is unable to serialize your object using 
	    	 *  the default serializer, you will get a run-time error.
	    	 *  It is good practice to define the Serializer for the Kafka.
	    	 */
	    	
	    	//Serialized class to be use for Key
	    	kafkaProducerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
	    	
	    	//Serialized class to be used for value
	    	kafkaProducerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,JsonSerializer.class);
	    	return kafkaProducerProperties;
	    }
	    
	    
	    
	    // ProduceFactory is responsible for creating the kafka producer instances.
	    @Bean
	    public ProducerFactory<String, Message> producerFactory() {
	      return new DefaultKafkaProducerFactory<>(producerConfigs());
	    }

	    //KafkaTemplate helps us to in sending messages to the respective topic
	    @Bean
	    public KafkaTemplate<String, Message> kafkaTemplate() {
	      return new KafkaTemplate<>(producerFactory());
	    }
}
