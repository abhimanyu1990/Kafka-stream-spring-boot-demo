package poc.kafka.configurations;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import poc.kafka.entity.Message;


@Configuration
public class KafkaConsumerConfiguration {

	@Value("${kafka.bootstrap-servers}")
	private String bootstrapServers;
    
	
	
	@Value("${kafka.consumer.group}")
	private String groupName;
	
    @Bean
    public Map<String,Object> consumerConfigs(){
    	HashMap<String,Object> kafkaConsumerProperties = new HashMap<>();
    	
    	/*
    	 * Kafka bootstrap servers are comma separated host and port address of the brokers in a "bootstrap" 
    	 * Kafka cluster that a Kafka client connects to initially to bootstrap itself.
    	 */
    	kafkaConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    	kafkaConsumerProperties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,groupName);
    	
    	
    	//Class to be used for de-searialization of key
    	kafkaConsumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    	
    	//Class to be used for de-searialization of value
    	kafkaConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,JsonDeserializer.class);
    	return kafkaConsumerProperties;
    }
    
    
    
    // ConsumerFactory is responsible for creating the kafka consumer instances.
    @Bean
    public ConsumerFactory<String, Message> consumerFactory() {
    	return new DefaultKafkaConsumerFactory<>(
                consumerConfigs(),
                new StringDeserializer(),
                new JsonDeserializer<>(Message.class));
    }

    
   // KafkaListenerContainer receives all the messages from all topics or partitions on a single thread.
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Message>> kafkaListenerContainerFactory() {
      ConcurrentKafkaListenerContainerFactory<String, Message> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
      factory.setConsumerFactory(consumerFactory());
      factory.setConcurrency(3);
      factory.getContainerProperties().setPollTimeout(3000);
      return factory;
    }
    
    
}
