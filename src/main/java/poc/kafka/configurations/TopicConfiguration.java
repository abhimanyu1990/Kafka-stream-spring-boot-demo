package poc.kafka.configurations;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class TopicConfiguration {
	
	@Value("${kafka.topic.name}")
	private String topicName;
	
	@Value("${kafka.topic.secondTopicName}")
	private String secondTopicName;
	
	@Value("${kafka.topic.replication-factor}")
	private int replicationFactor;
	
	@Value("${kafka.topic.no-of-partition}")
	private short numPartitions;
	
	/* A new Topic with specified name, replication factor and partition count will be created.
	 * A new topic to be created via Admin.createTopics(Collection)in our broker. 
	 * With Spring Boot, a KafkaAdminClient bean is automatically registered which is implementation class of Admin
	 */
	
	@Bean
	public NewTopic firstTopic() {
		return TopicBuilder.name(topicName).replicas(replicationFactor).partitions(numPartitions).build();
	}

	@Bean
	public NewTopic seondTopic() {
		return TopicBuilder.name(secondTopicName).replicas(replicationFactor).partitions(numPartitions).build();
	}
}