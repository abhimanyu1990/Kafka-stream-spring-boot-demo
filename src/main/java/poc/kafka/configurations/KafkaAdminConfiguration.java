package poc.kafka.configurations;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class KafkaAdminConfiguration {
	 @Value("${kafka.bootstrap-servers}")
	 private String bootstrapServers;
	 
	@Bean
	KafkaAdmin admin() {
	  Map<String, Object> configs = new HashMap<>();
	  configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	  return new KafkaAdmin(configs);
	}

}