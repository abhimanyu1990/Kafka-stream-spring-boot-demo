package poc.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import poc.kafka.entity.Message;

@Service
public class KafkaMessageSenderService {
	
	private static Logger LOGGER = LoggerFactory.getLogger(KafkaMessageSenderService.class);
	
	@Autowired
	private KafkaTemplate<String,Message> kafkaTemplate;
	
	// sending synchronous message to kafka broker
	public void sendMessage(String topicName, Message message) {
		kafkaTemplate.send(topicName, message);
	}
	
	
	//send asynchronous message to kafka broker
	public void sendAsyncMessage(String topicName, Message message) {
		
		ListenableFuture<SendResult<String, Message>> future= kafkaTemplate.send(topicName, message);
		future.addCallback(new ListenableFutureCallback<SendResult<String, Message>>() {
		      @Override
		      public void onSuccess(SendResult<String, Message> result) {
		    	  LOGGER.info("Message [{}] delivered with offset {}",message,result.getRecordMetadata().offset());
		      }
		  
		      @Override
		      public void onFailure(Throwable ex) {
		    	  LOGGER.warn("Unable to deliver message [{}]. {}", message,ex.getMessage());
		      }
		    });
	}

}