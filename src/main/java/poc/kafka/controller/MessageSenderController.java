
package poc.kafka.controller;



import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;


import poc.kafka.entity.Message;
import poc.kafka.service.KafkaMessageSenderService;

@RestController
public class MessageSenderController {
	
	@Autowired
	KafkaMessageSenderService kafkaMessageSenderService;
	
	@PostMapping("/api/send")
	public void sendMessage( @RequestBody Message message) {
		kafkaMessageSenderService.sendMessage("test", message);
	 }

}

