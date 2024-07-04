package com.infosys.controller;

import com.infosys.Dto.MessageDTO;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MessageController {

    @KafkaListener(topics = "test-topic", groupId = "${spring.kafka.consumer.group-id}")
    public void listenKafka(MessageDTO message) {
        System.out.println("Received message from Kafka: " + message.getMessage());
    }

    @PostMapping("/endpoint")
    public ResponseEntity<String> receiveMessage(@RequestBody MessageDTO message) {
        System.out.println("Received message via REST: " + message.getMessage());
        return ResponseEntity.ok("Message received");
    }
}

