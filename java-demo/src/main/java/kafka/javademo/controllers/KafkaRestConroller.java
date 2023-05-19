package kafka.javademo.controllers;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import constants.KafkaConstants;
import entity.Message;
import services.KafkaConsumerCreator;
import services.KafkaProducerCreator;

@RestController
@RequestMapping("/api/v1")
public class KafkaRestConroller {

    @GetMapping("/message-producer")
    public Object messageProducer(@RequestParam String message) throws JsonProcessingException {
        Map<Integer, Message> messageObjects = new HashMap<Integer, Message>();
        Producer<Long, String> producer = KafkaProducerCreator.createProducer();

        for (int index = 0; index < KafkaConstants.MESSAGE_COUNT; index++) {
            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(KafkaConstants.TOPIC_NAME, message + "-" + index);
            try {
                RecordMetadata metadata = producer.send(record).get();
                Message messageMap = new Message(KafkaConstants.TOPIC_NAME, metadata.partition(), metadata.offset(), message + "_" + index);
                messageObjects.put(index, messageMap);
            } catch (ExecutionException e) {
                System.out.println(e);
            } catch (InterruptedException e) {
                System.out.println(e);
            }
        }

        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(messageObjects);
    }

    @GetMapping("/message-consumer")
    public Object messageConsumer() throws JsonProcessingException {
        Map<Long, Message> messageObjects = new HashMap<Long, Message>();

        Consumer<Long, String> consumer = KafkaConsumerCreator.createConsumer();

        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            consumerRecords.forEach(record -> {
                Message messageMap = new Message(record.topic(), record.partition(), record.offset(), record.value());
                messageObjects.put(record.key(), messageMap);
            });

            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(messageObjects);
        }
    }

}
