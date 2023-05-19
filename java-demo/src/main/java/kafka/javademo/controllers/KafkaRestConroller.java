package kafka.javademo.controllers;

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
import org.springframework.web.bind.annotation.ResponseBody;
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
                System.out.println("Error in sending record");
                System.out.println(e);
            } catch (InterruptedException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }
        }

        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(messageObjects);
    }

    @GetMapping("/message-consumer")
    @ResponseBody
    public Object messageConsumer() throws JsonProcessingException {
        int noMessageFound = 0;

        Consumer<Long, String> consumer = KafkaConsumerCreator.createConsumer();

        Map<String, String> object = new HashMap<>();

        while (true) {
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(000);

            System.out.println("Consumer Records Count: " + consumerRecords.count());
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > KafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT) {
                    System.out.println("No Record available.");
                    // If no message found count is reached to threshold exit loop.
                    break;
                } else {
                    continue;
                }
            }

            // Print each record.
            consumerRecords.forEach(record -> {
                // System.out.println("Record Key " + record.key());
                // System.out.println("Record value " + record.value());
                // System.out.println("Record partition " + record.partition());
                // System.out.println("Record offset " + record.offset());

               // object.put(record.key(), record.value());
            });

            // commits the offset of record to broker.
            consumer.commitAsync();
        }
        consumer.close();

        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(object);
    }

}
