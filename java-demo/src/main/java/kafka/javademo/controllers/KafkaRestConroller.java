package kafka.javademo.controllers;

import java.time.Duration;
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

import constants.KafkaConstants;
import services.KafkaConsumerCreator;
import services.KafkaProducerCreator;

@RestController
@RequestMapping("/api/v1")
public class KafkaRestConroller {

  @GetMapping("/message-producer")
  public String messageProducer(@RequestParam String message) {

      String returnMessage = "";

      Producer<Long, String> producer = KafkaProducerCreator.createProducer();

      for (int index = 0; index < KafkaConstants.MESSAGE_COUNT; index++) {
          ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(KafkaConstants.TOPIC_NAME, message + "-" + index);
          try {
              RecordMetadata metadata = producer.send(record).get();
              System.out.println("Record sent with key " + index + " to partition " + metadata.partition() + " with offset " + metadata.offset());
              returnMessage += "Record sent with key " + index + " to partition " + metadata.partition() + " with offset " + metadata.offset() + System.getProperty("line.separator");
          } catch (ExecutionException e) {
              System.out.println("Error in sending record");
              System.out.println(e);
          } catch (InterruptedException e) {
              System.out.println("Error in sending record");
              System.out.println(e);
          }
      }

      return returnMessage;
  }

  @GetMapping("/message-consumer")
  public void messageConsumer() {
      Consumer<Long, String> consumer = KafkaConsumerCreator.createConsumer();

      int noMessageFound = 0;

      while (true) {

          ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
          // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
          if (consumerRecords.count() == 0) {
              noMessageFound++;
              if (noMessageFound > KafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                  // If no message found count is reached to threshold exit loop.
                  break;
              else
                  continue;
          }

          //print each record.
          consumerRecords.forEach(record -> {
              System.out.println("Record Key " + record.key());
              System.out.println("Record value " + record.value());
              System.out.println("Record partition " + record.partition());
              System.out.println("Record offset " + record.offset());
           });

          // commits the offset of record to broker.
           consumer.commitAsync();
      }
      consumer.close();
  }


}
