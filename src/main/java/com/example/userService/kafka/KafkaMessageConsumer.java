package com.example.userService.kafka;

import com.example.userService.config.KafkaConfig;
import com.example.userService.dto.UserNotificationDto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaMessageConsumer {

    public List<UserNotificationDto> getAllMessages(){
        List<UserNotificationDto> messages = new ArrayList<>();
        KafkaConfig kafkaConfig = new KafkaConfig();
        Properties props = kafkaConfig.createConsumerProperties();

        try(KafkaConsumer<String, UserNotificationDto> consumer = new KafkaConsumer<>(props)){
            List<TopicPartition> partitions = getPartitions(consumer,"users");

            if(partitions.isEmpty()) return messages;

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            readAllMessages(consumer,messages);
            return messages;

        }catch (Exception e){
            e.printStackTrace();
            return null;
        }

    }

    private List<TopicPartition> getPartitions(KafkaConsumer<String, UserNotificationDto> consumer, String topic){
        try {
            return consumer.partitionsFor(topic)
                    .stream()
                    .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))
                    .toList();
        }catch (Exception e){
            return new ArrayList<>();
        }
    }


    private void readAllMessages(KafkaConsumer<String, UserNotificationDto> consumer, List<UserNotificationDto> messages){
        int emptyPolls = 0;
        int maxEmptyPolls = 3;

        while(emptyPolls < maxEmptyPolls){
            try{
                ConsumerRecords<String, UserNotificationDto> records = consumer.poll(Duration.ofMillis(100));

                if(records.isEmpty()){
                    emptyPolls++;
                } else {
                    emptyPolls = 0;
                    for(ConsumerRecord<String, UserNotificationDto> record : records){
                        if(record.value() != null) messages.add(record.value());
                    }
                }
            }catch (RecordDeserializationException e){
                System.err.println("Deserialization error: Skipping current batch...");
                emptyPolls = 0;

                for(TopicPartition partition : consumer.assignment()){
                    long position = consumer.position(partition);
                    consumer.seek(partition, position + 1);
                }

            }
        }
    }
}
