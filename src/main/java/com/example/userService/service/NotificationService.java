package com.example.userService.service;

import com.example.userService.dto.Response;
import com.example.userService.dto.UserNotificationDto;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Service
public class NotificationService {

    public List<Response> sendUserMessage(){
        List<UserNotificationDto> userNotificationDtos = getAllMessages();
        List<Response> list = new ArrayList<>();

        for(UserNotificationDto userNotificationDto : userNotificationDtos){
            System.out.println(userNotificationDto.toString());
        }

        for (UserNotificationDto userNotificationDto : userNotificationDtos) {
            if(userNotificationDto.statusType().equals("CREATED")) list.add(new Response(userNotificationDto.email()
                    + " Здравствуйте! Ваш аккаунт на сайте был успешно создан"));
            if(userNotificationDto.statusType().equals("DELETED")) list.add(new Response(userNotificationDto.email()
                    + " Здравствуйте! Ваш аккаунт был удален"));
        }
        return list;
    }

    public List<UserNotificationDto> getAllMessages(){
        List<UserNotificationDto> messages = new ArrayList<>();
        Properties props = createConsumerProperties();

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

    private Properties createConsumerProperties(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "temp-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, UserNotificationDto.class.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        return props;
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

    private void readAllMessages(KafkaConsumer<String, UserNotificationDto> consumer,List<UserNotificationDto> messages){
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
