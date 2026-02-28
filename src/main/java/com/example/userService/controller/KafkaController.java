package com.example.userService.controller;

import com.example.userService.dto.Response;
import com.example.userService.dto.UserNotificationDto;
import com.example.userService.service.MessageProcessor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

    private final MessageProcessor messageProcessor;

    public KafkaController(MessageProcessor messageProcessor){
        this.messageProcessor = messageProcessor;
    }

    @GetMapping("/messages")
    public List<UserNotificationDto> getAll(){
        return messageProcessor.getAllMessages();
    }

    @GetMapping("/sendMessagesUsers")
    public List<Response> sendMessagesAllUsers(){
        return messageProcessor.sendUserMessage();
    }


}
