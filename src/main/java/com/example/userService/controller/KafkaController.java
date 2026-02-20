package com.example.userService.controller;

import com.example.userService.dto.Response;
import com.example.userService.dto.UserNotificationDto;
import com.example.userService.service.NotificationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

    private final NotificationService notificationService;

    public KafkaController(NotificationService notificationService){
        this.notificationService = notificationService;
    }

    @GetMapping("/messages")
    public List<UserNotificationDto> getAll(){
        return notificationService.getAllMessages();
    }

    @GetMapping("/sendMessagesUsers")
    public List<Response> sendMessagesAllUsers(){
        return notificationService.sendUserMessage();
    }


}
