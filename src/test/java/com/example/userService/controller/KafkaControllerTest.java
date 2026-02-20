package com.example.userService.controller;

import com.example.userService.dto.Response;
import com.example.userService.dto.UserNotificationDto;
import com.example.userService.service.NotificationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaControllerTest {

    @Mock
    private NotificationService notificationService;

    @InjectMocks
    private KafkaController kafkaController;

    private List<UserNotificationDto> testNotifications;
    private List<Response> testResponses;

    @BeforeEach
    void setUp() {
        // Подготовка тестовых данных
        testNotifications = Arrays.asList(
                new UserNotificationDto("user1", "user1@test.com", "CREATED"),
                new UserNotificationDto("user2", "user2@test.com", "DELETED"),
                new UserNotificationDto("user3", "user3@test.com", "UPDATED")
        );

        testResponses = Arrays.asList(
                new Response("user1@test.com Здравствуйте! Ваш аккаунт на сайте был успешно создан"),
                new Response("user2@test.com Здравствуйте! Ваш аккаунт был удален")
        );
    }

    @Test
    void getAll_ShouldReturnAllMessagesFromService() {
        // Arrange
        when(notificationService.getAllMessages()).thenReturn(testNotifications);

        // Act
        List<UserNotificationDto> result = kafkaController.getAll();

        // Assert
        assertNotNull(result);
        assertEquals(3, result.size());
        assertEquals("user1", result.get(0).userId());
        assertEquals("user1@test.com", result.get(0).email());
        assertEquals("CREATED", result.get(0).statusType());
        assertEquals("user2", result.get(1).userId());
        assertEquals("DELETED", result.get(1).statusType());
        assertEquals("user3", result.get(2).userId());
        assertEquals("UPDATED", result.get(2).statusType());

        // Verify
        verify(notificationService, times(1)).getAllMessages();
        verifyNoMoreInteractions(notificationService);
    }


    @Test
    void getAll_WhenServiceReturnsEmptyList_ShouldReturnEmptyList() {
        // Arrange
        when(notificationService.getAllMessages()).thenReturn(Collections.emptyList());

        // Act
        List<UserNotificationDto> result = kafkaController.getAll();

        // Assert
        assertNotNull(result);
        assertTrue(result.isEmpty());

        // Verify
        verify(notificationService, times(1)).getAllMessages();
    }

    @Test
    void getAll_WhenServiceReturnsNull_ShouldReturnNull() {
        // Arrange
        when(notificationService.getAllMessages()).thenReturn(null);

        // Act
        List<UserNotificationDto> result = kafkaController.getAll();

        // Assert
        assertNull(result);

        // Verify
        verify(notificationService, times(1)).getAllMessages();
    }

    @Test
    void sendMessagesAllUsers_ShouldReturnResponsesFromService() {
        // Arrange
        when(notificationService.sendUserMessage()).thenReturn(testResponses);

        // Act
        List<Response> result = kafkaController.sendMessagesAllUsers();

        // Assert
        assertNotNull(result);
        assertEquals(2, result.size());

        Response firstResponse = result.get(0);
        assertTrue(firstResponse.message().contains("user1@test.com"));
        assertTrue(firstResponse.message().contains("аккаунт на сайте был успешно создан"));

        Response secondResponse = result.get(1);
        assertTrue(secondResponse.message().contains("user2@test.com"));
        assertTrue(secondResponse.message().contains("аккаунт был удален"));

        // Verify
        verify(notificationService, times(1)).sendUserMessage();
        verifyNoMoreInteractions(notificationService);
    }
    @Test
    void sendMessagesAllUsers_WhenServiceReturnsEmptyList_ShouldReturnEmptyList() {
        // Arrange
        when(notificationService.sendUserMessage()).thenReturn(Collections.emptyList());

        // Act
        List<Response> result = kafkaController.sendMessagesAllUsers();

        // Assert
        assertNotNull(result);
        assertTrue(result.isEmpty());

        // Verify
        verify(notificationService, times(1)).sendUserMessage();
    }


}