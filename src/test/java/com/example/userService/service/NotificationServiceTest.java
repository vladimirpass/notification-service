package com.example.userService.service;

import com.example.userService.dto.Response;
import com.example.userService.dto.UserNotificationDto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;


import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class NotificationServiceTest {

    private NotificationService notificationService;

    private List<UserNotificationDto> testNotifications;

    @BeforeEach
    void setUp() {
        notificationService = new NotificationService();

        // Подготовка тестовых данных
        testNotifications = Arrays.asList(
                new UserNotificationDto("user1", "user1@test.com", "CREATED"),
                new UserNotificationDto("user2", "user2@test.com", "DELETED"),
                new UserNotificationDto("user3", "user3@test.com", "UPDATED")
        );
    }

    @Test
    void getAllMessages_SuccessfullyReadsAllMessages() {
        // Создаем мок для PartitionInfo
        org.apache.kafka.common.PartitionInfo mockPartitionInfo =
                mock(org.apache.kafka.common.PartitionInfo.class);
        when(mockPartitionInfo.partition()).thenReturn(0);

        List<org.apache.kafka.common.PartitionInfo> partitionInfos =
                Collections.singletonList(mockPartitionInfo);

        // Создаем тестовые записи
        List<ConsumerRecord<String, UserNotificationDto>> records = Arrays.asList(
                new ConsumerRecord<>("users", 0, 0, "key1", testNotifications.get(0)),
                new ConsumerRecord<>("users", 0, 1, "key2", testNotifications.get(1)),
                new ConsumerRecord<>("users", 0, 2, "key3", testNotifications.get(2))
        );

        // Создаем records с данными
        Map<TopicPartition, List<ConsumerRecord<String, UserNotificationDto>>> recordMap = new HashMap<>();
        TopicPartition tp = new TopicPartition("users", 0);
        recordMap.put(tp, records);

        ConsumerRecords<String, UserNotificationDto> mockRecords = new ConsumerRecords<>(recordMap);

        try (MockedConstruction<KafkaConsumer> mocked = mockConstruction(
                KafkaConsumer.class,
                (mock, context) -> {
                    // Настраиваем поведение для всех вызовов методов
                    when(mock.partitionsFor("users")).thenReturn(partitionInfos);
                    when(mock.poll(any(Duration.class)))
                            .thenReturn(mockRecords)
                            .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));
                })) {

            // Вызываем тестируемый метод
            List<UserNotificationDto> result = notificationService.getAllMessages();

            // Проверяем результаты
            assertNotNull(result);
            assertEquals(3, result.size());
            assertEquals("user1", result.get(0).userId());
            assertEquals("CREATED", result.get(0).statusType());
            assertEquals("user2@test.com", result.get(1).email());
        }
    }

    @Test
    void getAllMessages_WhenNoPartitions_ReturnsEmptyList() {
        try (MockedConstruction<KafkaConsumer> mocked = mockConstruction(
                KafkaConsumer.class,
                (mock, context) -> {
                    when(mock.partitionsFor("users")).thenReturn(Collections.emptyList());
                })) {

            List<UserNotificationDto> result = notificationService.getAllMessages();

            assertNotNull(result);
            assertTrue(result.isEmpty());

            // Проверяем, что assign и seekToBeginning не вызывались
            KafkaConsumer<?, ?> constructedMock = mocked.constructed().get(0);
            verify(constructedMock, never()).assign(any());
            verify(constructedMock, never()).seekToBeginning(any());
        }
    }

    @Test
    void getAllMessages_WhenDeserializationError_SkipsErrorBatch() {
        // Создаем мок для PartitionInfo
        org.apache.kafka.common.PartitionInfo mockPartitionInfo =
                mock(org.apache.kafka.common.PartitionInfo.class);
        when(mockPartitionInfo.partition()).thenReturn(0);

        List<org.apache.kafka.common.PartitionInfo> partitionInfos =
                Collections.singletonList(mockPartitionInfo);

        // Создаем валидные записи
        List<ConsumerRecord<String, UserNotificationDto>> validRecords = Collections.singletonList(
                new ConsumerRecord<>("users", 0, 2, "key3", testNotifications.get(2))
        );

        Map<TopicPartition, List<ConsumerRecord<String, UserNotificationDto>>> validRecordMap = new HashMap<>();
        TopicPartition tp = new TopicPartition("users", 0);
        validRecordMap.put(tp, validRecords);

        ConsumerRecords<String, UserNotificationDto> validMockRecords =
                new ConsumerRecords<>(validRecordMap);

        TopicPartition testPartition = new TopicPartition("users", 0);

        try (MockedConstruction<KafkaConsumer> mocked = mockConstruction(
                KafkaConsumer.class,
                (mock, context) -> {
                    when(mock.partitionsFor("users")).thenReturn(partitionInfos);

                    // Первый poll выбрасывает исключение десериализации
                    when(mock.poll(any(Duration.class)))
                            .thenThrow(new RecordDeserializationException(testPartition, 1, "Deserialization error", new Exception()))
                            .thenReturn(validMockRecords)
                            .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

                    when(mock.assignment()).thenReturn(Collections.singleton(testPartition));
                    when(mock.position(testPartition)).thenReturn(1L);
                })) {

            List<UserNotificationDto> result = notificationService.getAllMessages();

            assertNotNull(result);
            assertEquals(1, result.size());
            assertEquals("user3", result.get(0).userId());

            // Verify that seek was called to skip the error record
            KafkaConsumer<?, ?> constructedMock = mocked.constructed().get(0);
            verify(constructedMock, times(1)).seek(eq(testPartition), eq(2L));
        }
    }

    @Test
    void sendUserMessage_CreatesCorrectResponses() {
        // Создаем шпион для NotificationService
        NotificationService spyService = spy(notificationService);

        // Мокаем getAllMessages для возврата тестовых данных
        doReturn(testNotifications).when(spyService).getAllMessages();

        // Вызываем тестируемый метод
        List<Response> results = spyService.sendUserMessage();

        // Проверяем результаты
        assertNotNull(results);
        assertEquals(2, results.size()); // Только CREATED и DELETED

        // Проверяем сообщение для CREATED статуса
        assertTrue(results.stream()
                .anyMatch(r -> r.message().contains("user1@test.com") &&
                        r.message().contains("Здравствуйте! Ваш аккаунт на сайте был успешно создан")));

        // Проверяем сообщение для DELETED статуса
        assertTrue(results.stream()
                .anyMatch(r -> r.message().contains("user2@test.com") &&
                        r.message().contains("Здравствуйте! Ваш аккаунт был удален")));

        // Проверяем, что UPDATED статус не создал сообщение
        assertFalse(results.stream()
                .anyMatch(r -> r.message().contains("user3@test.com")));
    }


}
