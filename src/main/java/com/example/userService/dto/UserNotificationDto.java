package com.example.userService.dto;

public record UserNotificationDto(
        String userId,
        String email,
        String statusType
) {
}
