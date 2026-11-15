package io.github.xoage.orders.web;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record CreateOrderRequest(
        @NotBlank(message = "напиток не указан")
        @Size(max = 50, message = "название напитка до 50 символов")
        String drink,

        @NotBlank(message = "имя клиента не указано")
        @Size(max = 50, message = "имя клиента до 50 символов")
        String customerName
) {
}
