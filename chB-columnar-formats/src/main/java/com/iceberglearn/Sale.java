package com.iceberglearn;

public record Sale(
        long orderId,
        String region,
        String status,
        int quantity,
        long amountCents,
        String customerEmail) {
}
