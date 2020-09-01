package com.teste.stream.kafka;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		for (int i = 0; i < 20; i++) {
			createProducer(i);	
			Thread.sleep(2000);
		}
	}

	public static void createProducer(int index) throws InterruptedException, ExecutionException {
		try(var orderDispatcher = new KafkaDispatcher<Order>()){
			try(var emailDispatcher = new KafkaDispatcher<Email>()){
				var userId = UUID.randomUUID().toString();
				var orderId = UUID.randomUUID().toString();
				var amount = new BigDecimal(Math.random() * 5000 + 1);
				var order = new Order(userId, orderId, amount);
				orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);

				var email = new Email("Thank you for you order!", "body");
				emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);
			}
		}
	}

}
