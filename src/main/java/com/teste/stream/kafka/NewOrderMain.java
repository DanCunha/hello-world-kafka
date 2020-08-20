package com.teste.stream.kafka;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		for (int i = 0; i < 100; i++) {
			createProducer(i);	
			Thread.sleep(2000);
		}
	}

	public static void createProducer(int index) throws InterruptedException, ExecutionException {
		try(var dispatcher = new KafkaDispatcher()){
			var key = UUID.randomUUID().toString();
			var value = key + ",TICKET: " + index;
			dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

			var email = "Thank you for you order!";
			dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
		}

	}

}
