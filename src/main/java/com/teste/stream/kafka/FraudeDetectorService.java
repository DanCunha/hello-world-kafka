package com.teste.stream.kafka;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudeDetectorService {

	public static void main(String[] args) {
		var fraudService = new FraudeDetectorService();
		try(var service = new KafkaService<Order>(FraudeDetectorService.class.getSimpleName(),
				"ECOMMERCE_NEW_ORDER", 
				fraudService::parse,
				Order.class,
				Map.of() )){
			service.run();
		}
	}
	
	private void parse(ConsumerRecord<String, Order> record) {
		System.out.println("---------------------------------");
		System.out.println("Processing new order");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Order processed");
	}

}
