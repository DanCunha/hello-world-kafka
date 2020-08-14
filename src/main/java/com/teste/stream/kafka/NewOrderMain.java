package com.teste.stream.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		var producer = new KafkaProducer<String, String>(properties());
		var value = "321321,321344,PEDIJA";
		var record = new ProducerRecord<>("ECOMERCE_NEW_ORDER", "Daniel", value);
		var email = "Thank you for you order! sssss";
		var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", email, email);
		Callback callback = (data, ex) -> {
			if(ex != null) {
				ex.printStackTrace();
				return;
			}
			System.out.println("Sucesso " + data.topic() + " - Partition: " + data.partition() + "/ offset: " + data.offset() + "/ timestamp: " + data.timestamp());
		};
		producer.send(record, callback).get();
		producer.send(emailRecord, callback).get();
	}

	private static Properties properties() {
		var properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return properties;
	}

}
