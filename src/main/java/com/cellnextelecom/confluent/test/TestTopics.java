package com.cellnextelecom.confluent.test;

import com.cellnextelecom.confluent.*;


public class TestTopics {

	public static void main(String[] args) {
		ProducerConfluent producer = new ProducerConfluent();
		producer.producerKafka();
		ConsumerConfluent consumer = new ConsumerConfluent();
		consumer.consumKafka();
		
	}

}
