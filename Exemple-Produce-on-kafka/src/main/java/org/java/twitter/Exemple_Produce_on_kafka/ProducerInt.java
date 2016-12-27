package org.java.twitter.Exemple_Produce_on_kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public  class ProducerInt {
	KafkaProducer<String, String> producer = null;
	
	public KafkaProducer<String, String>  configureProducer(String listbrokers) {
		 Properties props = new Properties();
		    props.put("bootstrap.servers", "latitude:6667");
		    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		    producer = new KafkaProducer<String, String>(props);
		    System.out.println("Producer was created");	
		    return producer ;
	}
	/**
	 * 
	 * This method send data to the broker on the topic
	 *   
	 * @param topicName   
	 *           The name of the topic on kafka
	 * @param key      
	 *           The key of one message streamed
	 * @param value        
	 *           The value of one message streamed
	 * 
	 * */
	public void sendData(String topicName , String key ,String value)
	{	
		producer.send(new ProducerRecord<String, String>(topicName, key, value));	}
	
	public void closeProducteur()
	{
		producer.close();
		
	}

}
