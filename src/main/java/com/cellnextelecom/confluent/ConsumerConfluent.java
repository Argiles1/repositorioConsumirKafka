package com.cellnextelecom.confluent;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.io.*;
import java.nio.file.*;
import java.util.*;

import org.apache.kafka.clients.consumer.*;

public class ConsumerConfluent {
	
	public static String topic = "CAP_ES_PS_PRO";
	public static String config_file = "configuration.properties";
	
	public Properties loadConfig(){
		System.out.println("Path: "+Paths.get(config_file));
        if (!Files.exists(Paths.get(config_file))) {
        	System.out.println("Error llamando al configFile");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(config_file)) {
            cfg.load(inputStream);
        }
        catch(IOException e) {e.printStackTrace();}
        return cfg;
    }
	
	public List<String> consumKafka() {
		
		List<String> mensajesObtenidos = new ArrayList<String>();
		boolean salirbucle = true;
		// Load consumer configuration settings from a local file
	    // Reusing the loadConfig method from the ProducerExample class
	    Properties props = loadConfig();

	    // Add additional properties.
	    props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-cellnextelecom");
	    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	    
	    topic=props.getProperty("topic").toString().trim();
	    
		
	    try (final Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
        consumer.subscribe(Arrays.asList(topic));
        while (salirbucle) {
	        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
	        for (ConsumerRecord<String, String> record : records) {
	            String key = record.key();
	            String value = record.value();
	            mensajesObtenidos.add(value);
	            System.out.println(String.format("Consumed event from topic %s: key = %-10s value = %s", topic, key, value));
	        }
	        if (mensajesObtenidos.size()>0) salirbucle = false;        
        }   
        return mensajesObtenidos;
	   }
	    
	}
	
}
