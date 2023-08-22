package com.cellnextelecom.confluent;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerConfluent {
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
	
	public List<String> producerKafka() {
		
		List<String> mensajesObtenidos = new ArrayList<String>();
		// Load consumer configuration settings from a local file
	    // Reusing the loadConfig method from the ProducerExample class
	    Properties props = loadConfig();

	    // Add additional properties.
	    // props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-java-getting-started");
	    // props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	    
	    topic=props.getProperty("topic").toString().trim();
	    String[] users = {"eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther"};
        String[] items = {"book", "alarm clock", "t-shirts", "gift card", "batteries"};
        try (final Producer<String, String> producer = new KafkaProducer<>(props)) {
            final Random rnd = new Random();
            final Long numMessages = 10L;
            for (Long i = 0L; i < numMessages; i++) {
                String user = users[rnd.nextInt(users.length)];
                String item = items[rnd.nextInt(items.length)];

                producer.send(
                       new ProducerRecord<>(topic, user, item),
                        (event, ex) -> {
                            if (ex != null)
                                ex.printStackTrace();
                            else
                                System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topic, user, item);
                        });
            }
            System.out.printf("%s events were produced to topic %s%n", numMessages, topic);
        }
       return new ArrayList<String>(); 
	}
		
}
