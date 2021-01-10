package com.jvli.project;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class JvliKafkaApplication {
	
	private static final Logger logger = LoggerFactory.getLogger(JvliKafkaApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(JvliKafkaApplication.class, args);
		logger.info("<<<<<<<<<<<<<<<<Hello world,keep coding>>>>>>>>>>>>>>>>");
		logger.info("<<<<<<<<<<<<<<<<星光不问赶路人>>>>>>>>>>>>>>>>");
	}

}
