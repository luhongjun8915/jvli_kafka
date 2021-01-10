package com.jvli.project.utils;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaConfigUtil {

	/**
	 * 
	 * @return
	 * @Description: 获取生产者
	 * @Date: 2021年1月8日 下午4:25:47
	 * @Author: luhongjun
	 */
	public static Producer getKafkaProducer(String serverUrl) {
		// 生产消息
		Properties prop = new Properties();
		prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverUrl);
		prop.put(ProducerConfig.ACKS_CONFIG, "all");
		prop.put(ProducerConfig.RETRIES_CONFIG, 3); 
		prop.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		prop.put(ProducerConfig.LINGER_MS_CONFIG, 50);
		prop.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);
		prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<>(prop);
		return producer;
	}

	/**
	 * 
	 * @return 
	 * @Description: 获取消费者
	 * @Date: 2021年1月8日 下午4:26:09
	 * @Author: luhongjun
	 */
	public static KafkaConsumer getKafkaConsumer(String serverUrl,String groupId) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverUrl);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); 
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"); 
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000"); 
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); 
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		return consumer;
	}

}
