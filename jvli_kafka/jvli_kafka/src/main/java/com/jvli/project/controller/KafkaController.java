package com.jvli.project.controller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.jvli.project.response.BaseResponse;
import com.jvli.project.response.StatusCode;
import com.jvli.project.utils.KafkaConfigUtil;
import java.time.Duration;
import java.util.*;

@RestController
@RequestMapping("/task")
public class KafkaController {

	private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);

	public static final String SERVER_URL = "localhost:9092";

	public static final String TOPIC_NAME = "group_cetc";

	@RequestMapping(value = "/send/msg", method = RequestMethod.POST)
	public BaseResponse sendMsg() {
		BaseResponse response = new BaseResponse<>(StatusCode.SUCCESS);
		try {
			Producer<String, String> producer = KafkaConfigUtil.getKafkaProducer(SERVER_URL);

			producer.send(new ProducerRecord<String, String>(TOPIC_NAME, "hello world"),new Callback() {
				
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if(exception != null) {
						logger.error("KafakController#发送消息回调异常：{}",exception);
					}
					
				}
			});
			producer.close();
		} catch (Exception e) {
			logger.error("KafkaController#发送消息异常：{}", e.fillInStackTrace());
			response = new BaseResponse<>(StatusCode.FAIL);
		}
		return response;
	}

	@RequestMapping(value = "/consume/msg", method = RequestMethod.GET)
	public BaseResponse consumeMsg() {

		BaseResponse response = null;
		Object message = null;
		try {
			String groupId = "GROUP_" + TOPIC_NAME;
			
			KafkaConsumer<String, String> consumer = KafkaConfigUtil.getKafkaConsumer(SERVER_URL, groupId);
			consumer.subscribe(Arrays.asList(TOPIC_NAME));
			
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
		
			if (!records.isEmpty() && records.count() > 0) {
				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("offset = %d, key = %s, value = %s" + "\n", record.offset(), record.key(),
							record.value());
					Optional<?> clientMsg = Optional.ofNullable(record.value());
					if (clientMsg.isPresent()) {
						message = clientMsg.get();
						consumer.commitSync();
						logger.info("消费者接受消息：{}",message);
					}

				}
			}
			consumer.close();
			response = new BaseResponse<>(StatusCode.SUCCESS,message);
		} catch (Exception e) {
			logger.error("KafkaController#消费消息异常：{}",e);
			response = new BaseResponse<>(StatusCode.FAIL);
		}
		return response;
	}
}
