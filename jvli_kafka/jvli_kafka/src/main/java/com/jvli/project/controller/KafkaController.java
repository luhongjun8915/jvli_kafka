package com.jvli.project.controller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.tomcat.util.threads.TaskThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import com.jvli.project.config.MsgThread;
import com.jvli.project.response.BaseResponse;
import com.jvli.project.response.StatusCode;
import com.jvli.project.utils.KafkaConfigUtil;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/task")
public class KafkaController {

	private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);

	public static final String SERVER_URL = "localhost:9092";

	public static final String TOPIC_NAME = "group_cetc";

	@RequestMapping(value = "/send/msg", method = RequestMethod.POST)
	public BaseResponse sendMsg() {
		logger.info("KafkaController#发送消息>>>>>>>>>>>>>>>>>>>>>>>");
		BaseResponse response = new BaseResponse<>(StatusCode.SUCCESS);
		try {
			Producer<String, String> producer = KafkaConfigUtil.getKafkaProducer(SERVER_URL);

			producer.send(new ProducerRecord<String, String>(TOPIC_NAME, "hello world"), new Callback() {

				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception != null) {
						logger.error("KafakController#发送消息回调异常：{}", exception);
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

	@RequestMapping(value = "/consume/msg_01", method = RequestMethod.GET)
	public BaseResponse consumeMsg_01() {
		Object message = null;
		boolean flag = true;
		ExecutorService executors = null;
		KafkaConsumer<String, String> consumer = null;
		try {
			String groupId = "GROUP01_" + TOPIC_NAME;

			consumer = KafkaConfigUtil.getKafkaConsumer(SERVER_URL, groupId);
	        consumer.subscribe(Arrays.asList(TOPIC_NAME));
			
	        executors = new ThreadPoolExecutor(5, 7, 0L, TimeUnit.MILLISECONDS,
	                new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
	        //executors = Executors.newFixedThreadPool(5);
	        while (true) {
	            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
	            for (final ConsumerRecord record : records) {
	                Future<Boolean> future = executors.submit(new MsgThread(record));
	            }
	        }
	        
		} catch (Exception e) {
			logger.error("KafkaController#消费消息异常：{}", e);
			
		} finally {
			if (consumer != null) {
	            consumer.close();
	        }
	        if (executors != null) {
	            executors.shutdown();
	        }
	        try {
	            if (!executors.awaitTermination(10, TimeUnit.SECONDS)) {
	                System.out.println("Timeout.... Ignore for this case");
	            }
	        } catch (InterruptedException ignored) {
	            System.out.println("Other thread interrupted this shutdown, ignore for this case.");
	            Thread.currentThread().interrupt();
	        }
		}
		return new BaseResponse<>(StatusCode.SUCCESS);
	}

	@RequestMapping(value = "/consume/msg_02", method = RequestMethod.GET)
	public BaseResponse consumeMsg_02() {
		BaseResponse response = null;
		Object message = null;
		boolean flag = true;
		try {
			String groupId = "GROUP02_" + TOPIC_NAME;

			KafkaConsumer<String, String> consumer = KafkaConfigUtil.getKafkaConsumer(SERVER_URL, groupId);
			consumer.subscribe(Arrays.asList(TOPIC_NAME));

			while (flag) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
				if (!records.isEmpty() && records.count() > 0) {
					for (ConsumerRecord<String, String> record : records) {
						System.out.printf("offset = %d, key = %s, value = %s" + "\n", record.offset(), record.key(),
								record.value());
						Optional<?> clientMsg = Optional.ofNullable(record.value());
						if (clientMsg.isPresent()) {
							message = clientMsg.get();
							consumer.commitSync();
							logger.info("KafkaController#consumeMsg_02消费者接受消息：{}", message);
						}

					}
				} else {
					Thread.sleep(10000);
					logger.info("KafkaController#consumeMsg_02没有可消费的数据，休眠10秒执行");
				}
			}
			consumer.close();
			response = new BaseResponse<>(StatusCode.SUCCESS, message);
		} catch (Exception e) {
			logger.error("KafkaController#消费消息异常：{}", e);
			response = new BaseResponse<>(StatusCode.FAIL);
		}
		return response;
	}

}
