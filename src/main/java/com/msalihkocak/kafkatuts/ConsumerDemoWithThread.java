package com.msalihkocak.kafkatuts;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread(){

    }

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String bootstrapServers = "localhost:9092";
        String groupId = "java-consumer-app-3";
        String topicToSubscribe = "first_topic";
        CountDownLatch latch = new CountDownLatch(1); // to deal with multiple threads

        logger.info("Creating the consumer runnable..");
        Runnable consumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topicToSubscribe, latch);

        // Starting the thread
        Thread thread = new Thread(consumerRunnable);
        thread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) consumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited.");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application got interrupted", e.getStackTrace());
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        public ConsumerRunnable(String bootstrapServers,
                                String groupId,
                                String topic,
                                CountDownLatch latch){
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            this.consumer = new KafkaConsumer<String, String>(properties);
            this.consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try{
                while(true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0
                    for(ConsumerRecord<String, String> record : records){
                        logger.info("Key: " + record.key() + " value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", offset: " + record.offset());
                    }
                }
            } catch (WakeupException e){
                logger.info("Received shutdown signal!");
            }finally {
                consumer.close();
                latch.countDown(); // tell our main code we're done with consumer
            }
        }

        public void shutdown(){
            //interrupt consumer.poll creates WkeUpException
            consumer.wakeup();
        }

    }
}
