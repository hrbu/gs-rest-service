package com.example.restservice.changeprocessing;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * PoC Implementation of a Kafka Consumer with Spring Boot
 *
 * Resources / Cheatsheets:
 * 1. https://reflectoring.io/spring-boot-kafka/
 * 2. https://docs.spring.io/spring-kafka/docs/2.7.x/reference/html/#reference
 */

@Service
@KafkaListener(
    id = "poc-spring-consumer2",
    topics = "oracle.poc1.all-changes",
    properties = { "auto.offset.reset : latest", "auto.commit.interval.ms : 3000"}

)
public class OracleConsumer {
    private static final Logger log = LoggerFactory.getLogger(OracleConsumer.class);

    public OracleConsumer(){
        System.out.println("################ OracleConsumer:constructor #########");
    }




    @KafkaHandler
    public void onReceiving(Object workUnit, @Header(KafkaHeaders.OFFSET) Integer offset,
                            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Processing topic = {}, partition = {}, offset = {}, workUnit = {}",
                topic, partition, offset, workUnit);
        // System.out.println(workUnit);
    }
}
