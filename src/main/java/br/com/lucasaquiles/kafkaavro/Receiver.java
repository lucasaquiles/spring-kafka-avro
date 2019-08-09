package br.com.lucasaquiles.kafkaavro;

import avro.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.concurrent.CountDownLatch;

@Slf4j
public class Receiver {


    private CountDownLatch latch = new CountDownLatch(1);

    public CountDownLatch getLatch() {
        return latch;
    }

    @KafkaListener(topics = "${kafka.topic.avro}")
    public void receive(User user) {
        log.info("received user='{}'", user);
        latch.countDown();
    }
}
