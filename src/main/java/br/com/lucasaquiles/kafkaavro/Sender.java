package br.com.lucasaquiles.kafkaavro;

import br.com.lucasaquiles.kafkaavro.entity.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

@Slf4j
public class Sender {

    @Value("${kafka.topic.avro}")
    private String avroTopic;

    @Autowired
    private KafkaTemplate<String, User> kafkaTemplate;

    public void send(User user) {

        log.info("sending user={}", user);
        kafkaTemplate.send(avroTopic, user);
    }
}
