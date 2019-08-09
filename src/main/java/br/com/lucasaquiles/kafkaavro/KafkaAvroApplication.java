package br.com.lucasaquiles.kafkaavro;

import br.com.lucasaquiles.kafkaavro.entity.User;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaAvroApplication {

	public static void main(String[] args) {

		SpringApplication.run(KafkaAvroApplication.class, args);
	}

}
