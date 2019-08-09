package br.com.lucasaquiles.kafkaavro.entity;


import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class User {

    private Long id;
    private String email;
    private String fullName;
}
