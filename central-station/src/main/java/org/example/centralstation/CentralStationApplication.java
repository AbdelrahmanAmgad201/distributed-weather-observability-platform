package org.example.centralstation;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class CentralStationApplication {

    public static void main(String[] args) {
        SpringApplication.run(CentralStationApplication.class, args);
    }
}