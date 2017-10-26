package com.example.kafka.streams.example;

import com.example.kafka.streams.example.service.StreamService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ExampleApplication  implements CommandLineRunner {

	@Autowired
	private StreamService streamService;

	public static void main(String[] args) {
		SpringApplication.run(ExampleApplication.class, args);
	}

	@Override
	public void run(String... strings) throws Exception {
		streamService.serve();
	}
}
