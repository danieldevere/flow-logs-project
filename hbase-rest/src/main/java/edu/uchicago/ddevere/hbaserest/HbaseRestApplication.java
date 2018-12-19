package edu.uchicago.ddevere.hbaserest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import edu.uchicago.ddevere.hbaserest.controller.TableController;

@SpringBootApplication
public class HbaseRestApplication {

	public static void main(String[] args) {
		SpringApplication.run(HbaseRestApplication.class, args);
		System.out.println(args[0]);
		TableController.timeHolder.put("lastBatch", Long.valueOf(args[0]));
	}
}
