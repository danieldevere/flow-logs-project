package edu.uchicago.ddevere.networkflow;

import java.io.IOException;
import java.time.Clock;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
//@EnableWebMvc
public class NetworkFlowApplication {
	
//	@Bean
//	public WebMvcConfigurerAdapter mvcConfig() {
//		
//		return new WebMvcConfigurerAdapter() {
//			
//			@Override
//			public void addViewControllers(ViewControllerRegistry registry) {
//				registry.addViewController("/").setViewName("index");
//			}
//			
//			@Override
//			public void addResourceHandlers(ResourceHandlerRegistry registry) {
//				registry.addResourceHandler("/**").addResourceLocations("/templates	");
//			}
//		};
//	}
	
	@Bean
	public Clock clock() {
		return Clock.systemUTC();
	}
	
	@Bean
	public RestTemplate restTemplate() {
		RestTemplate restTemplate = new RestTemplate();
		restTemplate.setErrorHandler(new ResponseErrorHandler() {
			
			@Override
			public boolean hasError(ClientHttpResponse response) throws IOException {
				// TODO Auto-generated method stub
				return false;
			}
			
			@Override
			public void handleError(ClientHttpResponse response) throws IOException {
				// TODO Auto-generated method stub
				
			}
		});
		return restTemplate;
	}

	public static void main(String[] args) {
		SpringApplication.run(NetworkFlowApplication.class, args);
	}
}
