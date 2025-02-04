package com.example.analytics;

import com.example.analytics.binding.AnalyticsBinding;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;

@SpringBootApplication
@EnableBinding(AnalyticsBinding.class)
public class AnalyticsApplication {

  public static void main(String[] args) {
    SpringApplication.run(AnalyticsApplication.class, args);
  }

}
