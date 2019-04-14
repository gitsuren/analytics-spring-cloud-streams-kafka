package com.example.analytics.controller;

import com.example.analytics.binding.AnalyticsBinding;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.cloud.stream.binder.kafka.streams.QueryableStoreRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequiredArgsConstructor
public class CountRestController {

  private final QueryableStoreRegistry registry;

  @GetMapping("/counts")
  public Map<String, Long> counts() {
    Map<String, Long> counts = new HashMap<>();
    ReadOnlyKeyValueStore<String, Long> querableStoryType =
        this.registry.getQueryableStoreType(AnalyticsBinding.PAGE_COUNT_MV, QueryableStoreTypes.keyValueStore());
    KeyValueIterator<String, Long> all = querableStoryType.all();
    while (all.hasNext()) {
      KeyValue<String, Long> value = all.next();
      counts.put(value.key, value.value);
    }
    return counts;
  }
}
