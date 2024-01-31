package com.athenahealth.collector.colsubs.modulitheventdemo;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@Data
@RequiredArgsConstructor
public class EventPublishingService {
  final ApplicationEventPublisher eventPublisher;
  @Transactional
  public void doThePublish(UUID worldId) {
    eventPublisher.publishEvent(new FooEventRecord(worldId, "all your base are belong to us!"));
  }
}
