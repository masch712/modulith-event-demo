package com.athenahealth.collector.colsubs.modulitheventdemo;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.modulith.ApplicationModuleListener;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Data
@Service
@RequiredArgsConstructor
public class EventHandlingService {
  CountDownLatch handlerBeginLatch = new CountDownLatch(1);
  CountDownLatch handlerProceedLatch = new CountDownLatch(1);
  AtomicBoolean shouldHandlerThrowRuntimeException = new AtomicBoolean(false);

  final FooRepository fooRepository;

  public UUID createFoo() {
    FooEntity savedFoo = fooRepository.save(new FooEntity(UUID.randomUUID(), null));
    return savedFoo.getId();
  }

  @ApplicationModuleListener
  public void handleFooEvent(FooEventRecord fooEventRecord) throws InterruptedException {
    // Signal test thread that we've entered the handler
    handlerBeginLatch.countDown();

    log.info("handling event for Foo id: " + fooEventRecord.id());

    // Wait for test thread to tell us to proceed
    handlerProceedLatch.await();

    if (shouldHandlerThrowRuntimeException.get()) {
      throw new RuntimeException("RuntimeException from the event handler!");
    }

    FooEntity foo = fooRepository.findById(fooEventRecord.id()).orElseThrow();
    foo.setHandledAt(Instant.now());

    log.info("Handler complete!");
  }
}
