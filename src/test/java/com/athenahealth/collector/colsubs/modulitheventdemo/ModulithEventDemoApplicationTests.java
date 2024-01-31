package com.athenahealth.collector.colsubs.modulitheventdemo;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.modulith.events.core.EventPublication;
import org.springframework.modulith.events.core.EventPublicationRegistry;
import org.springframework.modulith.events.core.EventPublicationRepository;
import org.springframework.test.jdbc.JdbcTestUtils;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

@SpringBootTest
@Testcontainers
@Slf4j
class ModulithEventDemoApplicationTests {
  @Autowired
  JdbcTemplate jdbcTemplate;
  @Autowired
  TransactionTemplate transactionTemplate;
  @Autowired
  private ApplicationEventPublisher eventPublisher;
  @Autowired
  EventHandlingService eventHandlingService;
  @Autowired
  EventPublishingService eventPublishingService;
  @Autowired
  FooRepository fooRepository;
  @Autowired
  EventPublicationRepository eventPublicationRepository;
  @SpyBean
  EventPublicationRegistry eventPublicationRegistry;

  @Container
  @ServiceConnection
  private static final PostgreSQLContainer<?> postgresContainer = new PostgreSQLContainer<>("postgres:latest")
      .withDatabaseName("testdb")
      .withUsername("testuser")
      .withPassword("testpass");

  @BeforeAll
  static void beforeAll() {
    postgresContainer.start();
  }

  @AfterAll
  static void afterAll() {
    postgresContainer.stop();
  }

  @BeforeEach
  public void beforeEach() {
    JdbcTestUtils.deleteFromTables(jdbcTemplate, "foo_entity", "event_publication");

    this.eventHandlingService.setHandlerProceedLatch(new CountDownLatch(1));
    this.eventHandlingService.setHandlerBeginLatch(new CountDownLatch(1));
    this.eventHandlingService.getShouldHandlerThrowRuntimeException().set(false);
  }

  @Test
  void contextLoads() {
    assertThat(eventPublisher).isNotNull();
  }

  /**
   * Sanity check: Spawn a child thread that does some DML in a transaction;
   * keep that transaction open and run DML in another transaction in the main thread;
   * assert that the two threads use different postgres transaction IDs, because of course they do.
   */
  @Test
  void taskThreadUsesDistinctDatabaseSession() throws ExecutionException, InterruptedException {
    ExecutorService executorService = newFixedThreadPool(1);

    CountDownLatch taskTransactionContinueLatch = new CountDownLatch(1);
    Callable<String> task = () -> transactionTemplate.execute(
        status -> {
          status.setRollbackOnly();
          jdbcTemplate.update("insert into foo_entity (id) values (?)", UUID.randomUUID());

          // Keep the transaction open until main thread says to continue
          try {
            taskTransactionContinueLatch.await();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          ;
          return jdbcTemplate.queryForObject("SELECT pg_current_xact_id_if_assigned()", String.class);
        });

    Future<String> future = executorService.submit(task);

    String mainTransactionId = transactionTemplate.execute(
        status -> {
          status.setRollbackOnly();
          jdbcTemplate.update("insert into foo_entity (id) values (?)", UUID.randomUUID());
          return jdbcTemplate.queryForObject("SELECT pg_current_xact_id_if_assigned()", String.class);
        });

    taskTransactionContinueLatch.countDown();
    String taskTransactionId = future.get();

    assertThat(taskTransactionId).isNotEmpty();
    assertThat(mainTransactionId).isNotEmpty();
    assertThat(mainTransactionId).isNotEqualTo(taskTransactionId);
  }

  @Test
  void canCreateFoo() {
    UUID newFooId = eventHandlingService.createFoo();
    fooRepository.flush();
    fooRepository.findById(newFooId);
  }

  @Test
  void eventPublicationRowCommitsBeforeListenerHasStarted() throws InterruptedException, ExecutionException {
    UUID newFooId = eventHandlingService.createFoo();
    ExecutorService executorService = newFixedThreadPool(1);

    Runnable task = () -> eventPublishingService.doThePublish(newFooId);

    // TEST
    Future<?> future = executorService.submit(task);

    eventHandlingService.getHandlerBeginLatch().await();

    List<EventPublication> incompletePublicationsDuringEventHandling = eventPublicationRepository.findIncompletePublications();

    eventHandlingService.getHandlerProceedLatch().countDown();
    future.get();
    executorService.shutdown();
    assertThat(executorService.awaitTermination(5, TimeUnit.SECONDS)).isTrue().describedAs("executor service finished before timeout");

    assertThat(incompletePublicationsDuringEventHandling).hasSize(1);
    assertThat(incompletePublicationsDuringEventHandling.get(0).getEvent()).isInstanceOf(FooEventRecord.class);
    FooEventRecord eventPublication = (FooEventRecord) incompletePublicationsDuringEventHandling.get(0).getEvent();
    assertThat(eventPublication.id()).isEqualTo(newFooId);
  }

  /**
   * Publish an event (and let its handler run) from a new thread pool;
   * Spy on EventPublicationRegistry, forcing markCompleted to pause while we query the database to see that the event handler has committed
   */
  @Test
  void eventPublicationHandlerCommitsBeforeEventPublicationCompletionCommits() throws InterruptedException, ExecutionException {
    UUID newFooId = eventHandlingService.createFoo();

    // Don't allow markCompleted to proceed until this main thread tells it to
    CountDownLatch markCompletedBeginLatch = new CountDownLatch(1);
    CountDownLatch markCompletedProceedLatch = new CountDownLatch(1);
    doAnswer(invocation -> {
      markCompletedBeginLatch.countDown();
      markCompletedProceedLatch.await();
      invocation.callRealMethod();
      return null;
    }).when(eventPublicationRegistry).markCompleted(any(), any());

    ExecutorService executorService = newFixedThreadPool(1);

    Runnable task = () -> eventPublishingService.doThePublish(newFooId);

    // TEST
    Future<?> future = executorService.submit(task);

    eventHandlingService.getHandlerBeginLatch().await();
    eventHandlingService.getHandlerProceedLatch().countDown();

    future.get();
    executorService.shutdown();
    assertThat(executorService.awaitTermination(5, TimeUnit.SECONDS)).isTrue().describedAs("executor service finished before timeout");

    // Wait until just before we call markCompleted (i.e. until the event handler is complete)
    markCompletedBeginLatch.await();
    FooEntity fooEntity = fooRepository.findById(newFooId).orElseThrow();
    assertThat(fooEntity.getHandledAt()).isNotNull().describedAs("event handler committed");

    String handledAt = jdbcTemplate.queryForObject("select handled_at from foo_entity where id = ?", String.class, newFooId);
    assertThat(handledAt).isNotNull()
        .describedAs("event handler committed; confirmed via raw SQL, just to be absolutely sure");

    List<EventPublication> incompletePublicationsDuringEventHandling = eventPublicationRepository.findIncompletePublications();
    assertThat(incompletePublicationsDuringEventHandling).hasSize(1);
    assertThat(incompletePublicationsDuringEventHandling.get(0).getEvent()).isInstanceOf(FooEventRecord.class);
    FooEventRecord eventPublication = (FooEventRecord) incompletePublicationsDuringEventHandling.get(0).getEvent();
    assertThat(eventPublication.id()).isEqualTo(newFooId)
        .describedAs("event_publication is still incomplete (because we haven't counted down the latch yet)");

    // allow markCompleted to proceed
    markCompletedProceedLatch.countDown();
  }

  @Test
  void eventPublicationRowCommitsEvenIfListenerThrows() throws InterruptedException {
    eventHandlingService.getShouldHandlerThrowRuntimeException().set(true);
    UUID newWorldId = eventHandlingService.createFoo();
    ExecutorService executorService = newFixedThreadPool(2);

    Runnable publisher = () -> eventPublishingService.doThePublish(newWorldId);

    // TEST
    Future aaronFuture = executorService.submit(publisher);

    eventHandlingService.getHandlerBeginLatch().await();

    List<EventPublication> incompletePublications = eventPublicationRepository.findIncompletePublications();

    eventHandlingService.getHandlerProceedLatch().countDown();

    executorService.shutdown();
    assertThat(executorService.awaitTermination(5, TimeUnit.SECONDS)).isTrue().describedAs("executor service finished before timeout");
    assertThat(incompletePublications).hasSize(1);
    assertThat(incompletePublications.get(0).getEvent()).isInstanceOf(FooEventRecord.class);
    FooEventRecord eventPublication = (FooEventRecord) incompletePublications.get(0).getEvent();
    assertThat(eventPublication.id()).isEqualTo(newWorldId);

    assertThat(eventPublicationRepository.findIncompletePublications()).hasSize(1).describedAs("publication is still incomplete");
  }


}
