/*
 * Copyright 2024 Andre Gebers
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package agebe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

public class TimeoutSetTest {

  private static record Msg(int id) {};

  private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  private void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new RuntimeException("interruped", e);
    }
  }

  @Test
  void test() throws InterruptedException, ExecutionException, TimeoutException {
    TimeoutSet<Msg> ts = new TimeoutSet<>(Duration.ofSeconds(1), scheduler);
    ts.add(new Msg(1));
    Instant start = Instant.now();
    Set<Msg> set = ts.getFuture().get(10, TimeUnit.SECONDS);
    Instant end = Instant.now();
    assertTrue(end.toEpochMilli() - start.toEpochMilli() >= 1000);
    assertEquals(1, set.size());
    assertThrows(Exception.class, () -> ts.add(new Msg(2)));
  }

  @Test
  void test2() throws InterruptedException, ExecutionException, TimeoutException {
    TimeoutSet<Msg> ts = new TimeoutSet<>(Duration.ofSeconds(1), scheduler);
    Instant start = Instant.now();
    ts.add(new Msg(1));
    sleep(500);
    ts.add(new Msg(2));
    sleep(500);
    ts.add(new Msg(3));
    sleep(500);
    ts.add(new Msg(4));
    Set<Msg> set = ts.getFuture().get(10, TimeUnit.SECONDS);
    Instant end = Instant.now();
    assertTrue(end.toEpochMilli() - start.toEpochMilli() >= 2500);
    assertEquals(4, set.size());
  }

  @Test
  void afterCreationTest() throws InterruptedException, ExecutionException, TimeoutException {
    Instant start = Instant.now();
    TimeoutSet<Msg> ts = new TimeoutSet<>(null, Duration.ofSeconds(1), null, null, scheduler);
    Set<Msg> set = ts.getFuture().get(10, TimeUnit.SECONDS);
    Instant end = Instant.now();
    assertTrue(end.toEpochMilli() - start.toEpochMilli() >= 1000);
    assertEquals(0, set.size());
  }

  @Test
  void withSetCompletePredicateTest() throws InterruptedException, ExecutionException, TimeoutException {
    Instant start = Instant.now();
    TimeoutSet<Msg> ts = new TimeoutSet<>(null, null, s -> s.size() == 1, null, scheduler);
    ts.add(new Msg(1));
    Set<Msg> set = ts.getFuture().get(10, TimeUnit.SECONDS);
    Instant end = Instant.now();
    assertTrue(end.toEpochMilli() - start.toEpochMilli() < 1000);
    assertEquals(1, set.size());
  }

}
