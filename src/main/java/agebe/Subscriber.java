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

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Subscriber implements Consumer<Message> {

  private static final Logger log = LoggerFactory.getLogger(Subscriber.class);

  private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
    return new Thread(r, "scheduler");
  });

  private Map<Instant, TimeoutSet<Set<Message>, Message>> sets = new HashMap<>();

  @Override
  public synchronized void accept(Message m) {
    log.info("received '{}'", m);
    TimeoutSet<Set<Message>, Message> set = sets.computeIfAbsent(
        m.timestamp(),
        this::newTimeoutSet);
    try {
      set.add(m);
    } catch(Exception e) {
      log.error("failed to add message '{}' to timeout set", m, e);
    }
  }

  private TimeoutSet<Set<Message>, Message> newTimeoutSet(Instant key) {
    TimeoutSet<Set<Message>, Message> ts = TimeoutSet.ofCollection(
        Duration.ofMillis(1500),
        null,
        this::isSetComplete,
        new HashSet<>(),
        scheduler);
    ts.getFuture().whenCompleteAsync((r,e) -> process(r,e,key), ForkJoinPool.commonPool());
    return ts;
  }

  private boolean isSetComplete(Set<Message> set) {
    Set<String> completeSet = Set.of("1", "2", "3");
    return set.stream().map(Message::id).collect(Collectors.toSet()).equals(completeSet);
  }

  private void process(Set<Message> messages, Throwable t, Instant key) {
    if(t != null) {
      log.error("timeout set completed with exception", t);
    } else {
      log.info("processing set '{}'", messages);
    }
    scheduler.schedule(() -> cleanup(key), 9, TimeUnit.SECONDS);
  }

  private synchronized void cleanup(Instant key) {
    if(sets.remove(key) != null) {
      log.info("cleanup, removed '{}'", key);
    } else {
      log.warn("cleanup, expected key '{}' but was not present in sets map", key);
    }
  }

}
