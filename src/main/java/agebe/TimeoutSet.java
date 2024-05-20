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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * TimeoutSet can be used to collect a set of messages that belong together e.g. from Kafka and
 * start processing after the set is complete or after a defined timeout
 * @param <T>
 */
public class TimeoutSet<T> {

  private final Duration afterLastChangeTimeout;

  private final Duration afterCreationTimeout;

  private final Predicate<Set<T>> isSetCompletePredicate;

  private final Set<T> set;

  private final Instant created = Instant.now();

  private final CompletableFuture<Set<T>> future;

  private final ScheduledExecutorService scheduler;

  private Instant lastChange;

  private Instant completed;

  public TimeoutSet(
      Duration afterLastChangeTimeout,
      ScheduledExecutorService scheduler) {
    this(afterLastChangeTimeout, null, null, null, scheduler);
  }

  public TimeoutSet(
      Duration afterLastChangeTimeout,
      Duration afterCreateTimeout,
      Predicate<Set<T>> isSetCompletePredicate,
      Set<T> set,
      ScheduledExecutorService scheduler) {
    this.afterLastChangeTimeout = afterLastChangeTimeout;
    this.afterCreationTimeout = afterCreateTimeout;
    this.isSetCompletePredicate = isSetCompletePredicate!=null?isSetCompletePredicate:s->false;
    this.set = set!=null?set:new HashSet<>();
    future = new CompletableFuture<>();
    if(scheduler == null) {
      throw new IllegalArgumentException("scheduler is null");
    }
    this.scheduler = scheduler;
    if(afterCreationTimeout != null) {
      scheduler.schedule(
          (Runnable)this::checkAndComplete,
          afterCreationTimeout.toNanos(),
          TimeUnit.NANOSECONDS);
    }
  }

  public synchronized void add(T e) {
    if(future.isDone()) {
      throw new IllegalStateException("set is closed, no more additions after future is completed");
    }
    set.add(e);
    Instant now = Instant.now();
    lastChange = now;
    if(!checkAndComplete(now)) {
      if(afterLastChangeTimeout != null) {
        // no need to re-schedule if checkAndComplete returns false.
        // a later call to add has already created another schedule in this case
        scheduler.schedule(
            (Runnable)this::checkAndComplete,
            afterLastChangeTimeout.toNanos(),
            TimeUnit.NANOSECONDS);
      }
    }
  }

  private synchronized boolean checkAndComplete() {
    return checkAndComplete(Instant.now());
  }

  private synchronized boolean checkAndComplete(Instant now) {
    if(isCompleteOrTimeout(now)) {
      completed = now;
      future.complete(set);
      return true;
    } else {
      return false;
    }
  }

  public synchronized boolean isCompleteOrTimeout(Instant now) {
    // do timeout check first as likely less expensive
    return isTimeout(now) || isSetCompletePredicate.test(set);
  }

  public synchronized boolean isLastChangeTimeout() {
    return isAfterLastChangeTimeout(Instant.now());
  }

  public synchronized boolean isAfterLastChangeTimeout(Instant now) {
    return isTimeout(now, lastChange, afterLastChangeTimeout);
  }

  public synchronized boolean isAfterCreationTimeout() {
    return isAfterCreationTimeout(Instant.now());
  }

  public synchronized boolean isAfterCreationTimeout(Instant now) {
    return isTimeout(now, created, afterCreationTimeout);
  }

  private synchronized boolean isTimeout(Instant now, Instant i, Duration d) {
    if(now == null) {
      now = Instant.now();
    }
    if((i != null) && (d != null)) {
      Instant timeout = i.plus(d);
      return now.isAfter(timeout) || now.equals(timeout);
    } else {
      return false;
    }
  }

  public synchronized boolean isTimeout(Instant now) {
    return isAfterLastChangeTimeout(now) || isAfterCreationTimeout(now);
  }

  public CompletableFuture<Set<T>> getFuture() {
    return future;
  }

  public Instant getCompleted() {
    return completed;
  }

  public synchronized Set<T> getSet() {
    return set;
  }

  public Duration getAfterLastChangeTimeout() {
    return afterLastChangeTimeout;
  }

  public Duration getAfterCreationTimeout() {
    return afterCreationTimeout;
  }

  public Predicate<Set<T>> getIsSetCompletePredicate() {
    return isSetCompletePredicate;
  }

  public synchronized Instant getLastChange() {
    return lastChange;
  }

  public Instant getCreated() {
    return created;
  }

  public ScheduledExecutorService getScheduler() {
    return scheduler;
  }

}