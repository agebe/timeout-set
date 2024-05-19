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

public class TimeoutSet<T> {

  private Duration lastActivityTimeout;

  private Duration afterCreationTimeout;

  private Predicate<Set<T>> isSetCompletePredicate;

  private Set<T> set;

  private Instant lastActivity;

  private Instant created = Instant.now();

  private CompletableFuture<Set<T>> future;

  private ScheduledExecutorService scheduler;

  private Instant completed;

  public TimeoutSet(
      Duration lastActivityTimeout,
      ScheduledExecutorService scheduler) {
    this(lastActivityTimeout, null, null, null, scheduler);
  }

  public TimeoutSet(
      Duration lastActivityTimeout,
      Duration afterCreateTimeout,
      Predicate<Set<T>> isSetCompletePredicate,
      Set<T> set,
      ScheduledExecutorService scheduler) {
    this.lastActivityTimeout = lastActivityTimeout;
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
    lastActivity = now;
    if(!checkAndComplete(now)) {
      if(lastActivityTimeout != null) {
        // no need to re-schedule if checkAndComplete return false.
        // a later call to add as already created another schedule
        scheduler.schedule(
            (Runnable)this::checkAndComplete,
            lastActivityTimeout.toNanos(),
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

  public synchronized boolean isLastActivityTimeout() {
    return isLastActivityTimeout(Instant.now());
  }

  public synchronized boolean isLastActivityTimeout(Instant now) {
    return isTimeout(now, lastActivity, lastActivityTimeout);
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
    return isLastActivityTimeout(now) || isAfterCreationTimeout(now);
  }

  public CompletableFuture<Set<T>> getFuture() {
    return future;
  }

  public Instant getCompleted() {
    return completed;
  }

}
