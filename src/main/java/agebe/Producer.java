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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {

  private static final Logger log = LoggerFactory.getLogger(Producer.class);

  private Duration interval = Duration.ofSeconds(10);

  private Instant lastInterval;

  private MessageService messages;

  private int counter;

  public Producer(MessageService messages) {
    this.messages = messages;
    lastInterval = currentInterval(Instant.now());
    log.info("last interval '{}'", lastInterval);
  }

  private Instant currentInterval(Instant now) {
    return Instant.ofEpochMilli(now.toEpochMilli() / interval.toMillis() * interval.toMillis());
  }

  private long millisToNextInterval(Instant now) {
    Instant nextInterval = lastInterval.plusMillis(interval.toMillis());
    if(nextInterval.isBefore(now)) {
      throw new RuntimeException("next interval if before now");
    }
    return nextInterval.toEpochMilli() - now.toEpochMilli();
  }

  private void sleep(long ms) {
    try {
      if(ms > 0) {
        log.trace("sleep '{}'", ms);
        Thread.sleep(ms);
      } else {
        // sleep 1/4 millisecond
        Thread.sleep(0, 250_000);
      }
    } catch(InterruptedException e) {
      throw new RuntimeException("interrupted", e);
    }
  }

  public void run() {
    for(;;) {
      Instant now = Instant.now();
      Instant currentInterval = currentInterval(now);
      if(currentInterval.isAfter(lastInterval)) {
        try {
          sendMessages(currentInterval);
          log.info("done publishing messages for interval '{}'", currentInterval);
        } catch(Exception e) {
          log.error("send messages failed", e);
        } finally {
          lastInterval = currentInterval;
          counter++;
        }
      }
      sleep(millisToNextInterval(now)/2);
    }
  }

  private void sendMessages(Instant i) {
    int sequence = counter % 3;
    // sequence 2 should show exceptions on the subscriber side in the logs as the 2 second between the messages is
    // longer than the timeout on the TimeoutSet (1.5 seconds)
    log.info("doing sequence '{}'", sequence);
    switch(sequence) {
      case 0 -> {
        publish(1, i);
        publish(2, i);
        publish(3, i);
      }
      case 1 -> {
        sleep(1000);
        publish(2, i);
        sleep(1000);
        publish(1, i);
        sleep(1000);
        publish(3, i);
      }
      case 2 -> {
        sleep(2000);
        publish(1, i);
        sleep(2000);
        publish(2, i);
        sleep(2000);
        publish(3, i);
      }
    }
  }

  private void publish(int id, Instant i) {
    publish(new Message(String.valueOf(id), i));
  }

  private void publish(Message m) {
    log.info("publishing message '{}'", m);
    messages.publish(m);
  }

}
