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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;

// simple in memory message service that does publish/subscribe on a single topic
public class MessageService {

  private List<Consumer<Message>> subscribers = new ArrayList<>();

  public synchronized void publish(Message message) {
    subscribers.forEach(s -> ForkJoinPool.commonPool().execute(() -> s.accept(message)));
  }

  public synchronized void subscribe(Consumer<Message> consumer) {
    subscribers.add(consumer);
  }

}
