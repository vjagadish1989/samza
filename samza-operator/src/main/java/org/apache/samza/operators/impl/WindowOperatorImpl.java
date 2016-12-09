/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.operators.impl;

import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.spec.WindowOperatorSpec;
import org.apache.samza.operators.spec.WindowPaneState;
import org.apache.samza.operators.windows.TriggersBuilder;
import org.apache.samza.operators.windows.WindowKey;
import org.apache.samza.operators.windows.WindowOutput;
import org.apache.samza.operators.windows.BaseWindowFunction;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Implementation of the window operator.
 */
public class WindowOperatorImpl<M extends MessageEnvelope, K, WK extends WindowKey, WV, WM extends WindowOutput<WK, WV>> extends OperatorImpl<M, WM> {

  private final BaseWindowFunction<M, K, WV> window;
  private final List<TriggersBuilder.Trigger> defaultTriggers;
  private final Function<M, K> keyExtractor;

  Map<StoreKey, WindowPaneState<WV>>  hashMap = new HashMap();

  static class StoreKey<K> {
    static final StoreKey GLOBAL_STORE_KEY = new StoreKey(null, 0);
    private final long startTimestamp;
    private final K key;


    StoreKey(K key, long startTimeStamp) {
      this.startTimestamp = startTimeStamp;
      this.key = key;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      StoreKey<?> storeKey = (StoreKey<?>) o;

      if (startTimestamp != storeKey.startTimestamp) return false;
      return !(key != null ? !key.equals(storeKey.key) : storeKey.key != null);
    }

    @Override
    public int hashCode() {
      int result = (int) (startTimestamp ^ (startTimestamp >>> 32));
      result = 31 * result + (key != null ? key.hashCode() : 0);
      return result;
    }
  }

  public WindowOperatorImpl(WindowOperatorSpec spec) {
    window = spec.getWindow();
    defaultTriggers = window.getDefaultTriggers();
    keyExtractor = window.getKeyExtractor();

  }

  @Override
  public void onNext(M message, MessageCollector collector, TaskCoordinator coordinator) {
    K key = null;
    if (keyExtractor != null) {
      key = keyExtractor.apply(message);
    }

    StoreKey<K> windowKey = getStoreKey(key);

    //<m -> sk>
    //<k + some_timestamp>
    //multi-level hashtable --> one by timestamp; and within timestamp per key makes sense ;
  }

  private StoreKey<K> getStoreKey(K key) {
    long now = System.currentTimeMillis();
    TriggersBuilder.Trigger trigger = defaultTriggers.get(0);

    if (trigger instanceof TriggersBuilder.PeriodicTimeTrigger) {
      //time window.
      TriggersBuilder.PeriodicTimeTrigger timerTrigger = (TriggersBuilder.PeriodicTimeTrigger) trigger;
      long startTime = now - now % timerTrigger.getDelayMillis();
      return new StoreKey(key, startTime);
    }  else {
      return new StoreKey(key, 0);
    }
  }


  public Function<M, K> getKeyFunction() {
    Function<M, K> keyExtractor = window.getKeyExtractor();
    if (defaultTriggers != null) {

    }
    return keyExtractor;
  }
}
