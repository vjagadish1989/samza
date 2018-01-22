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

package org.apache.samza.kafka;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class SystemConsumerBenchmark {

  private final String factoryClassName;
  private final SystemFactory systemFactory;
  private final SystemConsumer systemConsumer;
  private final Config config;

  private final Set<String> topicsToConsumeFrom;
  private final int timeToRunSecs;

  public SystemConsumerBenchmark(String factoryClassName, Config config, int timeToRun) {
    this.factoryClassName = factoryClassName;
    this.systemFactory = Util.getObj(factoryClassName);
    this.topicsToConsumeFrom = ImmutableSet.of("PageViewEvent");
    this.config = config;
    this.systemConsumer = systemFactory.getConsumer("kafka", config, new MetricsRegistryMap());
    this.timeToRunSecs = timeToRun;
  }

  public void registerOffsets() {
    final Map<SystemStreamPartition, String> oldestOffsets = getOldestOffsets();
    oldestOffsets.forEach((topicPartition, oldestOffset) -> {
      systemConsumer.register(topicPartition, oldestOffset);
    });
  }

  public void testConsumerThroughput() throws Exception {
    AtomicInteger numMessages = new AtomicInteger();
    long startTime = System.currentTimeMillis();
    registerOffsets();
    systemConsumer.start();
    while (System.currentTimeMillis() - startTime <= timeToRunSecs) {
      Map<SystemStreamPartition, List<IncomingMessageEnvelope>> messages = systemConsumer.poll(getOldestOffsets().keySet(), 1000);
      messages.forEach((ssp, msgList) -> {
        msgList.forEach(msg -> {
          //System.out.println(msg.getKey());
          numMessages.incrementAndGet();
        });
      });
    }

    long totalTime = System.currentTimeMillis() - startTime;
    System.out.println(String.format("Total number of messages read: %s", numMessages));
    System.out.println(String.format("Total time taken: %s ms", totalTime));
    System.out.println(String.format("Messages per second: %s", numMessages.get() / (totalTime/1000)));
  }

  public Map<SystemStreamPartition, String> getOldestOffsets() {
    Map<SystemStreamPartition, String> oldestOffsets = new HashMap<>();
    SystemAdmin admin = systemFactory.getAdmin("kafka", config);
    final Map<String, SystemStreamMetadata> systemStreamMetadata = admin.getSystemStreamMetadata(topicsToConsumeFrom);
    systemStreamMetadata.forEach((topic, topicMetadata) -> {
      final Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitionMetadata =
          topicMetadata.getSystemStreamPartitionMetadata();
      partitionMetadata.forEach((partition, metadata) -> {
         oldestOffsets.put(new SystemStreamPartition("kafka", topic, partition), metadata.getOldestOffset());
      });
    });
    return oldestOffsets;
  }

  public static Config buildConfig(String factoryClassName, String zkUrl, String bootstrapUrl, String maxPollRecords) {
    Map<String, String> properties = ImmutableMap.<String, String>builder().put("job.name", "myjob")
        .put("job.id", "i001")
        .put("systems.kafka.samza.factory", factoryClassName)
        .put("systems.kafka.consumer.zookeeper.connect", zkUrl)
        .put("systems.kafka.producer.bootstrap.servers", bootstrapUrl)
        .put("systems.kafka.consumer.max.poll.records", maxPollRecords)
        .build();
    return new MapConfig(properties);
  }

  public static void main(String[] args) throws Exception {
    // "zk-lca1-kafka.stg.linkedin.com:12913/kafka-queuing"
    // "lca1-kafka-kafka-queuing-vip.stg.linkedin.com:10251"
    // "com.linkedin.samza.system.kafka.SamzaRawLiKafkaSystemFactory"
    // 10 <-- maxPollRec
    // 60 <-- time duration
    String zkServer = args[0];
    String bootstrapServer = args[1];
    String factoryClazz = args[2];
    String maxPollRecords = args[3];
    int timeout = Integer.parseInt(args[4]);

    Config config = buildConfig(factoryClazz, zkServer, bootstrapServer, maxPollRecords);

    new SystemConsumerBenchmark(factoryClazz, config, timeout).testConsumerThroughput();
  }
}
