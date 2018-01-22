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


import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaConsumerBenchmark {
  private final int maxPartitionId;
  private final String bootstrapUrl;
  private int  maxPollRecords = 0;
  private long testDuration = 0;

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerBenchmark.class);

  private long totalMessages = 0;
  private long totalTimeMillis;

  public KafkaConsumerBenchmark(String bootstrapUrl, int maxPollRecords, int maxPartitionId, long testDuration) {
    this.bootstrapUrl = bootstrapUrl;
    this.maxPartitionId = maxPartitionId;
    this.maxPollRecords = maxPollRecords;
    this.testDuration = testDuration;
  }

  public void testConsumerBehavior() throws InterruptedException {
    Properties props = new Properties();
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafkaNoPatch_group");
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapUrl);
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(this.maxPollRecords));
    int maxPartitionBytes = 20 * 1024 * 1024;
    int maxRequestBytes = maxPartitionBytes;

    props.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(maxPartitionBytes));
    props.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, String.valueOf(maxRequestBytes));

    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(props);
    Set<TopicPartition> topicPartitions = new HashSet<>();

    for (int i = 0; i < maxPartitionId; i++) {
      TopicPartition tp = new TopicPartition("PageViewEvent", i);
      topicPartitions.add(tp);
    }
    consumer.assign(topicPartitions);
    consumer.seekToBeginning(topicPartitions);

    long startTime = System.currentTimeMillis();

    while (System.currentTimeMillis() - startTime > testDuration) {
      pollConsumer(consumer, topicPartitions, 10);
    }

    totalTimeMillis = System.currentTimeMillis() - startTime;
    consumer.close();
    printStats();
  }

  public void pollConsumer(KafkaConsumer<byte[], byte[]> consumer, Set<TopicPartition> tps, long timeout) {

    ConsumerRecords<byte[], byte[]> records;
    // make a call on the client
    try {
      records = consumer.poll(timeout);

      if (records.count() > 0) {
        System.out.println(String.format("Obtained %s records from partitions ", records.count()));
      }
      for (ConsumerRecord<byte[], byte[]> record: records) {
        totalMessages++;
      }

    } catch (InvalidOffsetException e) {
      throw e;
    }
  }

  private void printStats() {
    System.out.println("==================================================");
    System.out.println("Total time taken: (secs) " + totalTimeMillis / (1000000000.00));
    System.out.println("Total messages: " + totalMessages + " QPS: " + (totalMessages ) / totalTimeMillis);
  }

  public static void main(String[] args) throws Exception {
    String bootstrapUrl = args[1];
    int maxPollRecords = Integer.parseInt(args[0]);
    int maxPartitionId = Integer.parseInt(args[1]);
    long testDuration = Long.parseLong(args[2]);
    KafkaConsumerBenchmark perf = new KafkaConsumerBenchmark(bootstrapUrl, maxPollRecords, maxPartitionId, testDuration);
    perf.testConsumerBehavior();
  }
}