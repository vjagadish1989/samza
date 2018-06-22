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

import com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.StreamValidationException;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.kafka.KafkaSystemAdmin;
import org.apache.samza.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class NewKafkaSystemFactory implements SystemFactory {
  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    String bootstrapUrl = config.get("systems.kafka.producer.bootstrap.servers");
    String maxPollRecords = config.get("systems.kafka.consumer.max.poll.records");
    String fetchMaxBytes = config.get("systems.kafka.consumer.max.partition.fetch.bytes");

    return new Consumer(bootstrapUrl, maxPollRecords, fetchMaxBytes);
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    return null;
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    SystemFactory factory = Util.getObj("org.apache.samza.system.kafka.KafkaSystemFactory", SystemFactory.class);
    return factory.getAdmin(systemName, config);
  }

  public static class WrapperAdmin implements SystemAdmin {

    private final SystemAdmin admin;

    public WrapperAdmin(SystemAdmin admin) {
      this.admin = admin;
    }
    @Override
    public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
      return admin.getOffsetsAfter(offsets);
    }

    @Override
    public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
      return admin.getSystemStreamMetadata(streamNames);
    }

    @Override
    public Integer offsetComparator(String offset1, String offset2) {
      return null;
    }

    @Override
    public boolean createStream(StreamSpec streamSpec) {
      return admin.createStream( streamSpec);
    }

    @Override
    public void validateStream(StreamSpec streamSpec) throws StreamValidationException {
      admin.validateStream(streamSpec);
    }

    @Override
    public boolean clearStream(StreamSpec streamSpec) {
      return admin.clearStream(streamSpec);
    }
  }

  public static class Consumer implements SystemConsumer {

    private final KafkaConsumer<byte[], byte[]> consumer;

    private final Map<TopicPartition, String> startingOffsets = new HashMap<>();
    private final Set<SystemStreamPartition> registeredPartitions = new HashSet<>();
    private final Set<TopicPartition> topicPartitions = new HashSet<>();
    private final String maxPollRecords;
    private final String bootstrapUrl;
    private final String partitionFetchMaxBytes;

    public Consumer(String bootstrapUrl, String maxPollRecords, String partitionFetchMaxBytes) {
      this.maxPollRecords = maxPollRecords;
      this.bootstrapUrl = bootstrapUrl;
      this.partitionFetchMaxBytes = partitionFetchMaxBytes;
      this.consumer = new KafkaConsumer<byte[], byte[]>(getConsumerProperties());
    }

    @Override
    public void register(SystemStreamPartition systemStreamPartition, String offset) {
      startingOffsets.put(new TopicPartition(systemStreamPartition.getStream(),
          systemStreamPartition.getPartition().getPartitionId()), offset);
      registeredPartitions.add(systemStreamPartition);
    }

    @Override
    public void start() {
      consumer.assign(startingOffsets.keySet());
      startingOffsets.forEach((tp, offset) -> {
        consumer.seek(tp, Long.parseLong(offset));
      });
    }

    @Override
    public void stop() {
      consumer.close();
    }


    public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll1(Set<SystemStreamPartition> polledPartitions,
                                                                          long timeout) throws InterruptedException {
      Set<TopicPartition> pauseSet = new HashSet<>();
      Set<TopicPartition> resumeSet = new HashSet<>();

      for (SystemStreamPartition registeredPartition: registeredPartitions) {
        if (polledPartitions.contains(registeredPartition)) {
          resumeSet.add(new TopicPartition(registeredPartition.getStream(), registeredPartition.getPartition().getPartitionId()));
        } else {
          pauseSet.add(new TopicPartition(registeredPartition.getStream(), registeredPartition.getPartition().getPartitionId()));
        }
      }
      //System.out.println("Polling from " + polledPartitions.size() + " " + resumeSet.size());
      System.out.println("Pause set: " + pauseSet.size() + " poll set: " + resumeSet.size());
      consumer.pause(pauseSet);
      consumer.resume(resumeSet);
      final ConsumerRecords records = consumer.poll(500);
      final Map<SystemStreamPartition, List<IncomingMessageEnvelope>> translatedRecords = translate(records);

      System.out.println("Translated record size: " + translatedRecords.size());
      if (translatedRecords.size() != 0) {
        for (SystemStreamPartition ssp : translatedRecords.keySet()) {
          System.out.println("returned records: " + translatedRecords.get(ssp).size());
        }
      } else {
        System.out.println("returned no records");
      }
      return translatedRecords;
    }

    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> translate(ConsumerRecords<byte[], byte[]> records) {
      Map<SystemStreamPartition, List<IncomingMessageEnvelope>> envelopes = new HashMap<>();
      for(ConsumerRecord record: records) {
        SystemStreamPartition ssp = new SystemStreamPartition("kafka", record.topic(), new Partition(record.partition()));
        IncomingMessageEnvelope envelope = new IncomingMessageEnvelope(ssp, String.valueOf(record.offset()), record.key(), record.value());

        envelopes.computeIfAbsent(ssp, k -> new ArrayList<>());
        envelopes.get(ssp).add(envelope);
      }
      return envelopes;
    }

    public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(Set<SystemStreamPartition> polledPartitions,
                                                                          long timeout) throws InterruptedException {
      Map<SystemStreamPartition, List<IncomingMessageEnvelope>> messages = new HashMap<>();

      for (SystemStreamPartition partition : polledPartitions) {
            pollSingle(new TopicPartition(partition.getStream(), partition.getPartition().getPartitionId()), messages);
      }
      /*
      for(SystemStreamPartition ssp : polledPartitions) {
        if (messages.containsKey(ssp)) {
          System.out.println("returned " + messages.get(ssp).size() + " messages for ssp " + ssp.getPartition().getPartitionId());
        } else {
          System.out.println("returned no messages for " + ssp.getPartition().getPartitionId());
        }
      }*/
      return messages;
    }

    public void pollSingle(TopicPartition partition, Map<SystemStreamPartition, List<IncomingMessageEnvelope>> messages) {
      //System.out.println("poll single " + partition);
      consumer.pause(topicPartitions);
      consumer.resume(ImmutableSet.of(partition));
      final ConsumerRecords<byte[], byte[]> records = consumer.poll(500);
      consumer.pause(ImmutableSet.of(partition));
      //System.out.println("paused " + partition + " from state");
      translate1(records, messages);
    }

    void translate1(ConsumerRecords<byte[], byte[]> records, Map<SystemStreamPartition, List<IncomingMessageEnvelope>> envelopes) {
      //System.out.println("begin records");
      for(ConsumerRecord record: records) {
       // System.out.println("inside envelopes " + record.partition() + " " + record.topic());
        SystemStreamPartition ssp = new SystemStreamPartition("kafka", record.topic(), new Partition(record.partition()));
        IncomingMessageEnvelope envelope = new IncomingMessageEnvelope(ssp, String.valueOf(record.offset()), record.key(), record.value());

        envelopes.computeIfAbsent(ssp, k -> new ArrayList<>());
        envelopes.get(ssp).add(envelope);
      }
     // System.out.println("end records");
    }


    public Properties getConsumerProperties() {
      Properties props = new Properties();
      props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapUrl);
      props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, this.maxPollRecords);
      props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
      props.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, this.partitionFetchMaxBytes);

      //props.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(maxPartitionBytes));
      //props.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, String.valueOf(maxRequestBytes));
      return props;
    }
  }
}
