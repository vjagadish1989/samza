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
package org.apache.samza.test.operator;

import kafka.utils.TestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.KafkaConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.test.harness.AbstractIntegrationTestHarness;
import scala.Option$;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Harness for writing integration tests for {@link StreamApplication}s.
 *
 * <p> This provides the following features for its sub-classes:
 * <ul>
 *   <li>
 *     Automatic Setup and teardown: Any non-trivial integration test brings up components like Zookeeper
 *     servers, Kafka brokers, Kafka producers and Kafka consumers. This harness initializes each
 *     of these components in {@link #setUp()} and shuts down each of them cleanly in {@link #tearDown()}.
 *     {@link #setUp()} and {@link #tearDown()} are automatically invoked from the Junit runner.
 *   <li>
 *     Interaction with Kafka: The harness provides convenience methods to interact with Kafka brokers, consumers
 *     and producers - for instance, methods to create topics, produce and consume messages.
 *   <li>
 *     Config defaults: Often Samza integration tests have to setup config boiler plate
 *     to perform even simple tasks like producing and consuming messages. This harness provides default string
 *     serdes for producing / consuming messages and a default system-alias named "kafka" that uses the
 *     {@link org.apache.samza.system.kafka.KafkaSystemFactory}.
 *   <li>
 *     Debugging: At times, it is convenient to debug integration tests locally from an IDE. This harness
 *     runs all its components (including Kafka brokers, Zookeeper servers and Samza) locally.
 * </ul>
 *
 * <p> <i> Implementation Notes: </i> <br/>
 * State persistence: {@link #tearDown()} clears all associated state (including topics and metadata) in Kafka and
 * Zookeeper. Hence, the state is not durable across invocations of {@link #tearDown()} <br/>
 *
 * Execution model: {@link StreamApplication}s are run as their own {@link org.apache.samza.job.local.ThreadJob}s.
 * Similarly, embedded Kafka servers and Zookeeper servers are run as their own threads.
 * {@link #produceMessage(String, int, String, String)} and {@link #getMessages(Collection, int)} are blocking calls.
 *
 * <h3>Usage Example</h3>
 * Here is an actual test that publishes a message into Kafka, runs an application, and verifies consumption
 * from the output topic.
 *
 *  <pre> {@code
 * class MyTest extends StreamApplicationIntegrationTestHarness {
 *   private final StreamApplication myApp = new StreamApplication();
 *   private final Collection<String> outputTopics = Collections.singletonList("output-topic");
 *   @Test
 *   public void test() {
 *     createTopic("mytopic", 1);
 *     produceMessage("mytopic", 0, "key1", "val1");
 *     runApplication(myApp, "myApp", null);
 *     List<ConsumerRecord<String, String>> messages = getMessages(outputTopics)
 *     Assert.assertEquals(messages.size(), 1);
 *   }
 * }}</pre>
 */
public class StreamApplicationIntegrationTestHarness extends AbstractIntegrationTestHarness {
  KafkaProducer producer;
  KafkaConsumer consumer;

  private static final int NUM_EMPTY_POLLS = 3;
  private static final Duration POLL_TIMEOUT_MS = Duration.ofSeconds(20);
  private static final String DEFAULT_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

  /**
   * Starts a single kafka broker, and a single embedded zookeeper server in their own threads.
   * Sub-classes should invoke {@link #zkConnect()} and {@link #bootstrapUrl()}s to
   * obtain the urls (and ports) of the started zookeeper and kafka broker.
   */
  @Override
  public void setUp() {
    super.setUp();

    Properties consumerDeserializerProperties = new Properties();
    consumerDeserializerProperties.setProperty("key.deserializer", DEFAULT_DESERIALIZER);
    consumerDeserializerProperties.setProperty("value.deserializer", DEFAULT_DESERIALIZER);

    producer = TestUtils.createNewProducer(
        bootstrapServers(),
        1,
        60 * 1000L,
        1024L * 1024L,
        0,
        0L,
        5 * 1000L,
        SecurityProtocol.PLAINTEXT,
        null,
        Option$.MODULE$.<Properties>apply(new Properties()),
        new StringSerializer(),
        new StringSerializer(),
        Option$.MODULE$.<Properties>apply(new Properties()));

    consumer = TestUtils.createNewConsumer(
        bootstrapServers(),
        "group",
        "earliest",
        4096L,
        "org.apache.kafka.clients.consumer.RangeAssignor",
        30000,
        SecurityProtocol.PLAINTEXT,
        Option$.MODULE$.<File>empty(),
        Option$.MODULE$.<Properties>empty(),
        Option$.MODULE$.<Properties>apply(consumerDeserializerProperties));
  }

  /**
   * Creates a kafka topic with the provided name and the number of partitions
   * @param topicName the name of the topic
   * @param numPartitions the number of partitions in the topic
   */
  public void createTopic(String topicName, int numPartitions) {
    TestUtils.createTopic(zkUtils(), topicName, numPartitions, 1, servers(), new Properties());
  }

  /**
   * Produces a message to the provided topic partition.
   * @param topicName the topic to produce messages to
   * @param partitionId the topic partition to produce messages to
   * @param key the key in the message
   * @param val the value in the message
   */
  public void produceMessage(String topicName, int partitionId, String key, String val) {
    producer.send(new ProducerRecord(topicName, partitionId, key, val));
    producer.flush();
  }

  @Override
  public int clusterSize() {
    return Integer.parseInt(KafkaConfig.TOPIC_DEFAULT_REPLICATION_FACTOR());
  }


  /**
   * Read messages from the provided list of topics until {@param threshold} messages have been read or until
   * {@link #NUM_EMPTY_POLLS} polls return no messages.
   *
   * The default poll time out is determined by {@link #POLL_TIMEOUT_MS} and the number of empty polls are
   * determined by {@link #NUM_EMPTY_POLLS}
   *
   * @param topics the list of topics to consume from
   * @param threshold the number of messages to consume
   * @return the list of {@link ConsumerRecord}s whose size can be atmost {@param threshold}
   */
  public List<ConsumerRecord<String, String>> getMessages(Collection<String> topics, int threshold) {
    int emptyPollCount = 0;
    List<ConsumerRecord<String, String>> recordList = new ArrayList<>();
    consumer.subscribe(topics);

    while (emptyPollCount < NUM_EMPTY_POLLS && recordList.size() < threshold) {
      ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT_MS.toMillis());
      if (!records.isEmpty()) {
        Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
        while (iterator.hasNext() && recordList.size() < threshold) {
          ConsumerRecord record = iterator.next();
          recordList.add(record);
        }
      } else {
        emptyPollCount++;
      }
    }
    return recordList;
  }

  /**
   * Executes the provided {@link StreamApplication} as a {@link org.apache.samza.job.local.ThreadJob}. The
   * {@link StreamApplication} runs in its own separate thread.
   *
   * @param app the application to run
   * @param appName the name of the application
   * @param overriddenConfigs configs to override
   */
  public void runApplication(StreamApplication app, String appName, Config overriddenConfigs) {

    Map<String, String> configs = new HashMap<>();
    configs.put("job.factory.class", "org.apache.samza.job.local.ThreadJobFactory");
    configs.put("job.name", appName);
    configs.put("app.class", app.getClass().getCanonicalName());
    configs.put("serializers.registry.json.class", "org.apache.samza.serializers.JsonSerdeFactory");
    configs.put("serializers.registry.string.class", "org.apache.samza.serializers.StringSerdeFactory");
    configs.put("systems.kafka.samza.factory", "org.apache.samza.system.kafka.KafkaSystemFactory");
    configs.put("systems.kafka.consumer.zookeeper.connect", zkConnect());
    configs.put("systems.kafka.producer.bootstrap.servers", bootstrapUrl());
    configs.put("systems.kafka.samza.key.serde", "string");
    configs.put("systems.kafka.samza.msg.serde", "string");
    configs.put("systems.kafka.samza.offset.default", "oldest");
    configs.put("job.coordinator.system", "kafka");
    configs.put("job.default.system", "kafka");
    configs.put("job.coordinator.replication.factor", "1");
    configs.put("task.window.ms", "1000");

    if (overriddenConfigs != null) {
      configs.putAll(overriddenConfigs);
    }

    ApplicationRunner runner = ApplicationRunner.fromConfig(new MapConfig(configs));
    runner.run(app);
  }

  /**
   * Shutdown and clear Zookeeper and Kafka broker state.
   */
  @Override
  public void tearDown() {
    super.tearDown();
  }
}
