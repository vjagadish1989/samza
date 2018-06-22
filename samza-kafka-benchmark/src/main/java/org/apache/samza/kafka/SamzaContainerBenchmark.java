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
import com.google.common.io.Files;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaSystemConfig;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.metrics.ReadableMetricsRegistry;
import org.apache.samza.metrics.reporter.JmxReporter;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.util.ScalaJavaUtil;
import org.apache.samza.util.SystemClock;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by jvenkatr on 1/22/18.
 */
public class SamzaContainerBenchmark {
  private final Config config;
  private final String factoryClazz;
  private final String zkUrl;
  private final String bootstrapUrl;
  private static final Logger LOG = LoggerFactory.getLogger("SamzaContainerBenchmark");
  private final String maxPollRecords;
  private final long testDurationMs;
  public static AtomicLong numProcessed = new AtomicLong(0);
  private final String topicName;
  private final long maxPartition;
  private final String partitionFetchBytes;
  private final String bemSize;


  public SamzaContainerBenchmark(String bootstrapUrl, String zkUrl, String factoryClazz, String maxPollRecords, long testDurationMs, long maxPartition, String topicName, String partitionFetchBytes, String bemSize) {
    this.factoryClazz = factoryClazz;
    this.zkUrl = zkUrl;
    this.bootstrapUrl = bootstrapUrl;
    this.maxPollRecords = maxPollRecords;
    this.testDurationMs = testDurationMs;
    this.topicName = topicName;
    this.maxPartition = maxPartition;
    this.partitionFetchBytes = partitionFetchBytes;
    this.bemSize = bemSize;

    this.config = buildConfig();
  }

  public Config buildConfig() {
    Map<String, String> cfg = ImmutableMap.<String, String>builder().put("job.name", "container-benchmark")
        .put("task.class", "org.apache.samza.kafka.SimpleStreamTask")
        .put("task.inputs","kafka."+topicName)
        .put("systems.kafka.samza.factory", factoryClazz)
        .put("systems.kafka.consumer.zookeeper.connect", zkUrl)
        .put("systems.kafka.producer.bootstrap.servers", bootstrapUrl)
        .put("systems.kafka.streams."+topicName+".reset.offset", "true")
        .put("systems.kafka.default.stream.samza.offset.default", "oldest")
        .put("job.coordinator.system", "kafka")
        .put("task.checkpoint.system", "kafka")
        .put("job.coordinator.replication.factor", "1")
        .put("systems.kafka.consumer.max.poll.records", this.maxPollRecords)
        .put("job.factory.class", "org.apache.samza.job.local.ProcessJobFactory")
        .put("job.systemstreampartition.matcher.class", "org.apache.samza.system.RangeSystemStreamPartitionMatcher")
        .put("job.systemstreampartition.matcher.config.ranges", "0-"+this.maxPartition)
        .put("systems.kafka.consumer.max.partition.fetch.bytes", partitionFetchBytes)
        .put("systems.kafka.samza.fetch.threshold", bemSize)
        .put("serializers.registry.avro.schemas", "http://lca1-schema-registry-vip-1.corp.linkedin.com:10252/schemaRegistry/schemas")
        .put("systems.kafka.kafka.cluster","tracking")
        .build();
   return new MapConfig(cfg);
  }

  public void runSamzaContainer()  {

    JobModel jobModel = getJobModel();
    System.out.println(jobModel);

    final SamzaContainer container = SamzaContainer.apply(
        "0",
        jobModel,
        this.config,
        ScalaJavaUtil.<String, MetricsReporter>toScalaMap(ImmutableMap.of("jmx", new MetricsReporter() {
          @Override
          public void start() {
          }

          @Override
          public void register(String source, ReadableMetricsRegistry registry) {
          }

          @Override
          public void stop() {
          }
        })),
        new SimpleStreamTaskFactory());

    final ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
    service.schedule(() -> {
      long throughput = numProcessed.get() / (this.testDurationMs/1000);
      System.out.println("Messages processed: " + numProcessed.get());
      System.out.println("Throughput: " + throughput);
      String output = String.format("%s, %s, %s, %s, %s, %s, %s", maxPartition, maxPollRecords, partitionFetchBytes,
          bemSize, topicName, SimpleStreamTask.shouldSleep, throughput);
      System.out.println("Output: " + output);
      try {
        BufferedWriter bw = new BufferedWriter(new FileWriter("results", true));
        bw.write(output);
        bw.newLine();
        bw.close();
      } catch (IOException e) {
        e.printStackTrace();
      }

      container.shutdown();
    }, this.testDurationMs, TimeUnit.MILLISECONDS);

    container.run();
    service.shutdownNow();
  }

  public JobModel getJobModel() {
    JavaSystemConfig systemConfig = new JavaSystemConfig(this.config);
    Map<String, SystemAdmin> systemAdmins = new HashMap<>();
    for (String systemName: systemConfig.getSystemNames()) {
      String systemFactoryClassName = systemConfig.getSystemFactory(systemName);
      if (systemFactoryClassName == null) {
        LOG.error(String.format("A stream uses system %s, which is missing from the configuration.", systemName));
        throw new SamzaException(String.format("A stream uses system %s, which is missing from the configuration.", systemName));
      }
      SystemFactory systemFactory = Util.<SystemFactory>getObj(systemFactoryClassName, SystemFactory.class);
      systemAdmins.put(systemName, systemFactory.getAdmin(systemName, this.config));
    }


    StreamMetadataCache streamMetadataCache = new StreamMetadataCache(
        new SystemAdmins(this.config), 5000, SystemClock.instance());

    String containerId = "0";

    return JobModelManager.readJobModel(this.config, Collections.emptyMap(), null, streamMetadataCache,
        Collections.singletonList(containerId));
  }

  public static void main(String[] args) {
    String zkUrl = args[0];
    String bootstrapUrl = args[1];
    String factoryClazz = args[2];
    String maxPollRecords = args[3];
    long testDurationMs = Integer.parseInt(args[4]);
    long maxPartition = Integer.parseInt(args[5]);
    String topicName = args[6];
    String partitionFetchBytes = args[7];
    String shouldSleep = args[8];
    String bemSize = args[9];

    System.out.println("max poll records " + maxPollRecords);
    if ("sleep".equals(shouldSleep)) {
      SimpleStreamTask.shouldSleep = true;
    }

    new SamzaContainerBenchmark(bootstrapUrl, zkUrl, factoryClazz, maxPollRecords, testDurationMs, maxPartition, topicName, partitionFetchBytes, bemSize).runSamzaContainer();
  }
}