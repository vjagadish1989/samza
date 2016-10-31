package org.apache.samza.test.integration;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.avro.Schema;
import org.apache.commons.collections.map.HashedMap;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.operators.MessageStreams;
import org.apache.samza.operators.internal.Operators;
import org.apache.samza.operators.task.StreamOperatorTask;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.io.Source;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jvenkatr on 10/25/16.
 */
public class RepartitionByTreeIDTask implements InitableTask, StreamOperatorTask {

  private static Logger logger = LoggerFactory.getLogger(RepartitionByTreeIDTask.class);
  private static final String ALL_SERVICE_CALLS="all-service-call-events";
  private static final String ALL_SERVICE_LOGS="all-service-logs";
  private static final String LAGGING_SERVICE_CALLS = "lagging-service-call-events";
  private static final String LAGGING_SERVICE_LOGS = "lagging-service-logs";

  private final long startTime = System.currentTimeMillis();
  private Counter sentToDebug;
  private Counter dropped;
  private Counter eventsProcessed;
  private Counter droppedFizzyEvents;
  private Counter droppedBadNamespaceEvents;
  private Counter droppedLogEvents;
  private Counter droppedNullTreeId;
  private Counter droppedBadSchemaEvents;
  private Gauge<Long> maxLag;
  private Map<Integer, Counter> treeIdDistribution = new HashMap<>();

  private boolean shouldDownSample = false;
  private double percentSample = 100;
  private long shutdownAfterMs = -1L;

  private int counter = 0;
  private Map<String, RecordReEncoder> reencoders = new HashMap<>();

  private String outputSystem;
  private SystemStream laggingServiceCalls;
  private SystemStream laggingLogEvents;
  private SystemStream serviceCalls;
  private SystemStream debugServiceCalls;
  private SystemStream logEvents;
  private SystemStream debugLogEvents;



  private Counter registerCounter(MetricsRegistry registry, String counterName) {
    return registry.newCounter("CallGraphRepartitioner", counterName);
  }



  @Override
  public void init(Config config, TaskContext context) throws Exception {
    outputSystem = config.get("task.systems.output");
    laggingServiceCalls = new SystemStream(outputSystem, LAGGING_SERVICE_CALLS);
    laggingLogEvents = new SystemStream(outputSystem, LAGGING_SERVICE_LOGS);
    serviceCalls = new SystemStream(outputSystem, ALL_SERVICE_CALLS);
    debugServiceCalls = new SystemStream(outputSystem, "service-call-debug");
    logEvents = new SystemStream(outputSystem, ALL_SERVICE_LOGS);
    debugLogEvents = new SystemStream(outputSystem, "service-log-debug");

    MetricsRegistry registry = context.getMetricsRegistry();
    sentToDebug = registerCounter(registry, "SentToDebug");
    dropped = registerCounter(registry, "DroppedEventsDueToDownSampling");
    eventsProcessed = registerCounter(registry, "CallGraphRepartitioner");
    droppedFizzyEvents = registerCounter(registry, "droppedFizzyEvents");
    droppedBadNamespaceEvents = registerCounter(registry, "droppedBadNamespaceEvents");
    droppedLogEvents = registerCounter(registry, "droppedLogEvents");
    droppedNullTreeId = registerCounter(registry, "droppedNullTreeId");
    droppedBadSchemaEvents = registerCounter(registry, "droppedBadSchemaEvents");
    maxLag = registry.newGauge("CallGraphRepartitioner", "maxWallclockLag", 0L);

    for (int bucket = 0; bucket < 10; bucket++ ) {
      String metricName = "treeIdBucket-" + bucket;
      treeIdDistribution.put(bucket, registry.newCounter("CallGraphRepartitioner", metricName));
    }
    shutdownAfterMs = config.getLong("cga.shutdown.after.ms", -1L);
    shouldDownSample = config.getBoolean("cga.repartition.downsample", false);
    percentSample = config.getInt("cga.repartition.sample.percent", 100);
    logger.info("Downsampling enabled = {}, sampling {} percent of messages.", shouldDownSample, percentSample);

    //Schema latestServiceCallEventSchema = loadSchema("servicecallevent.avsc");
    Schema latestLoggingEvent = null;//loadSchema("LoggingEvent.avsc");
    //reencoders.put("service_call", new RecordReEncoder(latestServiceCallEventSchema, Option.apply(registerCounter(registry, "ReEncodedServiceCalls"))));
    //reencoders.put("log_event", new RecordReEncoder(latestLoggingEvent, Option.apply(registerCounter(registry, "ReEncodedServiceCalls"))));

    logger.info("Done initialization");
  }

  @Override
  public void initOperators(Collection<MessageStreams.SystemMessageStream> sources) {
     sources.forEach(source -> {
      // source.map()
     });
  }
}
