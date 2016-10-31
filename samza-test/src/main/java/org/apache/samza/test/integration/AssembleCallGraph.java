package org.apache.samza.test.integration;

import com.google.common.base.Charsets;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.io.Resources;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStreams;
import org.apache.samza.operators.TriggerBuilder;
import org.apache.samza.operators.Windows;
import org.apache.samza.operators.data.IncomingSystemMessage;
import org.apache.samza.operators.data.Message;
import org.apache.samza.operators.task.StreamOperatorTask;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.io.Source;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by jvenkatr on 10/26/16.
 */
public class AssembleCallGraph implements StreamOperatorTask, InitableTask {

  private static final Logger LOG = LoggerFactory.getLogger(AssembleCallGraph.class);
  private static final String DEFAULT_OUTPUT_TOPIC = "service-call-graph";

  private static final int DEFAULT_WINDOW_TIMEOUT = 60 * 1000;
  private static final int DEFAULT_BLACKLIST_THRESHOLD = 1000;


  private int serviceCallWindowTimeoutMs;
  private int maxElementsPerTree;

  private Schema callGraphSchema = null;

  SummaryStatistics treeSizeSummary = new SummaryStatistics();
  DescriptiveStatistics treeSizePerWindow = new DescriptiveStatistics();

  private String outputSystem = null;
  private String outputTopicName = null;
  private final Cache<String, Boolean> blackList = CacheBuilder.newBuilder().expireAfterWrite(12, TimeUnit.HOURS).build();

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    serviceCallWindowTimeoutMs = config.getInt("cga.assemble.group.by.milli", DEFAULT_WINDOW_TIMEOUT);
    maxElementsPerTree = config.getInt("cga.assemble.blacklist.trigger", DEFAULT_BLACKLIST_THRESHOLD);
    outputSystem = config.get("cga.assemble.output.system");
    outputTopicName = config.get("cga.assemble.output.topic", DEFAULT_OUTPUT_TOPIC);
    callGraphSchema = fetchCallgraphSchema();
  }

  @Override
  public void initOperators(Collection<MessageStreams.SystemMessageStream> sources) {
    sources.forEach(source -> {
      source.filter(msg -> filter(msg))
            .window(Windows.<IncomingSystemMessage, String>intoSessions(msg -> (String)msg.getKey())
                           .setTriggers(TriggerBuilder.timeoutSinceFirstMessage(serviceCallWindowTimeoutMs)
                                                      .earlyTriggerWhenExceedWndLen(maxElementsPerTree)))
            .sink((windowOutput, collector, coordinator) -> {
              // Extract treeId with it's associated service calls.
              String treeId = windowOutput.getKey();
              Collection<IncomingSystemMessage> serviceCalls = windowOutput.getMessage();

              // Update blacklist if need be.
              if (serviceCalls.size() >= maxElementsPerTree) {
                blackList.put(treeId, true);
              }  else {
              // Emit output to output topic.
                collect(treeId, serviceCalls, collector);
              }
            });
    });
  }

  private boolean filter(IncomingSystemMessage msg) {
    String key = (String)msg.getKey();
    return blackList.getIfPresent(key) == null;
  }

  private void collect(String treeId, Collection<IncomingSystemMessage> serviceCalls, MessageCollector collector) {
    GenericRecord callGraphRecord = new GenericData.Record(callGraphSchema);
    callGraphRecord.put("treeId", treeId);
    callGraphRecord.put("treeReassembled", false);

    List<GenericRecord> serviceCallRecords = serviceCalls.stream()
        .map(msg -> (GenericRecord) msg.getMessage())
        .collect(Collectors.toList());
    callGraphRecord.put("serviceCalls", serviceCallRecords);
    collector.send(new OutgoingMessageEnvelope(new SystemStream(outputSystem, outputTopicName), treeId, callGraphRecord));

    int numMessages = serviceCalls.size();
    treeSizeSummary.addValue(numMessages);
    treeSizePerWindow.addValue(numMessages);
  }

  private static String readFile(String fileName) throws IOException {
    URL resource = Resources.getResource(fileName);
    return Resources.toString(resource, Charsets.UTF_8);
  }

  private static Schema fetchCallgraphSchema() throws IOException {
    String schemaTemplate = readFile("ServiceCallGraph.unmerged");
    String serviceCallEventSchema = readFile("servicecallevent.avsc");
    String callGraphSchemaString = schemaTemplate.replaceAll("REPLACEMESCE", serviceCallEventSchema);
    Schema callGraphSchema = Schema.parse(callGraphSchemaString);
    LOG.info("Parsed callGraph schema {}", callGraphSchema);
    return callGraphSchema;
  }

}
