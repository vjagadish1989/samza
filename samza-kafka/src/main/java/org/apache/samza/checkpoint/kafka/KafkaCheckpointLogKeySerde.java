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
package org.apache.samza.checkpoint.kafka;

import com.google.common.collect.ImmutableMap;
import org.apache.samza.SamzaException;
import org.apache.samza.container.TaskName;
import org.apache.samza.serializers.Serde;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.module.SimpleModule;

import java.io.IOException;
import java.util.LinkedHashMap;

/**
 * A serde for {@link KafkaCheckpointLogKey}.
 *
 * <p> Keys in the Kafka checkpoint log are serialized as JSON strings.
 * {"systemstreampartition-grouper-factory"" :"org.apache.samza.container.grouper.stream.GroupByPartitionFactory",
 *    "taskName":"Partition 0", "type":"checkpoint"}
 */
public class KafkaCheckpointLogKeySerde implements Serde<KafkaCheckpointLogKey> {

  private static final String SSP_GROUPER_FACTORY_FIELD = "systemstreampartition-grouper-factory";
  private static final String TASK_NAME_FIELD = "taskName";
  private static final String TYPE_FIELD = "type";

  private final ObjectMapper mapper;

  public KafkaCheckpointLogKeySerde() {
    mapper = new ObjectMapper();
  }

  @Override
  public byte[] toBytes(KafkaCheckpointLogKey key) {
    try {
      return mapper.writeValueAsBytes(ImmutableMap.of(
          SSP_GROUPER_FACTORY_FIELD, key.getGrouperFactoryClassName(),
          TASK_NAME_FIELD, key.getTaskName().toString(),
          TYPE_FIELD, key.getType()
        ));
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public KafkaCheckpointLogKey fromBytes(byte[] bytes) {
    try {
      LinkedHashMap<String, String> deserializedKey = mapper.readValue(bytes, LinkedHashMap.class);
      return new KafkaCheckpointLogKey(deserializedKey.get(SSP_GROUPER_FACTORY_FIELD),
          new TaskName(deserializedKey.get(TASK_NAME_FIELD)),
          deserializedKey.get(TYPE_FIELD));
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }
}
