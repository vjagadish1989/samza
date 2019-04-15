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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.samza.context.Context;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OutputOperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;

import java.util.Collection;
import java.util.Collections;


/**
 * An operator that sends incoming messages to an output {@link SystemStream}.
 */
class OutputOperatorImpl<M> extends OperatorImpl<M, Void> {

  private final OutputOperatorSpec<M> outputOpSpec;
  private final OutputStreamImpl<M> outputStream;
  private final SystemStream systemStream;

  OutputOperatorImpl(OutputOperatorSpec<M> outputOpSpec, SystemStream systemStream) {
    this.outputOpSpec = outputOpSpec;
    this.outputStream = outputOpSpec.getOutputStream();
    this.systemStream = systemStream;
  }

  @Override
  protected void handleInit(Context context) {
  }

  @Override
  protected CompletionStage<Collection<Void>> handleMessageAsync(M message, MessageCollector collector,
      TaskCoordinator coordinator) {
    Object key, value;
    if (outputStream.isKeyed()) {
      key = ((KV) message).getKey();
      value = ((KV) message).getValue();
    } else {
      key = null;
      value = message;
    }

    collector.send(new OutgoingMessageEnvelope(systemStream, null, key, value));
    return CompletableFuture.completedFuture(Collections.emptyList());
  }

  @Override
  protected void handleClose() {
  }

  @Override
  protected OperatorSpec<M, Void> getOperatorSpec() {
    return outputOpSpec;
  }
}
