package org.apache.samza.runtime;

import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;

/**
 * Config for the {@link HttpBasedJobCoordinator}
 */
public class HttpBasedJobCoordinatorConfig extends MapConfig {

  private final String coordinatorUrl;
  private final String streamProcessorId;
  private final String executionEnvironmentId;

  public HttpBasedJobCoordinatorConfig(String coordinatorUrl, String streamProcessorId, String executionEnvironmentId ) {
    this.coordinatorUrl = coordinatorUrl;
    this.streamProcessorId = streamProcessorId;
    this.executionEnvironmentId = executionEnvironmentId;
  }

  public String getCoordinatorUrl() {
    return coordinatorUrl;
  }

  public String getStreamProcessorId() {
    return streamProcessorId;
  }

  public String getExecutionEnvironmentId() {
    return executionEnvironmentId;
  }
}
