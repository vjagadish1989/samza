package org.apache.samza.runtime;

import org.apache.samza.config.Config;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorFactory;

/**
 * Created by jvenkatr on 6/7/17.
 */
public class HttpBasedJobCoordinatorFactory implements JobCoordinatorFactory {
  @Override
  public JobCoordinator getJobCoordinator(Config config) {
    String coordinatorUrl = System.getenv(ShellCommandConfig.ENV_COORDINATOR_URL());
    String executionEnvContainerId = System.getenv(ShellCommandConfig.ENV_EXECUTION_ENV_CONTAINER_ID());

    return new HttpBasedJobCoordinator(config);
  }
}
