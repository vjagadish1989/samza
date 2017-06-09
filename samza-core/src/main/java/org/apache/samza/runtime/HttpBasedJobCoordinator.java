package org.apache.samza.runtime;

import org.apache.samza.SamzaException;
import org.apache.samza.container.ContainerHeartbeatClient;
import org.apache.samza.container.ContainerHeartbeatMonitor;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorListener;
import org.apache.samza.job.model.JobModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 *
 * A {@link org.apache.samza.coordinator.JobCoordinator} implementation that fetches the {@link org.apache.samza.job.model.JobModel}
 * from a provided http end-point.
 *
 */
public class HttpBasedJobCoordinator implements JobCoordinator{

  private static final Logger LOG = LoggerFactory.getLogger(HttpBasedJobCoordinator.class);
  private static final int DELAY_MS = new Random().nextInt(SamzaContainer.DEFAULT_READ_JOBMODEL_DELAY_MS()) + 1;

  private final String coordinatorUrl;
  private final String streamProcessorId;
  private final String executionEnvironmentId;

  private ContainerHeartbeatMonitor containerHeartbeatMonitor;
  private JobCoordinatorListener coordinatorListener;

  public HttpBasedJobCoordinator(HttpBasedJobCoordinatorConfig config) {
    this.streamProcessorId = config.getStreamProcessorId();
    this.coordinatorUrl = config.getCoordinatorUrl();
    this.executionEnvironmentId = config.getExecutionEnvironmentId();
  }

  @Override
  public void start() {
    if (coordinatorListener != null) {
      throw new IllegalStateException("Listener cannot be null");
    }
    try {
      containerHeartbeatMonitor.start();

      JobModel jobModel = SamzaContainer.readJobModel(coordinatorUrl, DELAY_MS);
      coordinatorListener.onNewJobModel(streamProcessorId, jobModel);

    } catch (Exception e) {
      coordinatorListener.onCoordinatorFailure(e);
    }
  }

  @Override
  public void stop() {
    if (containerHeartbeatMonitor != null) {
      containerHeartbeatMonitor.stop();
    }

    if (coordinatorListener != null) {
      coordinatorListener.onJobModelExpired();
      coordinatorListener.onCoordinatorStop();
    }
  }

  @Override
  public String getProcessorId() {
    return streamProcessorId;
  }

  @Override
  public void setCoordinatorListener(JobCoordinatorListener coordinatorListener) {
    this.coordinatorListener = coordinatorListener;
    this.containerHeartbeatMonitor = new ContainerHeartbeatMonitor(() -> {
      Throwable jobCoordinatorException = new SamzaException("Container shutdown due to expired heartbeat");
      coordinatorListener.onCoordinatorFailure(jobCoordinatorException);
    }, new ContainerHeartbeatClient(coordinatorUrl, executionEnvironmentId));
  }

  @Override
  public JobModel getJobModel() {
    return SamzaContainer.readJobModel(coordinatorUrl, DELAY_MS);
  }
}
