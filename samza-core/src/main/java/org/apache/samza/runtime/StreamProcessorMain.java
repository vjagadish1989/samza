package org.apache.samza.runtime;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.SamzaContainerExceptionHandler;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.processor.StreamProcessorLifecycleListener;
import org.apache.samza.task.AsyncStreamTask;
import org.apache.samza.task.AsyncStreamTaskFactory;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.task.TaskFactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 *
 */
public class StreamProcessorMain {
  private static final Logger LOG = LoggerFactory.getLogger(StreamProcessorMain.class);
  private static final int DELAY_MS = new Random().nextInt(SamzaContainer.DEFAULT_READ_JOBMODEL_DELAY_MS()) + 1;
  private CountDownLatch latch = new CountDownLatch(1);

  private void run() throws InterruptedException {
    Config config = getConfig();
    ApplicationRunner runner = getRunner(config);
    Object taskFactory = TaskFactoryUtil.createTaskFactory(config, runner);

    StreamProcessor processor = createStreamProcessor(config, taskFactory);
    processor.start();
    latch.await();
  }

  private StreamProcessor createStreamProcessor(Config config, Object taskFactory) {
    StreamProcessorLifecycleListener listener = createLifecycleListener();

    if (taskFactory instanceof StreamTaskFactory) {
      return new StreamProcessor(config, new HashMap<>(), (StreamTaskFactory) taskFactory, listener);
    } else if (taskFactory instanceof AsyncStreamTaskFactory) {
      return new StreamProcessor(config, new HashMap<>(), (StreamTaskFactory) taskFactory, listener);
    }
    return null;
  }

  private StreamProcessorLifecycleListener createLifecycleListener() {
    return new StreamProcessorLifecycleListener() {
      @Override
      public void onStart() {
        LOG.info("StreamProcessor started");
      }

      @Override
      public void onShutdown() {
        latch.countDown();
        LOG.info("StreamProcessor stopped");
      }

      @Override
      public void onFailure(Throwable t) {
        latch.countDown();
        LOG.error("Exiting due to an error in StreamProcessor: {}", t);
        System.exit(1);
      }
    };
  }

  private Config getConfig() {
    String coordinatorUrl = System.getenv(ShellCommandConfig.ENV_COORDINATOR_URL());
    return SamzaContainer.readJobModel(coordinatorUrl, DELAY_MS).getConfig();
  }

  private ApplicationRunner getRunner(Config config) {
    return new AbstractApplicationRunner(config) {
      @Override
      public void run(StreamApplication streamApp) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void kill(StreamApplication streamApp) {
        throw new UnsupportedOperationException();
      }

      @Override
      public ApplicationStatus status(StreamApplication streamApp) {
        throw new UnsupportedOperationException();
      }
    };
  }

  public static void main(String[] args) throws Exception {

    Thread.setDefaultUncaughtExceptionHandler(
        new SamzaContainerExceptionHandler(() -> {
          LOG.info("Exiting process now from uncaught exception handler.");
          System.exit(1);
        }));

    new StreamProcessorMain().run();
  }
}
