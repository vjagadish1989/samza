package org.apache.samza.operators.impl;

import org.apache.commons.collections.map.HashedMap;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.spec.WindowPaneState;
import org.apache.samza.operators.windows.TriggersBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by jvenkatr on 12/9/16.
 */
public class TriggerManager<M extends MessageEnvelope, WK, WV> {

  private final Triggerable listener;
  List<TriggersBuilder.PeriodicTimeTrigger> periodicTimeTriggers = new ArrayList<>();

  List<TriggersBuilder.TimeSinceFirstMessageTrigger> timeSinceFirstMsgTriggers = new ArrayList<>();
  List<TriggersBuilder.TimeSinceLastMessageTrigger> timeSinceLastMsgTriggers = new ArrayList<>();
  Timer timer = new Timer();

  List<TriggersBuilder.CountTrigger> countTriggers = new ArrayList<>();

  Map<WK, TriggerTimerTask> timeSinceFirstMsgTriggersMap = new HashMap<>();
  Map<WK, TriggerTimerTask> timeSinceLastMsgTriggersMap = new HashMap<>();


  private final ScheduledExecutorService scheduler =
      Executors.newScheduledThreadPool(1);

  public enum TriggerResult {
    FIRE,
    CONTINUE
  }

  public static class Context<WK> {
    WK key;
    TriggersBuilder.Trigger trigger;

    private Context(WK key, TriggersBuilder.Trigger trigger) {
      this.key = key;
      this.trigger = trigger;
    }
  }

  public interface Triggerable<WK> {
    void onTrigger(TriggerManager.Context<WK> context);
  }

  public class TriggerTimerTask<WK> extends TimerTask {
    // TBD: should this also include a trigger context??

    private final WK key;
    private final TriggersBuilder.Trigger trigger;

    public TriggerTimerTask(WK key, TriggersBuilder.Trigger trigger) {
      this.key = key;
      this.trigger = trigger;
    }

    @Override
    public void run() {
      listener.onTrigger(new Context(key, trigger));
    }

  }

  public void start() {
    //Process and schedule callbacks for last msg triggers.
    for(TriggersBuilder.PeriodicTimeTrigger timeTrigger : periodicTimeTriggers) {
      TimerTask triggerable = new TimerTask() {
        @Override
        public void run() {
          //no key hint
          listener.onTrigger(new Context(null, timeTrigger));
        }
      };

      timer.scheduleAtFixedRate(triggerable, timeTrigger.getDelayMillis(), timeTrigger.getDelayMillis());
    }
  }

  public TriggerManager(List<TriggersBuilder.Trigger> triggers, Triggerable listener) {
    this.listener = listener;
  }



  public TriggerResult onMessage(M msg, WK key, WindowPaneState<WV> state) {

    TriggerResult result = TriggerResult.CONTINUE;

    //Process time since first msg trigger.
    if (state.getNumberMessages() == 1) {
      for (TriggersBuilder.TimeSinceFirstMessageTrigger trigger : timeSinceFirstMsgTriggers) {
        TriggerTimerTask tt = new TriggerTimerTask(key, trigger);
        timer.schedule(tt, trigger.getDelayMillis());
        timeSinceFirstMsgTriggersMap.put(key, tt);
      }
    }

    //Process and schedule callbacks for last msg triggers.
    TriggerTimerTask triggerTimerTask = timeSinceLastMsgTriggersMap.get(key);

    if (triggerTimerTask != null) {
      triggerTimerTask.cancel();
      timer.purge();
      timeSinceLastMsgTriggersMap.remove(key);
    }
    timeSinceLastMsgTriggersMap.put(key, new TriggerTimerTask(key, timeSinceLastMsgTriggers.get(0)));


    //count based triggers
    for (TriggersBuilder.CountTrigger trigger : countTriggers) {
      if (state.getNumberMessages() >= trigger.getCount()) {
        result = TriggerResult.FIRE;
      }
    }


    return result;
  }

}
