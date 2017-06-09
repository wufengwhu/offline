package concurrency.disruptor.handler;

import com.lmax.disruptor.EventHandler;
import concurrency.disruptor.event.LongEvent;


/**
 * Created by fengwu
 * Date: 16/8/11
 */
public class LongEventHandler implements EventHandler<LongEvent> {

    @Override
    public void onEvent(LongEvent event, long sequence, boolean endOfBatch) throws Exception {
        System.out.println("Event: " + event);
    }
}
