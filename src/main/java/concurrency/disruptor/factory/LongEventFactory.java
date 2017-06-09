package concurrency.disruptor.factory;

import com.lmax.disruptor.EventFactory;
import concurrency.disruptor.event.LongEvent;

/**
 * Created by fengwu
 * Date: 16/8/11
 */
public class LongEventFactory implements EventFactory<LongEvent> {
    @Override
    public LongEvent newInstance() {
        return new LongEvent();
    }
}
