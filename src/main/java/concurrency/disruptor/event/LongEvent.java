package concurrency.disruptor.event;

/**
 * Created by fengwu
 * Date: 16/8/11
 */
public class LongEvent {
    private long value;

    public void set(long value)
    {
        this.value = value;
    }
}
