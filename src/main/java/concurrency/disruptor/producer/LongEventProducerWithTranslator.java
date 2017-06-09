package concurrency.disruptor.producer;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import concurrency.disruptor.event.LongEvent;

import java.nio.ByteBuffer;

/**
 * Created by fengwu
 * Date: 16/8/11
 */
public class LongEventProducerWithTranslator {
//    private final RingBuffer<LongEvent> ringBuffer;

//    public LongEventProducerWithTranslator(RingBuffer<LongEvent> ringBuffer) {
//        this.ringBuffer = ringBuffer;
//    }
//
//    private static final EventTranslatorOneArg<LongEvent, ByteBuffer> TRANSLATOR =
//            (event, sequence, bb) -> event.set(bb.getLong(0));
//
//    public void onData(ByteBuffer bb) {
//        ringBuffer.publishEvent(TRANSLATOR, bb);
//    }
}
