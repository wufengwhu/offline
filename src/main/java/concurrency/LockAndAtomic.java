package concurrency;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by fengwu
 * Date: 16/8/22
 */
public class LockAndAtomic {

    abstract class PseudoRandom {
        abstract int nextInt(int n);

        public int calculateNext(int seed) {
            return new Random().nextInt(seed);
        }
    }

    @ThreadSafe
    public class ReentrantLockPseudoRandom extends PseudoRandom {
        private final Lock lock = new ReentrantLock(false);
        private int seed;

        public ReentrantLockPseudoRandom(int seed) {
            this.seed = seed;
        }

        @Override
        int nextInt(int n) {
            lock.lock();
            try {
                int s = seed;
                int seed = calculateNext(s);
                int remainder = s % n;
                return remainder > 0 ? remainder : remainder + n;
            } finally {
                lock.unlock();
            }
        }
    }


    @ThreadSafe
    public class AtomicPseudoRandom extends PseudoRandom{
        private AtomicInteger seed;

        AtomicPseudoRandom(int seed){
            this.seed = new AtomicInteger(seed);
        }

        @Override
        int nextInt(int n) {
            while (true){    // 外层循环不断尝试这个过程
                int s = seed.get();
                int nextSeed = calculateNext(s);
                if(seed.compareAndSet(s, nextSeed)){
                    int remainder = s % n;
                    return remainder > 0 ? remainder:remainder + n;
                }
            }
        }
    }

}
