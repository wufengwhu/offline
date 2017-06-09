package network.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

/**
 * Created by fengwu
 * Date: 16/8/29
 */
public class GetAndSetDataAPIUsage implements Watcher {
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private static ZooKeeper zk = null;
    private static Stat stat = new Stat();


    class IDataCallBack implements AsyncCallback.DataCallback {

        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            System.out.println(rc + "," + path + "," + new String(data));
            System.out.println(stat.getCzxid() + "," + stat.getMzxid() + "," + stat.getVersion());
        }
    }

    class IStatCallBack implements AsyncCallback.StatCallback {

        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            if (rc == 0) {
                System.out.println("SUCCESS");
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        String parentPath = "/zk-book";
        GetAndSetDataAPIUsage getAndSetDataAPIUsage = new GetAndSetDataAPIUsage();
        zk = new ZooKeeper("localhost:2181", 5000, getAndSetDataAPIUsage);
        connectedSemaphore.await();
        System.out.println(new String(zk.getData(parentPath, true, stat)));
        zk.setData(parentPath, "123".getBytes(), -1);
        Thread.sleep(Integer.MAX_VALUE);
    }

    @Override
    public void process(WatchedEvent event) {
        if (Event.KeeperState.SyncConnected == event.getState()) {
            if (Event.EventType.None == event.getType() && null == event.getPath()) {
                connectedSemaphore.countDown();
            } else if (event.getType() == Event.EventType.NodeDataChanged) {
                //System.out.println(new String(zk.getData(event.getPath(), true, stat)));
                zk.getData(event.getPath(), true, new IDataCallBack(), null);
                //System.out.println(stat.getCzxid() + "," + stat.getMzxid() + "," + stat.getVersion());
            }
        }
    }
}
