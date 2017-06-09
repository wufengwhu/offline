package network.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by fengwu
 * Date: 16/8/26
 */
public class ZookeeperConstructorUsageSimple implements Watcher {

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);


    @Override
    public void process(WatchedEvent event) {
        System.out.println("Receive watched event:" + event);
        if (Event.KeeperState.SyncConnected == event.getState()) {
            connectedSemaphore.countDown();
        }
    }

    public static void main(String[] args) {
        try {
            ZooKeeper zookeeper = new ZooKeeper("localhost:2181", 5000, new ZookeeperConstructorUsageSimple());
            System.out.println(zookeeper.getState());
            long sessionid = zookeeper.getSessionId();
            byte[] passwd = zookeeper.getSessionPasswd();

            try {
                connectedSemaphore.await();
                String path1 = zookeeper.create("/zk-test-ephemeral-", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                System.out.println("Success create znode: " + path1);

                String path2 = zookeeper.create("/zk-test-ephemeral-", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL_SEQUENTIAL);

                System.out.println("Success create znode: " + path2);


                // Usr illegal sessionId and sessionPasswd
//                zookeeper = new ZooKeeper("localhost:2181", 5000,
//                        new ZookeeperConstructorUsageSimple(), 1L, "test".getBytes());
//
//                // Use correct sessionId and sessionPasswd
//                zookeeper = new ZooKeeper("localhost:2181", 5000, new ZookeeperConstructorUsageSimple(), sessionid,
//                        passwd);
//                Thread.sleep(Integer.MAX_VALUE);
            } catch (InterruptedException e) {
                System.out.println("zookeeper session established");

            } catch (KeeperException e) {
                e.printStackTrace();
                System.out.println("zookeeper catch keeper exception");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
