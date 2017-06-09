package network.zookeeper;

import org.apache.zookeeper.*;

import java.util.concurrent.CountDownLatch;

/**
 * Created by fengwu
 * Date: 16/8/29
 */
public class AuthSample implements Watcher{
    private static CountDownLatch countDownLatchSemaphore  = new CountDownLatch(1);
    final static String PATH = "/zk-book-auth_test";

    public static void main(String[] args) throws Exception {
        ZooKeeper zookeeper = new ZooKeeper("localhost:2181", 5000, new AuthSample());
        zookeeper.addAuthInfo("digest", "foo:true".getBytes());
//        zookeeper.exists(PATH, true);
        zookeeper.delete(PATH, -1);
        zookeeper.create(PATH, "init".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.EPHEMERAL);
        Thread.sleep(Integer.MAX_VALUE);
        ZooKeeper zooKeeper2 = new ZooKeeper("localhost:2181", 5000, new AuthSample());

        zooKeeper2.getData(PATH, false, null);
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            if (Event.KeeperState.SyncConnected == event.getState()) {
                if (Event.EventType.None == event.getType() && null == event.getPath()) {
                    countDownLatchSemaphore.countDown();

                } else if (Event.EventType.NodeCreated == event.getType()) {
                    System.out.println("Node(" + event.getPath() + ")Created");
                }
            }
        }catch (Exception e){

        }
    }
}
