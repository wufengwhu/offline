package network.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by fengwu
 * Date: 16/8/26
 */
public class ZooKeeperCreateAPIASyncUsage implements Watcher {
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private static ZooKeeper zooKeeper = null;

    AsyncCallback.StringCallback callback() {
        return new IStringCallback();
    }

    AsyncCallback.Children2Callback children2Callback() {
        return new IChidren2CallBack();
    }

    class IStringCallback implements AsyncCallback.StringCallback {

        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            System.out.println("Create path result: [" + rc + "," + path + "," + ctx + ", real path name :" + name);
        }
    }

    class IChidren2CallBack implements AsyncCallback.Children2Callback {

        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
            System.out.println("Get  Children znode result :[ response code:" + rc + ", param path : " + path + ", ctx"
                    + ctx + ", children list : " + children + ", state:" + stat);
        }
    }

    public static void main(String[] args) {
        try {
            String parentPath = "/zk-book";
            ZooKeeperCreateAPIASyncUsage zooKeeperCreateAPIASyncUsage = new ZooKeeperCreateAPIASyncUsage();
            zooKeeper = new ZooKeeper("localhost:2181", 5000, zooKeeperCreateAPIASyncUsage);
            connectedSemaphore.await();
//            zooKeeper.delete("/zk-test-ephemeral-", 0);
            zooKeeper.create(parentPath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT, zooKeeperCreateAPIASyncUsage.callback(), "I am  context .");
//            if(zooKeeper.exists(parentPath + "/c1", false))
            //zooKeeper.delete(parentPath + "/c1", 0);
            zooKeeper.create(parentPath + "/c1", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

//            List<String> childrenList = zooKeeper.getChildren(parentPath, true);  // 使用同步api获取子节点列表
            zooKeeper.getChildren(parentPath, true, zooKeeperCreateAPIASyncUsage.children2Callback(), null);
//            System.out.println(childrenList);
            //zooKeeper.delete(parentPath + "/c2", 0);
            zooKeeper.create(parentPath + "/c2", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            Thread.sleep(Integer.MAX_VALUE);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (Event.KeeperState.SyncConnected == event.getState()) {
            if (Event.EventType.None == event.getType() && null == event.getPath()) {

                connectedSemaphore.countDown();
            } else if (event.getType() == Event.EventType.NodeChildrenChanged) {
                try {
                    System.out.println("ReGet Child:" + zooKeeper.getChildren(event.getPath(), true));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
