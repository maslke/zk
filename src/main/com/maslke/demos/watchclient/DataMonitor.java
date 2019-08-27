package com.maslke.demos.watchclient;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Arrays;

public class DataMonitor implements AsyncCallback.StatCallback {

    private ZooKeeper zk;
    private String znode;
    boolean dead;
    private DataMonitorListener listener;
    private byte[] prevData;

    public DataMonitor(ZooKeeper zk, String znode, DataMonitorListener listener) {
        this.zk = zk;
        this.znode = znode;
        this.listener = listener;

        //Return the stat of the node of the given path.
        //在获取到znode的状态之后，会调用StatCallback接口的processResult方法
        zk.exists(znode, true, this, null);
    }

    public void handle(WatchedEvent event) {
        String path = event.getPath();
        if (event.getType() == Watcher.Event.EventType.None) {
            switch (event.getState()) {
                case SyncConnected:
                    System.out.println("zk connected.");
                    break;
                case Expired:
                    dead = true;
                    listener.closing(KeeperException.Code.SESSIONEXPIRED.intValue());
                    break;
            }
        }
        else {
            if (path != null && path.equals(znode)) {
                zk.exists(znode, true, this, null);
            }
        }
    }

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        boolean exists;
        if (rc == KeeperException.Code.OK.intValue()) {
            exists = true;
        }
        else if (rc == KeeperException.Code.NONODE.intValue()) {
            /**
             * 当zookeeper启动，客户端初始连接上去的时候，如果znode节点不存在，则statCallback的回调会首先进入这里。
             */
            exists = false;
        }
        else if (rc == KeeperException.Code.SESSIONEXPIRED.intValue() || rc == KeeperException.Code.NOAUTH.intValue()) {
            dead = true;
            listener.closing(rc);
            return;
        }
        else {
            zk.exists(znode, true, this, null);
            return;
        }

        byte[] b = null;
        if (exists) {
            try {
                System.out.println("获取znode节点的数据");
                b = zk.getData(znode, false, null);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        if ((b == null && b != prevData) || (b != null && !Arrays.equals(prevData, b))) {
            listener.exists(b);
            prevData = b;
        }
    }
}
