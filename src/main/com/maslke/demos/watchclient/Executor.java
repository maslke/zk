package com.maslke.demos.watchclient;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class Executor implements Watcher, Runnable, DataMonitorListener {
    private String znode;
    /**
     * 实现了StatCallback接口，在使用exists获取到znode的节点信息之后，将会调用接口中的processResult方法
     */
    private DataMonitor dm;
    private ZooKeeper zk;
    private String pathname;

    public Executor(String hostPort, String znode, String filename)
            throws IOException {
        this.pathname = filename;
        this.znode = znode;
        this.zk = new ZooKeeper(hostPort, 3000, this);
        this.dm = new DataMonitor(this.zk, this.znode, this);
    }

    public static void main(String[] args) {
       String hostPort = "localhost:2181,localhost:2182,localhost:2183";
       String znode = "/temp";
       String filename = "C:/Users/DELL/Desktop/123.inf";
        try {
            new Executor(hostPort, znode, filename).run();
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        dm.handle(event);
    }

    @Override
    public void run() {
        try {
            synchronized (this) {
                /**
                 * 使用了Object的wait和notify机制，来退出循环。
                 * 在zookeeper的客户端连接断掉之后，dead会置为true，则会跳出循环。
                 * 线程会在wait调用的时候，进入休眠状态。等待notify来进行唤醒。
                 */
                while (!dm.dead) {
                    wait();
                }
            }
        }
        catch (InterruptedException ex) {

        }
    }

    @Override
    public void closing(int rc) {
        synchronized (this) {
            notifyAll();
        }
    }

    @Override
    public void exists(byte[] data) {
        if (data == null) {
            System.out.println("Stopping child");
        }
        else {
            System.out.println("Stopping child");
            try {
                FileOutputStream fos = new FileOutputStream(pathname, true);
                fos.write(data);
                fos.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("Starting child");
        }
    }
}
