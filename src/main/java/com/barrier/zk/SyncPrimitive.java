package com.barrier.zk;


import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

class SyncPrimitive implements Watcher {
	static ZooKeeper zk = null;
    static Integer mutex;
    
    String root;

    SyncPrimitive(String address) {
        if(zk == null){
            try {
                System.out.println("Starting ZK:");
                zk = new ZooKeeper(address, 3000, this);
                mutex = new Integer(-1);
                System.out.println("Finished starting ZK: " + zk);
            } catch (IOException e) {
                System.out.println(e.toString());
                zk = null;
            }
        }
        //else mutex = new Integer(-1);
    }
    
    synchronized public void process(WatchedEvent event) {
    	if (event == null) {
			return;
		}
		Event.KeeperState keeperState = event.getState();
		Event.EventType eventType = event.getType();

    
    	if (Event.KeeperState.SyncConnected == keeperState) {
	        synchronized (mutex) {
	            this.log("有客户端成功连接进来 ");
	            mutex.notify();
	        }
    	} else if (Event.KeeperState.Disconnected == keeperState) {
			this.log("断开连接");
		} else if (Event.KeeperState.AuthFailed == keeperState) {
			this.log("权限检查失败");
		} else if (Event.KeeperState.Expired == keeperState) {
			this.log("会话失效");
		}
    }
    
    public void log(String s) {
    	System.out.println(s);
    }

}