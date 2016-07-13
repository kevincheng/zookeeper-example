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
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/*
 * an barrier example from the hadoop zookeeper doc.
 */
public class Barrier extends SyncPrimitive {
	int size;
	String name;

	public Barrier(String address, String root, int size) {
		super(address);
		this.root = root;
		this.size = size;

		if (this.zk != null) {
			try {
				Stat s = zk.exists(this.root, false);
				if (s == null) {
					this.zk.create(this.root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
			} catch (KeeperException e) {
				this.log(e.toString());
			} catch (InterruptedException e) {
				this.log(e.toString());
			}

			try {
				this.name = new String(InetAddress.getLocalHost().getCanonicalHostName().toString());

			} catch (UnknownHostException e) {
				this.log(e.toString());
			}
		}
	}

	public boolean enter() throws KeeperException, InterruptedException {
		this.zk.create(this.root+'/'+name, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		while(true) {
			synchronized (this.mutex) {
				List<String> list = zk.getChildren(this.root, true);
				
				if(list.size() < this.size) {
					this.mutex.wait();
				} else {
					return true;
				}
			}
		}
	}
	
	public boolean leave() throws KeeperException, InterruptedException{ 
		this.zk.delete(root + "/" + name, 0);
        while (true) {
            synchronized (this.mutex) {
                List<String> list = zk.getChildren(root, true);
                    if (list.size() > 0) {
                        this.mutex.wait();
                    } else {
                        return true;
                    }
                }
            }
    }
	

}
