package com.barrier.zk;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.distributedlock.zk.DistributedLock;

/*
 * an barrier example from the hadoop zookeeper doc.
 */
public class Barrier extends SyncPrimitive {
	int size;
	String name;
	CountDownLatch countDownLatch;

	public Barrier(String address, String root, int size, String name, CountDownLatch countDownLatch) {
		super(address);
		this.root = root;
		this.size = size;
		this.countDownLatch = countDownLatch;
		this.name = name;

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
					this.log("enter suscess");
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
                    	this.log("leave success");
                    	testOver();
                        return true;
                    }
                }
            }
    }
	
	public void testOver(){
		this.countDownLatch.countDown();
	}
	
	public static void main(String args[] ){
		int size = 5;
		String root = "/rootpath";
		CountDownLatch threadSemaphore = new CountDownLatch(size);
		
		for (int i = 0; i < size; i++) {
			
			new Thread() {
				@Override
				public void run() {
					try {
						Random rand = new Random();
				     	int r = rand.nextInt(100);
						Barrier barrier = new Barrier("localhost:2181", root, size, 
								"name" + Thread.currentThread().getId(), threadSemaphore);
						
						Thread.sleep(r*10);
						boolean flag = barrier.enter();
						System.out.println( "enter barrier:" + flag);
						r = rand.nextInt(100);
						Thread.sleep(r*10);
						barrier.leave();
						
					} catch (KeeperException e){
						
					} catch (InterruptedException e){

			        }
				}
			}.start();
		}
		try {
			threadSemaphore.await();
			System.out.println("所有线程运行结束!");
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
	}

}
