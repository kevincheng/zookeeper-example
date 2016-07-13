package com.distributedlock.zk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import java.util.List;
import java.awt.print.Printable;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

public class DistributedLock implements Watcher {
	private static final int SESSION_TIMEOUT = 10000;
	private static final String GROUP_PATH = "/distlocks";
	private static final String SUB_PATH = GROUP_PATH + "/sub";
	private static final Logger LOG = LoggerFactory.getLogger(DistributedLock.class);
	private static final int THREAD_NUM = 10;
	private static final String CONNECTION_STRING = "localhost:2181";
	private static final CountDownLatch threadSemaphore = new CountDownLatch(THREAD_NUM);
	
	private int threadId;
	private ZooKeeper zk = null;
	private String selfPath;
	private String LOG_PREFIX;
	private String waitPath;
	private CountDownLatch connectedSemaphore = new CountDownLatch(1);  

	public DistributedLock(int id) {
		this.threadId = id;
		LOG_PREFIX = "第" + id + "线程";
	}

	public static void main(String[] args) {
		for (int i = 0; i < THREAD_NUM; i++) {
			final int threadId = i + 1;
			new Thread() {
				@Override
				public void run() {
					try {
						DistributedLock dc = new DistributedLock(threadId);
						dc.createConnection(CONNECTION_STRING, SESSION_TIMEOUT);
						// GROUP_PATH不存在的话，由一个线程创建即可；
						synchronized (threadSemaphore) {
							dc.createPath(GROUP_PATH, "该节点由线程" + threadId + "创建", true);
						}
						dc.getLock();
					} catch (Exception e) {
						LOG.error("【第" + threadId + "个线程】 抛出的异常：");
						e.printStackTrace();
					}
				}
			}.start();
		}
		try {
			threadSemaphore.await();
			LOG.info("所有线程运行结束!");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public boolean createPath(String path, String data, boolean watch) throws KeeperException, InterruptedException {
		if (this.zk.exists(path, watch) == null) {
			String reString = this.zk.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			this.log("节点创建成功, Path:" + reString + ", content:" + data);
		}
		return true;
	}

	public void createConnection(String connectString, int sessionTimeout) throws IOException, InterruptedException {
		zk = new ZooKeeper(connectString, sessionTimeout, this);
		connectedSemaphore.await();  
	}

	public void releaseConnection() {
		if (this.zk != null) {
			try {
				this.zk.close();
			} catch (InterruptedException e) {

			}
		}

		this.log("释放连接");
	}

	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub

		if (event == null) {
			return;
		}
		Event.KeeperState keeperState = event.getState();
		Event.EventType eventType = event.getType();

		if (Event.KeeperState.SyncConnected == keeperState) {
			if (Event.EventType.None == eventType) {
				this.log("成功连接上服务器");
				connectedSemaphore.countDown();
			} else if (Event.EventType.NodeDeleted == eventType && event.getPath().equals(this.waitPath)) {
				this.log("前面的任务已经释放锁，这里需要检查下能不能拿到锁");
				try {
					if (this.checkMinPath()) {
						this.getLockSuccess();
					}
				} catch (KeeperException e) {
					// TODO: handle exception
				} catch (InterruptedException e) {
					// TODO: handle exception
				}
			}
		} else if (Event.KeeperState.Disconnected == keeperState) {
			this.log("断开连接");
		} else if (Event.KeeperState.AuthFailed == keeperState) {
			this.log("权限检查失败");
		} else if (Event.KeeperState.Expired == keeperState) {
			this.log("会话失效");
		}
	}

	private void getLock() throws KeeperException, InterruptedException {
		this.selfPath = zk.create(SUB_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		this.log(this.LOG_PREFIX + "创建锁路径" + this.selfPath);
		if (this.checkMinPath()) {
			this.getLockSuccess();
		}
	}

	public boolean checkMinPath() throws KeeperException, InterruptedException {
		List<String> subNodes = zk.getChildren(this.GROUP_PATH, false);
		Collections.sort(subNodes);
		int index = subNodes.indexOf(this.selfPath.substring(this.GROUP_PATH.length() + 1));

		switch (index) {
		case -1: {
			this.log("节点已经不存在");
			return false;
		}
		case 0: {
			this.log("子节点中最小,可以获取锁");
			return true;
		}

		default: {
			this.waitPath = this.GROUP_PATH + "/" + subNodes.get(index - 1);
			this.log("获取子节点中，排在我前面的" + this.waitPath);
			;
			try {
				zk.getData(this.waitPath, true, new Stat());
			} catch (KeeperException e) {
				if (zk.exists(this.waitPath, false) == null) {
					this.log("前面的节点" + this.waitPath + "已经失效");
					return checkMinPath();
				} else {
					throw e;
				}
			}
		}
		}
		return false;
	}

	public void getLockSuccess() throws KeeperException, InterruptedException {
		if (zk.exists(this.selfPath, false) == null) {
			this.log("本节点已经不存在了");
			return;
		}
		this.log("获取锁成功");
		Thread.sleep(2000);
		this.log("开始删除本节点");
		zk.delete(this.selfPath, -1);
		this.releaseConnection();
		threadSemaphore.countDown();  
	}

	private void log(String s) {
		System.out.println(this.LOG_PREFIX + s + this.selfPath);;
//		LOG.info(this.LOG_PREFIX + s + this.selfPath);
	}
}
