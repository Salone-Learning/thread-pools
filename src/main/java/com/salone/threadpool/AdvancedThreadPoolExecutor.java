package com.salone.threadpool;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class AdvancedThreadPoolExecutor extends AbstractExecutorService {

	private boolean allowCoreThreadTimeOut;
	private int corePoolSize;
	private int maximumPoolSize;
	private int largestPoolSize;
	private final BlockingQueue<Runnable> workQueue;
	private long keepAliveTime;
	private final ThreadFactory threadFactory;

	private long completedTaskCount;

	private final AtomicInteger ctl = new AtomicInteger(ctlOf(RUNNING, 0));
	private static final int COUNT_BITS = Integer.SIZE - 3;
	private static final int CAPACITY = (1 << COUNT_BITS) - 1;

	private static final int RUNNING = -1 << COUNT_BITS;
	private static final int SHUTDOWN = 0 << COUNT_BITS;
	private static final int STOP = 1 << COUNT_BITS;
	private static final int TIDYING = 2 << COUNT_BITS;
	private static final int TERMINATED = 3 << COUNT_BITS;

	private static final boolean ONLY_ONE = true;

	private final ReentrantLock mainLock = new ReentrantLock();

	private final Condition termination = mainLock.newCondition();

	private final HashSet<Worker> workers = new HashSet<Worker>();

	public AdvancedThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
		if (corePoolSize < 0 || maximumPoolSize <= 0 || maximumPoolSize < corePoolSize || keepAliveTime < 0)
			throw new IllegalArgumentException();

		if (workQueue == null || threadFactory == null)
			throw new NullPointerException();

		this.corePoolSize = corePoolSize;
		this.maximumPoolSize = maximumPoolSize;
		this.workQueue = workQueue;
		this.keepAliveTime = unit.toNanos(keepAliveTime);
		this.threadFactory = threadFactory;
	}

	private class Worker extends AbstractQueuedSynchronizer implements Runnable {
		private static final long serialVersionUID = -6965888914770306472L;

		final Thread thread;
		volatile long completedTasks;

		Worker() {
			setState(-1);
			thread = threadFactory.newThread(this);
		}

		@Override
		public void run() {
			runWorker(this);
		}

		protected boolean isHeldExclusively() {
			return getState() != 0;
		}

		protected boolean tryAcquire(int unused) {
			if (compareAndSetState(0, 1)) {
				setExclusiveOwnerThread(Thread.currentThread());
				return true;
			}
			return false;
		}

		protected boolean tryRelease(int unused) {
			setExclusiveOwnerThread(null);
			setState(0);
			return true;
		}

		public void lock() {
			acquire(1);
		}

		public boolean tryLock() {
			return tryAcquire(1);
		}

		public void unlock() {
			release(1);
		}

		void interruptIfStarted() {
			Thread t;
			if (getState() >= 0 && (t = thread) != null && !t.isInterrupted()) {
				try {
					t.interrupt();
				} catch (SecurityException ignore) {
				}
			}
		}
	}

	final void runWorker(Worker w) {
		boolean completedAbruptly = true;

		Runnable task = null;
		Thread wt = Thread.currentThread();

		try {
			while ((task = getTask()) != null) {
				w.lock();

				if ((runStateAtLeast(ctl.get(), STOP) || (Thread.interrupted() && runStateAtLeast(ctl.get(), STOP)))
						&& !wt.isInterrupted())
					wt.interrupt();
				try {
					beforeExecute(wt, task);
					Throwable thrown = null;
					try {
						task.run();
					} catch (RuntimeException x) {
						thrown = x;
						throw x;
					} catch (Error x) {
						thrown = x;
						throw x;
					} catch (Throwable x) {
						thrown = x;
						throw new Error(x);
					} finally {
						afterExecute(task, thrown);
					}
				} finally {
					task = null;
					w.completedTasks++;
					w.unlock();
				}
			}
			completedAbruptly = false;
		} finally {
			processWorkerExit(w, completedAbruptly);
		}
	}

	protected void beforeExecute(Thread t, Runnable r) {
	}

	protected void afterExecute(Runnable r, Throwable t) {
	}

	protected void terminated() {
	}

	protected void onShutdown() {
	}

	private Runnable getTask() {
		boolean timedOut = false;

		for (;;) {
			int c = ctl.get();
			int rs = runStateOf(c);

			if (rs >= SHUTDOWN && (rs >= STOP || workQueue.isEmpty())) {
				decrementWorkerCount();
				return null;
			}

			int wc = workerCountOf(c);
			boolean timed = allowCoreThreadTimeOut || wc > corePoolSize;

			if ((wc > maximumPoolSize || (timed && timedOut)) && (wc > 1 || workQueue.isEmpty())) {
				if (compareAndDecrementWorkerCount(c))
					return null;
				continue;
			}

			try {
				Runnable r = timed ? workQueue.poll(keepAliveTime, TimeUnit.NANOSECONDS) : workQueue.take();
				if (r != null)
					return r;
				timedOut = true;
			} catch (InterruptedException retry) {
				timedOut = false;
			}
		}
	}

	private void processWorkerExit(Worker w, boolean completedAbruptly) {
		if (completedAbruptly)
			decrementWorkerCount();

		final ReentrantLock mainLock = this.mainLock;
		mainLock.lock();
		try {
			completedTaskCount += w.completedTasks;
			workers.remove(w);
		} finally {
			mainLock.unlock();
		}

		tryTerminate();

		int c = ctl.get();
		if (runStateLessThan(c, STOP)) {
			if (!completedAbruptly) {
				int min = allowCoreThreadTimeOut ? 0 : corePoolSize;
				if (min == 0 && !workQueue.isEmpty())
					min = 1;
				if (workerCountOf(c) >= min)
					return;
			}
			addWorker(false);
		}
	}

	private boolean addWorker(boolean core) {
		retry: for (;;) {
			int c = ctl.get();
			int rs = runStateOf(c);

			for (;;) {
				int wc = workerCountOf(c);
				if (wc >= CAPACITY || wc >= (core ? corePoolSize : maximumPoolSize))
					return false;
				if (compareAndIncrementWorkerCount(c))
					break retry;
				c = ctl.get();
				if (runStateOf(c) != rs)
					continue retry;
			}
		}

		boolean workerStarted = false;
		boolean workerAdded = false;
		Worker w = null;
		try {
			w = new Worker();
			final Thread t = w.thread;
			if (t != null) {
				final ReentrantLock mainLock = this.mainLock;
				mainLock.lock();
				try {
					int rs = runStateOf(ctl.get());

					if (rs < SHUTDOWN) {
						if (t.isAlive()) // precheck that t is startable
							throw new IllegalThreadStateException();
						workers.add(w);
						int s = workers.size();
						if (s > largestPoolSize)
							largestPoolSize = s;
						workerAdded = true;
					}
				} finally {
					mainLock.unlock();
				}
				if (workerAdded) {
					t.start();
					workerStarted = true;
				}
			}
		} finally {
			if (!workerStarted)
				addWorkerFailed(w);
		}
		return workerStarted;
	}

	private void addWorkerFailed(Worker w) {
		final ReentrantLock mainLock = this.mainLock;
		mainLock.lock();
		try {
			if (w != null)
				workers.remove(w);
			decrementWorkerCount();
			tryTerminate();
		} finally {
			mainLock.unlock();
		}
	}

	final void tryTerminate() {
		for (;;) {
			int c = ctl.get();
			if (isRunning(c) || runStateAtLeast(c, TIDYING) || (runStateOf(c) == SHUTDOWN && !workQueue.isEmpty()))
				return;
			if (workerCountOf(c) != 0) {
				interruptIdleWorkers(ONLY_ONE);
				return;
			}

			final ReentrantLock mainLock = this.mainLock;
			mainLock.lock();
			try {
				if (ctl.compareAndSet(c, ctlOf(TIDYING, 0))) {
					try {
						terminated();
					} finally {
						ctl.set(ctlOf(TERMINATED, 0));
						termination.signalAll();
					}
					return;
				}
			} finally {
				mainLock.unlock();
			}
		}
	}

	private void interruptIdleWorkers() {
		interruptIdleWorkers(false);
	}

	private void interruptWorkers() {
		final ReentrantLock mainLock = this.mainLock;
		mainLock.lock();
		try {
			for (Worker w : workers)
				w.interruptIfStarted();
		} finally {
			mainLock.unlock();
		}
	}

	private void interruptIdleWorkers(boolean onlyOne) {
		final ReentrantLock mainLock = this.mainLock;
		mainLock.lock();
		try {
			for (Worker w : workers) {
				Thread t = w.thread;
				if (!t.isInterrupted() && w.tryLock()) {
					try {
						t.interrupt();
					} catch (SecurityException ignore) {
					} finally {
						w.unlock();
					}
				}
				if (onlyOne)
					break;
			}
		} finally {
			mainLock.unlock();
		}
	}

	private List<Runnable> drainQueue() {
		BlockingQueue<Runnable> q = workQueue;
		ArrayList<Runnable> taskList = new ArrayList<Runnable>();
		q.drainTo(taskList);
		if (!q.isEmpty()) {
			for (Runnable r : q.toArray(new Runnable[0])) {
				if (q.remove(r))
					taskList.add(r);
			}
		}
		return taskList;
	}

	public List<Runnable> shutdownNow() {
		List<Runnable> tasks;
		final ReentrantLock mainLock = this.mainLock;
		mainLock.lock();
		try {
			advanceRunState(STOP);
			interruptWorkers();
			tasks = drainQueue();
		} finally {
			mainLock.unlock();
		}
		tryTerminate();
		return tasks;
	}

	private void advanceRunState(int targetState) {
		for (;;) {
			int c = ctl.get();
			if (runStateAtLeast(c, targetState) || ctl.compareAndSet(c, ctlOf(targetState, workerCountOf(c))))
				break;
		}
	}

	public void execute(Runnable command) {
		if (command == null)
			throw new NullPointerException();

		int c = ctl.get();
		if (workerCountOf(c) < corePoolSize) {
			if (addWorker(true)) {
				workQueue.offer(command);
				return;
			}
			c = ctl.get();
		}
		if (isRunning(c) && workQueue.offer(command)) {
			int recheck = ctl.get();
			if (!isRunning(recheck) && remove(command))
				reject(command);
			else if (workerCountOf(recheck) == 0)
				addWorker(false);
		} else if (!addWorker(false))
			reject(command);
	}

	public boolean remove(Runnable task) {
		boolean removed = workQueue.remove(task);
		tryTerminate();
		return removed;
	}

	final void reject(Runnable command) {
	}

	public void shutdown() {
		final ReentrantLock mainLock = this.mainLock;
		mainLock.lock();
		try {
			advanceRunState(SHUTDOWN);
			interruptIdleWorkers();
			onShutdown();
		} finally {
			mainLock.unlock();
		}
		tryTerminate();
	}

	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		long nanos = unit.toNanos(timeout);
		final ReentrantLock mainLock = this.mainLock;
		mainLock.lock();
		try {
			for (;;) {
				if (runStateAtLeast(ctl.get(), TERMINATED))
					return true;
				if (nanos <= 0)
					return false;
				nanos = termination.awaitNanos(nanos);
			}
		} finally {
			mainLock.unlock();
		}
	}

	public boolean isShutdown() {
		return !isRunning(ctl.get());
	}

	private static boolean runStateLessThan(int c, int s) {
		return c < s;
	}

	private static boolean runStateAtLeast(int c, int s) {
		return c >= s;
	}

	private static boolean isRunning(int c) {
		return c < SHUTDOWN;
	}

	private boolean compareAndIncrementWorkerCount(int expect) {
		return ctl.compareAndSet(expect, expect + 1);
	}

	private boolean compareAndDecrementWorkerCount(int expect) {
		return ctl.compareAndSet(expect, expect - 1);
	}

	private void decrementWorkerCount() {
		do {
		} while (!compareAndDecrementWorkerCount(ctl.get()));
	}

	private int runStateOf(int c) {
		return c & ~CAPACITY;
	}

	private static int workerCountOf(int c) {
		return c & CAPACITY;
	}

	private static int ctlOf(int rs, int wc) {
		return rs | wc;
	}

	public void setCorePoolSize(int corePoolSize) {
		if (corePoolSize < 0)
			throw new IllegalArgumentException();
		int delta = corePoolSize - this.corePoolSize;
		this.corePoolSize = corePoolSize;
		if (workerCountOf(ctl.get()) > corePoolSize)
			interruptIdleWorkers();
		else if (delta > 0) {
			int k = Math.min(delta, workQueue.size());
			while (k-- > 0 && addWorker(true)) {
				if (workQueue.isEmpty())
					break;
			}
		}
	}

	public boolean prestartCoreThread() {
		return workerCountOf(ctl.get()) < corePoolSize && addWorker(true);
	}

	void ensurePrestart() {
		int wc = workerCountOf(ctl.get());
		if (wc < corePoolSize)
			addWorker(true);
		else if (wc == 0)
			addWorker(false);
	}

	public int prestartAllCoreThreads() {
		int n = 0;
		while (addWorker(true))
			++n;
		return n;
	}

	public void setMaximumPoolSize(int maximumPoolSize) {
		if (maximumPoolSize <= 0 || maximumPoolSize < corePoolSize)
			throw new IllegalArgumentException();
		this.maximumPoolSize = maximumPoolSize;
		if (workerCountOf(ctl.get()) > maximumPoolSize)
			interruptIdleWorkers();
	}

	protected void finalize() {
		shutdown();
	}

	public boolean isAllowCoreThreadTimeOut() {
		return allowCoreThreadTimeOut;
	}

	public void setAllowCoreThreadTimeOut(boolean allowCoreThreadTimeOut) {
		this.allowCoreThreadTimeOut = allowCoreThreadTimeOut;
	}

	public int getLargestPoolSize() {
		return largestPoolSize;
	}

	public void setLargestPoolSize(int largestPoolSize) {
		this.largestPoolSize = largestPoolSize;
	}

	public long getKeepAliveTime() {
		return keepAliveTime;
	}

	public void setKeepAliveTime(long keepAliveTime) {
		this.keepAliveTime = keepAliveTime;
	}

	public long getCompletedTaskCount() {
		return completedTaskCount;
	}

	public void setCompletedTaskCount(long completedTaskCount) {
		this.completedTaskCount = completedTaskCount;
	}

	public int getCorePoolSize() {
		return corePoolSize;
	}

	public int getMaximumPoolSize() {
		return maximumPoolSize;
	}

	public boolean isTerminating() {
		int c = ctl.get();
		return !isRunning(c) && runStateLessThan(c, TERMINATED);
	}

	public boolean isTerminated() {
		return runStateAtLeast(ctl.get(), TERMINATED);
	}
}
