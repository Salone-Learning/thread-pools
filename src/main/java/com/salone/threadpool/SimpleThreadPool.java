package com.salone.threadpool;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

public class SimpleThreadPool {

	private boolean stopped;
	private int capacity;
	private Set<Worker> workers;
	private BlockingQueue<Runnable> workQueue;
	private final ThreadFactory threadFactory;

	public SimpleThreadPool(int capacity, ThreadFactory threadFactory) {
		this.capacity = capacity;
		this.threadFactory = threadFactory;

		this.workQueue = new LinkedBlockingQueue<>(capacity);

		for (int i = 0; i < this.capacity; i++) {
			Worker worker = new Worker();
			workers.add(worker);

			Thread thread = worker.thread;
			thread.start();
		}
	}

	public void execute(Runnable task) {
		if (stopped) {
			throw new IllegalStateException("Thread Pool is not running");
		}

		workQueue.offer(task);
	}

	public synchronized void stop() {
		stopped = true;

		for (Worker worker : workers) {
			if (!worker.isStopped()) {
				worker.interrupt();
			}
		}
	}

	public boolean isStopped() {
		return stopped;
	}

	private class Worker implements Runnable {

		private boolean stopped;
		final Thread thread;

		public Worker() {
			this.thread = threadFactory.newThread(this);
		}

		@Override
		public void run() {
			Runnable task = null;

			while (!stopped && (task = workQueue.poll()) != null) {
				try {
					task.run();
				} catch (Exception e) {
					// Report exception but don't kill the Worker
					// as it will not be recovered automatically
				}
			}

		}

		public synchronized void interrupt() {
			if (!thread.isInterrupted()) {
				try {
					thread.interrupt();
				} catch (SecurityException ignore) {
				}
			}

			stopped = true;
		}

		public synchronized boolean isStopped() {
			return stopped;
		}
	}
}
