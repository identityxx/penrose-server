/**
 * Copyright (c) 2000-2006, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.thread;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadManager {

    public Logger log = LoggerFactory.getLogger(getClass());

    public ThreadManagerConfig threadManagerConfig;

    public int corePoolSize                  = ThreadManagerConfig.DEFAULT_CORE_POOL_SIZE;
    public int maximumPoolSize               = ThreadManagerConfig.DEFAULT_MAXIMUM_POOL_SIZE;
    public long keepAliveTime                = ThreadManagerConfig.DEFAULT_KEEP_ALIVE_TIME;
    
    public TimeUnit unit                     = TimeUnit.SECONDS;
    public BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();

    public ThreadGroup        threadGroup;
    public ThreadPoolExecutor executorService;

    public ThreadManager(String name) {
        threadGroup = new ThreadGroup(name);
    }

    public void init(ThreadManagerConfig threadManagerConfig) {

        log.debug("Initializing ThreadManager...");

        this.threadManagerConfig = threadManagerConfig;

        String s = threadManagerConfig.getParameter(ThreadManagerConfig.CORE_POOL_SIZE);
        if (s != null) corePoolSize = Integer.parseInt(s);
        log.debug(" - corePoolSize: "+corePoolSize);

        s = threadManagerConfig.getParameter(ThreadManagerConfig.MAXIMUM_POOL_SIZE);
        if (s != null) maximumPoolSize = Integer.parseInt(s);
        log.debug(" - maximumPoolSize: "+maximumPoolSize);

        s = threadManagerConfig.getParameter(ThreadManagerConfig.KEEP_ALIVE_TIME);
        if (s != null) keepAliveTime = Integer.parseInt(s);
        log.debug(" - keepAliveTime: "+keepAliveTime);

        executorService = new ThreadPoolExecutor(
                corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                unit,
                workQueue
        );

        executorService.setThreadFactory(new ThreadFactory() {
            AtomicInteger threadId = new AtomicInteger();
            public Thread newThread(Runnable r) {
                return new Thread(threadGroup, r, threadGroup.getName()+"-"+threadId.getAndIncrement());
            }
        });
    }

    public void destroy() throws Exception {
        if (executorService != null) executorService.shutdown();
    }

    public boolean isRunning() {
        return executorService != null && !executorService.isShutdown();
    }
    
    public void execute(Runnable runnable) throws Exception {
        executorService.execute(runnable);
    }

    public int getCorePoolSize() {
        return corePoolSize;
    }

    public void setCorePoolSize(int corePoolSize) {
        this.corePoolSize = corePoolSize;
    }

    public int getMaximumPoolSize() {
        return maximumPoolSize;
    }

    public void setMaximumPoolSize(int maximumPoolSize) {
        this.maximumPoolSize = maximumPoolSize;
    }

    public long getKeepAliveTime() {
        return keepAliveTime;
    }

    public void setKeepAliveTime(long keepAliveTime) {
        this.keepAliveTime = keepAliveTime;
    }

    public TimeUnit getUnit() {
        return unit;
    }

    public void setUnit(TimeUnit unit) {
        this.unit = unit;
    }

    public BlockingQueue<Runnable> getWorkQueue() {
        return workQueue;
    }

    public void setWorkQueue(BlockingQueue<Runnable> workQueue) {
        this.workQueue = workQueue;
    }

    public ThreadGroup getThreadGroup() {
        return threadGroup;
    }

    public void setThreadGroup(ThreadGroup threadGroup) {
        this.threadGroup = threadGroup;
    }

    public ThreadPoolExecutor getExecutorService() {
        return executorService;
    }

    public void setExecutorService(ThreadPoolExecutor executorService) {
        this.executorService = executorService;
    }

    public ThreadManagerConfig getThreadManagerConfig() {
        return threadManagerConfig;
    }

    public void setThreadManagerConfig(ThreadManagerConfig threadManagerConfig) {
        this.threadManagerConfig = threadManagerConfig;
    }
}
