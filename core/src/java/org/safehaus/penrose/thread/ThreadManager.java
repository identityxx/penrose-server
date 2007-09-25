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
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.naming.PenroseContext;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadManager {

    public final static String MAX_THREADS      = "maxThreads";
    public final static int DEFAULT_MAX_THREADS = 20;

    public Logger log = LoggerFactory.getLogger(getClass());

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;

    private ThreadGroup        threadGroup;
    private ThreadPoolExecutor executorService;

    public ThreadManager() {
        threadGroup = new ThreadGroup("Penrose");
    }

    public void start() throws Exception {
        String s = penroseConfig.getProperty(MAX_THREADS);
        int maxThreads = s == null ? DEFAULT_MAX_THREADS : Integer.parseInt(s);

        executorService = new ThreadPoolExecutor(
                maxThreads,
                maxThreads,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>()
        );

        executorService.setThreadFactory(new ThreadFactory() {
            AtomicInteger threadId = new AtomicInteger();
            public Thread newThread(Runnable r) {
                return new Thread(threadGroup, r, threadGroup.getName()+"-"+threadId.getAndIncrement());
            }
        });
    }

    public void stop() throws Exception {
        if (executorService != null) executorService.shutdown();
    }

    public boolean isRunning() {
        return executorService != null && !executorService.isShutdown();
    }
    
    public void execute(Runnable runnable) throws Exception {
        executorService.execute(runnable);
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;
    }
}
