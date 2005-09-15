/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.event;

public interface CacheListener {
    public void beforeLoadEntries(CacheEvent event) throws Exception;
    public void afterLoadEntries(CacheEvent event) throws Exception;
}
