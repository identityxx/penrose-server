/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.event;

public interface CacheListener {
    public void beforeLoadEntries(CacheEvent event) throws Exception;
    public void afterLoadEntries(CacheEvent event) throws Exception;
}
