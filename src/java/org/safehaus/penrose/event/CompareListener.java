/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.event;

/**
 * @author Endi S. Dewata
 */
public interface CompareListener {
    public void beforeCompare(CompareEvent event) throws Exception;
    public void afterCompare(CompareEvent event) throws Exception;
}
