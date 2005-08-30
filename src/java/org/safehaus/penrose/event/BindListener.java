/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.event;

/**
 * @author Endi S. Dewata
 */
public interface BindListener {

    public void beforeBind(BindEvent event) throws Exception;
    public void afterBind(BindEvent event) throws Exception;

    public void beforeUnbind(BindEvent event) throws Exception;
    public void afterUnbind(BindEvent event) throws Exception;
}
