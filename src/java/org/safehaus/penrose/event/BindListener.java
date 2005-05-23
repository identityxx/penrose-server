/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
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
