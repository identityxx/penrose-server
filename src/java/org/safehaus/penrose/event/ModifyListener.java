/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.event;

/**
 * @author Endi S. Dewata
 */
public interface ModifyListener {
    
    public void beforeModify(ModifyEvent event) throws Exception;
    public void afterModify(ModifyEvent event) throws Exception;
}
