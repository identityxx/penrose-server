/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
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
