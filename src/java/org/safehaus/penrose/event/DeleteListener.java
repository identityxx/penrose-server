/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.event;

/**
 * @author Endi S. Dewata
 */
public interface DeleteListener {
    public void beforeDelete(DeleteEvent event) throws Exception;
    public void afterDelete(DeleteEvent event) throws Exception;
}
