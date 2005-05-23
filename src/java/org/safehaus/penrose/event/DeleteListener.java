/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
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
