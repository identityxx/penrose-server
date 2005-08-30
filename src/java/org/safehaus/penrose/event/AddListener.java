/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.event;

/**
 * @author Endi S. Dewata
 */
public interface AddListener {

    public void beforeAdd(AddEvent event) throws Exception;
    public void afterAdd(AddEvent event) throws Exception;
}
