/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.event;

/**
 * @author Endi S. Dewata
 */
public interface SearchListener {
    public void beforeSearch(SearchEvent event) throws Exception;
    public void afterSearch(SearchEvent event) throws Exception;
}
