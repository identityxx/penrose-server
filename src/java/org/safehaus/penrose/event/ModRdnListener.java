/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.event;

/**
 * @author Endi S. Dewata
 */
public interface ModRdnListener {
    
    public void beforeModRdn(ModRdnEvent event) throws Exception;
    public void afterModRdn(ModRdnEvent event) throws Exception;
}
