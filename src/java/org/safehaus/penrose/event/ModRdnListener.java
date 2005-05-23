/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
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
