/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.module;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public interface ModuleConfig {

    public String getModuleName();
    public String getModuleClass();
    
    public String getParameter(String name);
    public Collection getParameterNames();
}
