/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.module;

import org.safehaus.penrose.event.*;

/**
 * @author Endi S. Dewata
 */
public interface Module extends AddListener, BindListener, DeleteListener, ModifyListener, SearchListener {

    public void init(ModuleConfig config) throws Exception;
}
