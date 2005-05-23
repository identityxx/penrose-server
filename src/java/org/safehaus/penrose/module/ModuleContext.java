/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.module;

import org.safehaus.penrose.event.*;

/**
 * @author Endi S. Dewata
 */
public interface ModuleContext {

    public void addConnectionListener(ConnectionListener l);
    public void removeConnectionListener(ConnectionListener l);

    public void addBindListener(BindListener l);
    public void removeBindListener(BindListener l);

    public void addSearchListener(SearchListener l);
    public void removeSearchListener(SearchListener l);

    public void addCompareListener(CompareListener l);
    public void removeCompareListener(CompareListener l);

    public void addAddListener(AddListener l);
    public void removeAddListener(AddListener l);

    public void addDeleteListener(DeleteListener l);
    public void removeDeleteListener(DeleteListener l);

    public void addModifyListener(ModifyListener l);
    public void removeModifyListener(ModifyListener l);
}
