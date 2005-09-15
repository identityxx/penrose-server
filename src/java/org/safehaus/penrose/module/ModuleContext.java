/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
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
