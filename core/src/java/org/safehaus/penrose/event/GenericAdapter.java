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
package org.safehaus.penrose.event;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class GenericAdapter
        implements BindListener, AddListener, ModifyListener, ModRdnListener,
        DeleteListener, SearchListener, CompareListener
    {

    Logger log = LoggerFactory.getLogger(getClass());

    public GenericAdapter() {
        log.debug("GenericAdapter.<init>()");
    }

    public boolean beforeBind(BindEvent e) throws Exception {
        log.debug("GenericAdapter.beforeBind()");
        return true;
    }

    public void afterBind(BindEvent e) throws Exception {
        log.debug("GenericAdapter.afterBind()");
    }

    public boolean beforeUnbind(BindEvent e) throws Exception {
        log.debug("GenericAdapter.beforeUnbind()");
        return true;
    }

    public void afterUnbind(BindEvent e) throws Exception {
        log.debug("GenericAdapter.afterUnbind()");
    }

    public boolean beforeAdd(AddEvent event) throws Exception {
        log.debug("GenericAdapter.beforeAdd()");
        return true;
    }

    public void afterAdd(AddEvent event) throws Exception {
        log.debug("GenericAdapter.afterAdd()");
    }

    public boolean beforeModify(ModifyEvent event) throws Exception {
        log.debug("GenericAdapter.beforeModify()");
        return true;
    }

    public void afterModify(ModifyEvent event) throws Exception {
        log.debug("GenericAdapter.afterModify()");
    }

    public boolean beforeModRdn(ModRdnEvent event) throws Exception {
        log.debug("GenericAdapter.beforeModRdn()");
        return true;
    }

    public void afterModRdn(ModRdnEvent event) throws Exception {
        log.debug("GenericAdapter.afterModRdn()");
    }

    public boolean beforeDelete(DeleteEvent event) throws Exception {
        log.debug("GenericAdapter.beforeDelete()");
        return true;
    }

    public void afterDelete(DeleteEvent event) throws Exception {
        log.debug("GenericAdapter.afterDelete()");
    }

    public boolean beforeSearch(SearchEvent event) throws Exception {
        log.debug("GenericAdapter.beforeSearch()");
        return true;
    }

    public void afterSearch(SearchEvent event) throws Exception {
        log.debug("GenericAdapter.afterSearch()");
    }

    public boolean beforeCompare(CompareEvent event) throws Exception {
        log.debug("GenericAdapter.beforeCompare()");
        return true;
    }

    public void afterCompare(CompareEvent event) throws Exception {
        log.debug("GenericAdapter.afterCompare()");
    }
}
