/**
 * Copyright (c) 2000-2006, Identyx Corporation.
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
package org.safehaus.penrose.ldap;

import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.RDN;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;

import java.util.Collection;
import java.util.Arrays;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class SearchRequest extends Request {

    public final static int SCOPE_BASE      = 0;
    public final static int SCOPE_ONE       = 1;
    public final static int SCOPE_SUB       = 2;

    public final static int DEREF_NEVER     = 0;
    public final static int DEREF_SEARCHING = 1;
    public final static int DEREF_FINDING   = 2;
    public final static int DEREF_ALWAYS    = 3;

    protected DN dn;
    protected Filter filter;

    protected int scope         = SCOPE_SUB;
    protected int dereference   = DEREF_ALWAYS;
    protected boolean typesOnly = false;

    protected long sizeLimit    = 0;
    protected long timeLimit    = 0;

    protected Collection<String> attributes = new ArrayList<String>();

    public SearchRequest() {
    }

    public SearchRequest(SearchRequest request) throws Exception {
        super(request);

        dn          = request.dn;
        filter      = request.filter;
        scope       = request.scope;
    	dereference = request.dereference;
    	typesOnly   = request.typesOnly;
    	sizeLimit   = request.sizeLimit;
    	timeLimit   = request.timeLimit;

        attributes.addAll(request.attributes);
    }
    
    public int getDereference() {
        return dereference;
    }

    public void setDereference(int dereference) {
        this.dereference = dereference;
    }

    public boolean isTypesOnly() {
        return typesOnly;
    }

    public void setTypesOnly(boolean typesOnly) {
        this.typesOnly = typesOnly;
    }

    public int getScope() {
        return scope;
    }

    public void setScope(int scope) {
        this.scope = scope;
    }

    public Collection<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Collection<String> attributes) {
        if (this.attributes == attributes) return;
        this.attributes.clear();
        if (attributes == null) return;
        this.attributes.addAll(attributes);
    }

    public void setAttributes(String[] attributes) {
        this.attributes.clear();
        if (attributes == null) return;
        this.attributes.addAll(Arrays.asList(attributes));
    }

    public long getSizeLimit() {
        return sizeLimit;
    }

    public void setSizeLimit(long sizeLimit) {
        this.sizeLimit = sizeLimit;
    }

    public long getTimeLimit() {
        return timeLimit;
    }

    public void setTimeLimit(long timeLimit) {
        this.timeLimit = timeLimit;
    }

    public DN getDn() {
        return dn;
    }

    public void setDn(String dn) throws Exception {
        this.dn = new DN(dn);
    }

    public void setDn(RDN rdn) throws Exception {
        this.dn = new DN(rdn);
    }

    public void setDn(DN dn) {
        this.dn = dn;
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(String filter) throws Exception {
        this.filter = FilterTool.parseFilter(filter);
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }
}
