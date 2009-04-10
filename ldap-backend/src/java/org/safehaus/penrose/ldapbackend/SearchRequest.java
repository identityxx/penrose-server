package org.safehaus.penrose.ldapbackend;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public interface SearchRequest extends Request {

    public final static int SCOPE_BASE      = 0;
    public final static int SCOPE_ONE       = 1;
    public final static int SCOPE_SUB       = 2;

    public final static int DEREF_NEVER     = 0;
    public final static int DEREF_SEARCHING = 1;
    public final static int DEREF_FINDING   = 2;
    public final static int DEREF_ALWAYS    = 3;

    public void setDn(DN dn) throws Exception;
    public DN getDn() throws Exception;

    public void setFilter(Filter filter) throws Exception;
    public Filter getFilter() throws Exception;

    public void setScope(int scope) throws Exception;
    public int getScope() throws Exception;

    public void setTimeLimit(long timeLimit) throws Exception;
    public long getTimeLimit() throws Exception;

    public void setSizeLimit(long sizeLimit) throws Exception;
    public long getSizeLimit() throws Exception;

    public void setAttributes(Collection<String> attributes) throws Exception;
    public Collection<String> getAttributes() throws Exception;
}
