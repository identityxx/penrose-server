package org.safehaus.penrose.ldapbackend;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public interface ModifyRequest extends Request {

    public void setDn(DN dn) throws Exception;
    public DN getDn() throws Exception;

    public void setModifications(Collection<Modification> modifications) throws Exception;
    public Collection<Modification> getModifications() throws Exception;
}
