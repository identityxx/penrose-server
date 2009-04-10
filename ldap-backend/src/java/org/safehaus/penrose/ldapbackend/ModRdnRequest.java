package org.safehaus.penrose.ldapbackend;

/**
 * @author Endi S. Dewata
 */
public interface ModRdnRequest extends Request {

    public void setDn(DN dn) throws Exception;
    public DN getDn() throws Exception;

    public void setNewRdn(RDN rdn) throws Exception;
    public RDN getNewRdn() throws Exception;

    public void setDeleteOldRdn(boolean deleteOldRdn) throws Exception;
    public boolean getDeleteOldRdn() throws Exception;
}
