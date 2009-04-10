package org.safehaus.penrose.ldapbackend;

/**
 * @author Endi S. Dewata
 */
public interface AbandonRequest extends Request {

    public void setIdToAbandon(int idToAbandon) throws Exception;
    public int getIdToAbandon() throws Exception;
}