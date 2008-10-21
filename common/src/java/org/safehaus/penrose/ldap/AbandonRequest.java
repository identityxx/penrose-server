package org.safehaus.penrose.ldap;

/**
 * @author Endi S. Dewata
 */
public class AbandonRequest extends Request implements Cloneable {

    protected int idToAbandon;

    public AbandonRequest() {
    }

    public AbandonRequest(AbandonRequest request) {
        super(request);
        idToAbandon = request.getIdToAbandon();
    }

    public int getIdToAbandon() {
        return idToAbandon;
    }

    public void setIdToAbandon(int idToAbandon) throws Exception {
        this.idToAbandon = idToAbandon;
    }

    public Object clone() throws CloneNotSupportedException {
        AbandonRequest request = (AbandonRequest)super.clone();

        request.idToAbandon = idToAbandon;

        return request;
    }
}