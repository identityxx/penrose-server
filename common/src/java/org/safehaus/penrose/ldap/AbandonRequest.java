package org.safehaus.penrose.ldap;

/**
 * @author Endi S. Dewata
 */
public class AbandonRequest extends Request implements Cloneable {

    protected String operationName;

    public AbandonRequest() {
    }

    public AbandonRequest(AbandonRequest request) {
        super(request);
        operationName = request.getOperationName();
    }

    public String getOperationName() {
        return operationName;
    }

    public void setOperationName(String operationName) {
        this.operationName = operationName;
    }

    public Object clone() throws CloneNotSupportedException {
        AbandonRequest request = (AbandonRequest)super.clone();

        request.operationName = operationName;

        return request;
    }
}