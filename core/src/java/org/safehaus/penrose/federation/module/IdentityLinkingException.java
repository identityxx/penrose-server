package org.safehaus.penrose.federation.module;

import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.DN;

/**
 * @author Endi Sukma Dewata
 */
public class IdentityLinkingException extends Exception {

    private DN sourceDn;
    private Attributes sourceAttributes;

    private DN targetDn;
    private Attributes targetAttributes;

    private String reason;

    public IdentityLinkingException(Throwable cause) {
        super(cause);
    }
    
    public IdentityLinkingException(String message, Throwable cause) {
        super(message, cause);
    }

    public DN getSourceDn() {
        return sourceDn;
    }

    public void setSourceDn(DN sourceDn) {
        this.sourceDn = sourceDn;
    }

    public Attributes getSourceAttributes() {
        return sourceAttributes;
    }

    public void setSourceAttributes(Attributes sourceAttributes) {
        this.sourceAttributes = sourceAttributes;
    }

    public DN getTargetDn() {
        return targetDn;
    }

    public void setTargetDn(DN targetDn) {
        this.targetDn = targetDn;
    }

    public Attributes getTargetAttributes() {
        return targetAttributes;
    }

    public void setTargetAttributes(Attributes targetAttributes) {
        this.targetAttributes = targetAttributes;
    }

    public String getReason() {
        return reason == null ? getMessage() : reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }
}
