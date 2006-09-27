package org.safehaus.penrose.session;

/**
 * @author Endi S. Dewata
 */
public class ReferralEvent {

    public final static int REFERRAL_ADDED    = 0;
    public final static int REFERRAL_REMOVED  = 1;

    private int type;
    private Object referral;

    public ReferralEvent(int type) {
        this.type = type;
    }

    public ReferralEvent(int type, Object referral) {
        this.type = type;
        this.referral = referral;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public Object getReferral() {
        return referral;
    }

    public void setReferral(Object referral) {
        this.referral = referral;
    }
}
