package org.safehaus.penrose.session;

/**
 * @author Endi S. Dewata
 */
public interface ReferralListener {

    public void referralAdded(ReferralEvent event);
    public void referralRemoved(ReferralEvent event);
}
