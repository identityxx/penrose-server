package org.safehaus.penrose.ldap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ReferralException;
import javax.naming.Context;
import javax.naming.NamingException;
import java.util.Hashtable;

/**
 * @author Endi S. Dewata
 */
public class PenroseReferralException extends ReferralException {

    Logger log = LoggerFactory.getLogger(getClass());

    private Object referralInfo;
    private boolean moreReferrals;
    private Context referralContext;

    public PenroseReferralException(Object referralInfo, boolean moreReferrals) {
        this.referralInfo = referralInfo;
        this.moreReferrals = moreReferrals;
    }

    public Object getReferralInfo() {
        log.debug("getReferralInfo() => "+referralInfo);
        return referralInfo;
    }

    public void setReferralInfo(Object referralInfo) {
        this.referralInfo = referralInfo;
    }

    public void setReferralContext(Context referralContext) {
        this.referralContext = referralContext;
    }

    public Context getReferralContext() {
        log.debug("getReferralContext() => "+referralContext);
        return referralContext;
    }

    public Context getReferralContext(Hashtable env) throws NamingException {
        log.debug("getReferralContext("+env+") => "+referralContext);
        return referralContext;
    }

    public boolean skipReferral() {
        log.debug("skipReferral() => "+moreReferrals);
        return moreReferrals;
    }

    public void retryReferral() {
        log.debug("retryReferral()");
    }
}
