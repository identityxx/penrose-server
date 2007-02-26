package org.safehaus.penrose.apacheds;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.apache.directory.shared.ldap.NotImplementedException;

import javax.naming.ReferralException;
import javax.naming.Context;
import javax.naming.NamingException;
import java.util.Hashtable;
import java.util.List;
import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class PenroseReferralException extends ReferralException {

    Logger log = LoggerFactory.getLogger(getClass());

    private final List refs;

    private int index = 0;


    /**
     * @see ReferralException#ReferralException()
     */
    public PenroseReferralException(Collection refs)
    {
        log.debug("Creating referral exception: "+refs);
        this.refs = new ArrayList( refs );
    }


    /**
     * @see ReferralException#ReferralException(java.lang.String)
     */
    public PenroseReferralException(Collection refs, String explanation)
    {
        super( explanation );
        log.debug("Creating referral exception: "+refs);
        this.refs = new ArrayList( refs );
    }


    /**
     * Always returns {@link org.apache.directory.shared.ldap.message.ResultCodeEnum#REFERRAL}
     *
     * @see org.apache.directory.shared.ldap.exception.LdapException#getResultCode()
     */
    public ResultCodeEnum getResultCode()
    {
        return ResultCodeEnum.REFERRAL;
    }


    public Object getReferralInfo()
    {
        Object referral = refs.get( index );
        log.debug("Returning referral #"+index+": "+referral);
        return referral;
    }


    public Context getReferralContext() throws NamingException
    {
        throw new NotImplementedException();
    }


    public Context getReferralContext( Hashtable arg ) throws NamingException
    {
        throw new NotImplementedException();
    }


    public boolean skipReferral()
    {
        index++;
        log.debug("Skipping to referral #"+index);
        return index < refs.size();
    }


    public void retryReferral()
    {
        throw new NotImplementedException();
    }
}
