/**
 * Copyright (c) 2000-2006, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.ldap;

import org.safehaus.penrose.session.PenroseSearchResults;
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPDN;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.directory.shared.ldap.exception.LdapReferralException;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.ReferralException;
import javax.naming.directory.*;
import java.util.Collection;
import java.util.List;

/**
 * @author Endi S. Dewata
 */
public class PenroseEnumeration implements NamingEnumeration {

    Logger log = LoggerFactory.getLogger(getClass());

    public PenroseSearchResults searchResults;

    public PenroseEnumeration(PenroseSearchResults searchResults) {
        this.searchResults = searchResults;
    }

    public void close() throws NamingException {
        searchResults.close();
    }

    public boolean hasMore() throws NamingException {

        boolean hasNext = searchResults.hasNext();
        if (hasNext) return true;

        List referrals = searchResults.getReferrals();
        log.debug("Search operation returned "+referrals.size()+" referral(s).");

        if (!referrals.isEmpty()) {
            PenroseReferralException lre = new PenroseReferralException(referrals);
            throw lre;
            /*
            String referral = (String)referrals.remove(0);
            log.debug("Referral: "+referral);
            throw new PenroseReferralException(referral, !referrals.isEmpty());
            */
        }

        log.warn("Search operation returned "+searchResults.getTotalCount()+" entries.");

        int rc = searchResults.getReturnCode();
        if (rc != LDAPException.SUCCESS) {
            throw ExceptionTool.createNamingException(rc, "RC: "+rc);
        }

        return false;
    }

    public Object next() throws NamingException {
        SearchResult result = (SearchResult)searchResults.next();
        log.info("Returning \""+result.getName()+"\" to client.");

        return result;
    }

    public boolean hasMoreElements() {
        try {
            return hasMore();
        } catch (Exception e) {
            return false;
        }
    }

    public Object nextElement() {
        try {
            return next();
        } catch (Exception e) {
            return null;
        }
    }
}
