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
