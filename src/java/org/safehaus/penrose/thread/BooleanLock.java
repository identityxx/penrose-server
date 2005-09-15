/** * Copyright (c) 2000-2005, Identyx Corporation.
 * 
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */package org.safehaus.penrose.thread;/** * @author Administrator */public class BooleanLock {	private boolean value;		public BooleanLock(boolean initialValue) {		value = initialValue;	}		public BooleanLock() {		this(false);	}		public synchronized void setValue(boolean newValue) {		if (newValue != value) {			value = newValue;			notifyAll();		}	}		public synchronized boolean waitToSetTrue(long msTimeout) 	throws InterruptedException {		boolean success = waitUntilFalse(msTimeout);		if (success) {			setValue(true);		}		return success;	}		public synchronized boolean waitToSetFalse(long msTimeout)	throws InterruptedException {		boolean success = waitUntilTrue(msTimeout);		if (success) {			setValue(false);		}		return success;	}		public synchronized boolean isTrue() {		return value;	}		public synchronized boolean isFalse() {		return !value;	}		public synchronized boolean waitUntilTrue(long msTimeout) 	throws InterruptedException {		return waitUntilStateIs(true, msTimeout);	}		public synchronized boolean waitUntilFalse(long msTimeout) 	throws InterruptedException {		return waitUntilStateIs(false, msTimeout);	}		public synchronized boolean waitUntilStateIs(boolean state, long msTimeout)	throws InterruptedException {		if (msTimeout == 0L) {			while (value != state) {				wait(); // wait indefinitely until notified			}			// condition has finally been met			return true;		}		// only wait for the specified amount of time		long endTime = System.currentTimeMillis() + msTimeout;		long msRemaining = msTimeout;		while (value != state && msRemaining > 0L) {			wait(msRemaining);			msRemaining = endTime - System.currentTimeMillis();		}		// may have timed out, or may have met value,		// calculate return value		return (value==state);	}}