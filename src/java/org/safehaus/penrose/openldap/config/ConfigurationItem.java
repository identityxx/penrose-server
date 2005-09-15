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
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */package org.safehaus.penrose.openldap.config;/** */public class ConfigurationItem {	protected String originalText;	protected String modifiedText;		/**	 * 	 */	public ConfigurationItem() {		super();	}		public ConfigurationItem(String originalText) {		this.originalText = originalText;		// this.modifiedText = originalText;	}		public String toString() {		// return modifiedText;		if (modifiedText == null) {			return originalText;		} else {			return modifiedText;		}	}	}