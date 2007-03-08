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
package org.safehaus.penrose.session;

/**
 * @author Endi S. Dewata
 */
public interface ResultsListener {

    public boolean preAdd(ResultsEvent event) throws Exception;
    public void postAdd(ResultsEvent event) throws Exception;

    public boolean preRemove(ResultsEvent event) throws Exception;
    public void postRemove(ResultsEvent event) throws Exception;

    public boolean preClose(ResultsEvent event) throws Exception;
    public void postClose(ResultsEvent event) throws Exception;

}
