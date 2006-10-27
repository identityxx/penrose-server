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
package org.safehaus.penrose.source;

/**
 * @author Endi S. Dewata
 */
public interface FieldConfigMBean {

    public String getName() throws Exception;
    public void setName(String name) throws Exception;

    public String getPrimaryKey() throws Exception;
    public void setPrimaryKey(String primaryKey) throws Exception;

    public String getOriginalName() throws Exception;
    public void setOriginalName(String originalName) throws Exception;

    public String getType() throws Exception;
    public void setType(String type) throws Exception;

    public int getLength() throws Exception;
    public void setLength(int length) throws Exception;

    public int getPrecision() throws Exception;
    public void setPrecision(int precision) throws Exception;

    public boolean isSearchable() throws Exception;
    public void setSearchable(boolean searchable) throws Exception;

    public boolean isUnique() throws Exception;
    public void setUnique(boolean unique) throws Exception;

    public boolean isIndex() throws Exception;
    public void setIndex(boolean index) throws Exception;
}
