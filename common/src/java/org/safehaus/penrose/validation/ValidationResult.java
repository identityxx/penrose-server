/**
 * Copyright 2009 Red Hat, Inc.
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
package org.safehaus.penrose.validation;

import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class ValidationResult implements Serializable {

    public final static long serialVersionUID = 1L;

    public final static String WARNING = "WARNING";
    public final static String ERROR   = "ERROR";

    public final static int PARTITION  = 1;
    public final static int CONNECTION = 2;
    public final static int SOURCE     = 3;
    public final static int MAPPING    = 4;
    public final static int ENTRY      = 5;
    public final static int MODULE     = 6;

    private String type;
    private String message;
    private String partitionName;
    private int objectType;
    private String objectName;

    public ValidationResult(String type, String message, String partitionName, int objectType, String objectName) {
        this.type = type;
        this.message = message;
        this.partitionName = partitionName;
        this.objectType = objectType;
        this.objectName = objectName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    public int getObjectType() {
        return objectType;
    }

    public void setObjectType(int objectType) {
        this.objectType = objectType;
    }
}
