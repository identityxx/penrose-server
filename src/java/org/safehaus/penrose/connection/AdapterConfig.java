/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.connection;

import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class AdapterConfig implements Serializable {

    private String adapterName;
    private String adapterClass;
    private String description;

    public String getAdapterClass() {
        return adapterClass;
    }

    public void setAdapterClass(String adapterClass) {
        this.adapterClass = adapterClass;
    }

    public String getAdapterName() {
        return adapterName;
    }

    public void setAdapterName(String adapterName) {
        this.adapterName = adapterName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
