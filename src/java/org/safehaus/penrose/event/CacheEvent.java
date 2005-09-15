/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.event;

import org.safehaus.penrose.mapping.SourceDefinition;


/**
 * @author Endi S. Dewata
 */
public class CacheEvent extends Event {

    public final static int BEFORE_LOAD_ENTRIES = 0;
    public final static int AFTER_LOAD_ENTRIES = 1;

    private SourceDefinition sourceConfig;

    public CacheEvent(Object source, SourceDefinition sourceConfig, int type) {
        super(source, type);
        this.sourceConfig = sourceConfig;
    }

    public SourceDefinition getSourceConfig() {
        return sourceConfig;
    }

    public void setSourceConfig(SourceDefinition sourceConfig) {
        this.sourceConfig = sourceConfig;
    }
}
