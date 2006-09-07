package org.safehaus.penrose.service;

import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.Penrose;

/**
 * @author Endi S. Dewata
 */
public interface ServiceContext {

    public PenroseConfig getPenroseConfig();
    public Penrose getPenrose();
}
