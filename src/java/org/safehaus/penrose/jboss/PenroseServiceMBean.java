/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.jboss;

import org.jboss.system.Service;
import org.jboss.system.ServiceMBean;

/**
 * @author Endi S. Dewata
 */
public interface PenroseServiceMBean extends Service { //, ServiceMBean {

    public int getPort();
    public void setPort(int port);

    public String getHome();
    public void setHome(String home);
}
