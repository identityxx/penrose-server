package org.safehaus.penrose.naming;

import javax.naming.spi.InitialContextFactory;
import javax.naming.Context;
import javax.naming.NamingException;
import java.util.Hashtable;

/**
 * @author Endi S. Dewata
 */
public class PenroseInitialContextFactory implements InitialContextFactory {

    public final static PenroseContext context = new PenroseContext();

    public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException {
        return context;
    }
}
