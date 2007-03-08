package org.safehaus.penrose.naming;

import javax.naming.*;
import java.util.Hashtable;
import java.util.Map;
import java.util.HashMap;

/**
 * @author Endi S. Dewata
 */
public class PenroseContext implements Context {

    Map map = new HashMap();
    
    public Object lookup(Name name) throws NamingException {
        return map.get(name.toString());
    }

    public Object lookup(String name) throws NamingException {
        return map.get(name);
    }

    public void bind(Name name, Object object) throws NamingException {
        map.put(name.toString(), object);
    }

    public void bind(String name, Object object) throws NamingException {
        map.put(name, object);
    }

    public void rebind(Name name, Object object) throws NamingException {
        map.put(name.toString(), object);
    }

    public void rebind(String name, Object object) throws NamingException {
        map.put(name, object);
    }

    public void unbind(Name name) throws NamingException {
        map.remove(name.toString());
    }

    public void unbind(String name) throws NamingException {
        map.remove(name);
    }

    public void rename(Name oldName, Name newName) throws NamingException {
        map.put(newName.toString(), map.get(oldName.toString()));
    }

    public void rename(String oldName, String newName) throws NamingException {
        map.put(newName, map.get(oldName));
    }

    public NamingEnumeration<NameClassPair> list(Name name) throws NamingException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public NamingEnumeration<NameClassPair> list(String name) throws NamingException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public NamingEnumeration<Binding> listBindings(Name name) throws NamingException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public NamingEnumeration<Binding> listBindings(String name) throws NamingException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void destroySubcontext(Name name) throws NamingException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void destroySubcontext(String name) throws NamingException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public Context createSubcontext(Name name) throws NamingException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Context createSubcontext(String name) throws NamingException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Object lookupLink(Name name) throws NamingException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Object lookupLink(String name) throws NamingException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public NameParser getNameParser(Name name) throws NamingException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public NameParser getNameParser(String name) throws NamingException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Name composeName(Name name, Name prefix) throws NamingException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public String composeName(String name, String prefix) throws NamingException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Object addToEnvironment(String propName, Object propVal) throws NamingException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Object removeFromEnvironment(String propName) throws NamingException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Hashtable<?, ?> getEnvironment() throws NamingException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void close() throws NamingException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public String getNameInNamespace() throws NamingException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
