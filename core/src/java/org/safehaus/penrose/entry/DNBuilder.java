package org.safehaus.penrose.entry;

import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class DNBuilder {

    Logger log = Logger.getLogger(getClass());
    
    public List rdns = new ArrayList();

    public void set(String dn) {
        rdns.clear();
        append(dn);
    }

    public void set(DN dn) {
        rdns.clear();
        append(dn);
    }

    public void set(RDN rdn) {
        rdns.clear();
        rdns.add(rdn);
    }

    public void append(String dn) {
        Collection list = parse(dn);
        for (Iterator i=list.iterator(); i.hasNext(); ) {
            RDN rdn = (RDN)i.next();
            rdns.add(rdn);
        }
    }

    public void append(RDN rdn) {
        rdns.add(rdn);
    }

    public void append(DN dn) {
        Collection list = dn.getRdns();
        for (Iterator i=list.iterator(); i.hasNext(); ) {
            RDN rdn = (RDN)i.next();
            rdns.add(rdn);
        }
    }

    public void prepend(String dn) {
        Collection list = parse(dn);
        for (Iterator i=list.iterator(); i.hasNext(); ) {
            RDN rdn = (RDN)i.next();
            rdns.add(0, rdn);
        }
    }

    public void prepend(RDN rdn) {
        rdns.add(0, rdn);
    }

    public void prepend(DN dn) {
        Collection list = dn.getRdns();
        for (Iterator i=list.iterator(); i.hasNext(); ) {
            RDN rdn = (RDN)i.next();
            rdns.add(0, rdn);
        }
    }

    public boolean isEmpty() {
        return rdns.isEmpty();
    }

    public int getSize() {
        return rdns.size();
    }

    public void clear() {
        rdns.clear();
    }

    public static RDN parseRdn(String rdn) {
        Collection list = parse(rdn);
        if (list.isEmpty()) return null;
        return (RDN)list.iterator().next();
    }
    /**
     * Take a "string" representation of a distinguished name and return an
     * ordered list or relative distingished names.
     * The string is formatted as
     *     <attributeName>=<attributeValue>[+<attributeName>=<attributeValue>],...
     * attributeName is case insensitive.
     * attributeValue is case sensitive. 
     *     Contents may be escaped with backslash to
     *     avoid the special meaning they would have otherwise. 
     *     Or, the value may be wrapped by double quotes in which case
     *     the special characters do not need to be escaped, but the quotes are not
     *     part of the value.
     * A multi-valued RDN will have name=value pairs separated by the '+'
     *     character.
     * @param dn
     * @return a Collection of RDN objects
     */
    public static Collection parse(String dn) {

        Collection list =  new ArrayList();

        if (dn == null || "".equals(dn)) return list;

        char[] value = dn.toCharArray();
        TreeMap map = new TreeMap();
        boolean inName = true;
        boolean sawDQ = false;
        StringBuilder sb = new StringBuilder(100);
        String name = null;
        String val = null;
        for (int i = 0; i < value.length; i++)
        {
        	if (inName)
        	{
        		switch (value[i])
        		{
        		case '=':
        			name = sb.toString().trim();
        			sb.delete(0, sb.length());
        			inName = false;
        			break;
    			default:
        			sb.append(value[i]);
        		}        		
        	}
        	else
        	{
        		switch (value[i])
        		{
        		case '"':
        			sawDQ = true;
        			break;

        		case '+':
        			// an rdn w/multiple attributes
        			val = sb.toString();
        			if (!sawDQ)
        			{
        				val = val.trim();
        			}
        			map.put(name, val);
        			inName = true;
        			sawDQ = false;
        			sb.delete(0, sb.length());
        			break;
        		case ',':
        		case ';':
        			val = sb.toString();
        			if (!sawDQ)
        			{
        				val = val.trim();
        			}
        			map.put(name, val);
        			list.add(new RDN(map));
        			map = new TreeMap();
        			inName = true;
        			sb.delete(0, sb.length());
        			break;
        		case '\\':
        			if (!sawDQ)
        			{
        				// this is an escape - pick up next character
        				switch (value[++i])
        				{
        				case 'a': case 'b': case 'c': case 'd': case 'e': case 'f':
        				case 'A': case 'B': case 'C': case 'D': case 'E': case 'F':
        				case '0': case '1': case '2': case '3': case '4':
        				case '5': case '6': case '7': case '8': case '9':
        					// assume hexpair
        					String x = new String(new char[] { value[i], value[++i] });
        					sb.append((char) Integer.parseInt(x, 16));
        					break;
    					default:
    						// normal escaped special character
    						sb.append(value[i]);	
        				}
        				break;
        			}
        			// else no escapes - fall through and keep the backslash literal
    			default:
    				sb.append(value[i]);	
        		}
        	}
        }
        // a well formed DN will have left us 1 more RDN at the end
		val = sb.toString();
		if (!sawDQ)
		{
			val = val.trim();
		}
		map.put(name, val);
		list.add(new RDN(map));
        
        return list;
    }

    public DN toDn() {
        DN dn = new DN();
        dn.rdns.addAll(rdns);
        return dn;
    }

    public String toString() {
        return toDn().toString();
    }
}
