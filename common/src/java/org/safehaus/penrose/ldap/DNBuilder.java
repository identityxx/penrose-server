package org.safehaus.penrose.ldap;

import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class DNBuilder {

    Logger log = Logger.getLogger(getClass());

    public List<RDN> rdns = new ArrayList<RDN>();

    public void set(String dn) throws Exception {
        rdns.clear();
        append(dn);
    }

    public void set(DN dn) throws Exception {
        rdns.clear();
        append(dn);
    }

    public void set(RDN rdn) {
        rdns.clear();
        rdns.add(rdn);
    }

    public void append(String dn) throws Exception {
        if (dn == null) return;
        Collection<RDN> list = parse(dn);
        for (RDN rdn : list) {
            rdns.add(rdn);
        }
    }

    public void append(RDN rdn) {
        if (rdn == null) return;
        rdns.add(rdn);
    }

    public void append(DN dn) throws Exception {
        if (dn == null) return;
        Collection<RDN> list = dn.getRdns();
        for (RDN rdn : list) {
            rdns.add(rdn);
        }
    }

    public void prepend(String dn) throws Exception {
        if (dn == null) return;
        Collection<RDN> list = parse(dn);
        for (RDN rdn : list) {
            rdns.add(0, rdn);
        }
    }

    public void prepend(RDN rdn) {
        if (rdn == null) return;
        rdns.add(0, rdn);
    }

    public void prepend(DN dn) throws Exception {
        if (dn == null) return;
        Collection<RDN> list = dn.getRdns();
        for (RDN rdn : list) {
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

    public static RDN parseRdn(String rdn) throws Exception {
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
     * @param dn DN
     * @throws java.lang.Exception Invalid DN format
     * @return a Collection of RDN objects
     */
    public static Collection<RDN> parse(String dn) throws Exception {

        Collection<RDN> list = new ArrayList<RDN>();

        if (dn == null || "".equals(dn)) return list;

        char[] chars = dn.toCharArray();
        int start = 0;
        int end = chars.length;

        if (chars[start] == '"' && chars[end-1] == '"') {
            start++; end--;
        }

        Map<String,Object> map = new TreeMap<String,Object>();
        boolean inName = true;
        boolean sawDQ = false;
        boolean inDQ = false;
        StringBuilder sb = new StringBuilder(100);
        String name = null;
        List<Integer> bytes = new ArrayList<Integer>();
        Object val;

        for (int i = start; i < end; i++) {

            if (inName) {

                switch (chars[i]) {
        		case '=':
        			name = sb.toString().trim();
        			sb.setLength(0);
        			inName = false;
        			break;

                default:
        			sb.append(chars[i]);
        		}

            } else {

                switch (chars[i]) {
        		case '"':
        			sawDQ = true;
                    inDQ = !inDQ;                    
                    break;

        		case '+':
                    if (!inDQ) {
            			// an rdn w/multiple attributes
                        if (!bytes.isEmpty()) {
                            val = toByteArray(bytes);
                            bytes.clear();
                        } else {
                            val = sawDQ ? sb.toString() : sb.toString().trim();
                            sb.setLength(0);
                        }
            			map.put(name, val);
            			inName = true;
            			sawDQ = false;
            			break;
                    }

                case ',':
        		case ';':
                    if (!inDQ) {
                        if (!bytes.isEmpty()) {
                            val = toByteArray(bytes);
                            bytes.clear();
                        } else {
                            val = sawDQ ? sb.toString() : sb.toString().trim();
                            sb.setLength(0);
                        }
            			map.put(name, val);
            			list.add(new RDN(map));
            			map = new TreeMap<String,Object>();
            			inName = true;
            			break;
                    }

                case '#':
                    if (!inDQ) {
                        // this is an escape - pick up next character
                        while (i < end && chars[i+1] != '+' && chars[i+1] != ',' && chars[i+1] != ';') {
                            if (isHexDigit(chars[++i]))
                            {
                                // assume hexpair
                                String x = new String(new char[] { chars[i], chars[++i] });
                                bytes.add(Integer.parseInt(x, 16));
                            } else {
                                // invalid hexchar
                            }
                        }
                        break;
                    }

                case '\\':
        			if (!inDQ) {
        				// this is an escape - pick up next character
        				if (isHexDigit(chars[i+1]) && isHexDigit(chars[i+2])) {
        					// Interpret all immediately follwing "\xx" escaped characters
        					// using UTF-8 decoding, as individual decoding of each character
        					// would break for >1 byte UTF-8 sequences.

        					// determine number of escaped characters
        					int counter = 1;
        					int s = i; // points to slash
        					i += 3;
        					while(i < end-2
        							&& chars[i] == '\\'
        							&& isHexDigit(chars[i+1])
        							&& isHexDigit(chars[i+2])) {
        						counter++;
        						i += 3;
        					}

        					// store un-escaped characters in byte buffer for decoding
        					byte utfBytes[] = new byte[counter];

        					i = s;
        					for(int j = 0; j < counter; j++) {
        						String x = new String(new char[] { chars[i+1], chars[i+2] });
        						utfBytes[j] = (byte)Integer.parseInt(x, 16);
        						i += 3;
        					}

                            String str;
                            try {
                                str = new String(utfBytes, "UTF-8");

                            } catch (Exception e) {
                                throw new RuntimeException(e.getMessage(), e);
                            }

                            // decode sequence
                            sb.append(str);

							i--;

        				} else {
                             // normal escaped special character
                            i++;
                             sb.append(chars[i]);
                         }
        				break;
        			}
        			// else no escapes - fall through and keep the backslash literal

                default:
    				sb.append(chars[i]);
        		}
        	}
        }

        if (inName) throw new Exception("Invalid DN format: "+dn);

        // a well formed DN will have left us 1 more RDN at the end
        if (!bytes.isEmpty()) {
            val = toByteArray(bytes);
        } else {
            val = sawDQ ? sb.toString() : sb.toString().trim();
        }

        map.put(name, val);
		list.add(new RDN(map));

        return list;
    }

    private static boolean isHexDigit(char c) {
		return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
	}

    private static byte[] toByteArray(List<Integer> bytes) {
        byte[] ba = new byte[bytes.size()];
        for (int i = 0; i < ba.length; i++ ) {
            ba[i] = bytes.get(i).byteValue();
        }
        return ba;
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
