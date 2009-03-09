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
package org.safehaus.penrose.interpreter;

import bsh.Interpreter;
import bsh.Parser;
import bsh.ParserConstants;
import bsh.NameSpace;

import java.io.StringReader;
import java.util.*;

import org.safehaus.penrose.Penrose;

/**
 * @author Endi S. Dewata
 */
public class DefaultInterpreter extends org.safehaus.penrose.interpreter.Interpreter {

    public Map<String,Object> variables = new HashMap<String,Object>();
    public Interpreter interpreter;

    public DefaultInterpreter() {
    }

    public Collection parse(String script) throws Exception {
        List<Token> tokens = new ArrayList<Token>();
        try {
            Parser parser = new Parser(new StringReader(script+";"));
            //log.debug("Parsing: "+script);
            bsh.Token token = parser.getNextToken();
            while (token != null && !"".equals(token.image)) {
                //log.debug(" - ["+token.image+"] ("+token.kind+")");

                if (token.kind == ParserConstants.IDENTIFIER) {
                    tokens.add(new Token(token.image, Token.IDENTIFIER));

                } else if (token.kind == ParserConstants.STRING_LITERAL) {
                    tokens.add(new Token(token.image, Token.STRING_LITERAL));

                } else if (token.kind == ParserConstants.DOT) {
                    tokens.add(new Token(token.image, Token.DOT));

                } else {
                    tokens.add(new Token(token.image, Token.OTHER));
                }

                token = parser.getNextToken();
            }

            tokens.remove(tokens.size()-1);

        } catch (Exception e) {
            Penrose.errorLog.error(e.getMessage(), e);
        }
        return tokens;
    }

    public Collection<String> parseVariables(String script) throws Exception {
        Collection<String> tokens = new ArrayList<String>();
        try {
            Parser parser = new Parser(new StringReader(script+";"));
            //log.debug("Parsing: "+script);
            bsh.Token token = parser.getNextToken();
            while (token != null && !"".equals(token.image)) {
                //log.debug(" - ["+token.image+"] ("+token.kind+")");
                if (token.kind == ParserConstants.IDENTIFIER) {
                    tokens.add(token.image);
                }
                token = parser.getNextToken();
            }

        } catch (Exception e) {
            Penrose.errorLog.error(e.getMessage(), e);
        }
        return tokens;
    }

    public void set(String name, Object value) throws Exception {
        if (interpreter != null) {
            interpreter.set(name, value);
        }
        variables.put(name, value);
    }

    public Object get(String name) throws Exception {
        if (interpreter != null) {
            int i = name.indexOf(".");
            if (i >= 0) {
                String object = name.substring(0, i);
                if (interpreter.get(object) == null) return null;
            }
            return interpreter.get(name);
        }
        return variables.get(name);
    }

    public Object eval(String script) throws Exception {
        try {
            if (script == null) return null;
            if (interpreter == null) {
                //log.debug("###################################################################");
                //log.debug("# NEW INTERPRETER");

                interpreter = new Interpreter();
                interpreter.setClassLoader(classLoader);
                
                for (Iterator i=variables.keySet().iterator(); i.hasNext(); ) {
                    String name = (String)i.next();
                    Object value = variables.get(name);
                    interpreter.set(name, value);
                    //log.debug("# - "+name+": "+value);
                }
                //log.debug("###################################################################");
            }
            return interpreter.eval(script);

        } catch (Exception e) {
            //log.debug("BeanShellException: "+e.getMessage(), e);
            throw e;
            //return null;
        }
    }

    public void clear() throws Exception {
        //log.debug("Clearing interpreter:");
        if (interpreter != null) {
            NameSpace ns = interpreter.getNameSpace();
/*
            log.debug("BeanShell names:");
            String names[] = ns.getAllNames();
            for (int i=0; i<names.length; i++) {
                log.debug(" - "+names[i]);
            }
*/
            //log.debug("BeanShell methods:");
            String methodNames[] = ns.getMethodNames();
            for (int i=0; i<methodNames.length; i++) {
                //log.debug(" - "+methodNames[i]);
                interpreter.unset(methodNames[i]);
            }

            //log.debug("BeanShell variables:");
            String variableNames[] = ns.getVariableNames();
            for (int i=0; i<variableNames.length; i++) {
                //log.debug(" - "+variableNames[i]+": "+interpreter.get(variableNames[i]));
                interpreter.unset(variableNames[i]);
            }
        }
/*
        log.debug("Variables:");
        for (Iterator i=variables.keySet().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            log.debug(" - "+name);
        }
*/
        variables.clear();
    }
}
