package org.safehaus.penrose.interpreter;

import bsh.Interpreter;
import bsh.Token;
import bsh.Parser;
import bsh.ParserConstants;

import java.io.StringReader;
import java.util.Collection;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Endi S. Dewata
 */
public class DefaultInterpreter extends org.safehaus.penrose.interpreter.Interpreter {

    Logger log = LoggerFactory.getLogger(getClass());

    public Interpreter interpreter;

    public DefaultInterpreter() {
        interpreter = new Interpreter();
    }

    public Collection parseVariables(String script) throws Exception {
        Collection tokens = new ArrayList();
        try {
            Parser parser = new Parser(new StringReader(script+";"));
            //log.debug("Parsing: "+script);
            Token token = parser.getNextToken();
            while (token != null && !"".equals(token.image)) {
                //log.debug(" - ["+token.image+"] ("+token.kind+")");
                if (token.kind == ParserConstants.IDENTIFIER) {
                    tokens.add(token.image);
                }
                token = parser.getNextToken();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return tokens;
    }

    public void set(String name, Object value) throws Exception {
        interpreter.set(name, value);
    }

    public Object get(String name) throws Exception {
        return interpreter.get(name);
    }

    public Object eval(String expression) throws Exception {
        try {
            if (expression == null) return null;
            return interpreter.eval(expression);

        } catch (Exception e) {
            log.debug("BeanShellException: "+e.getMessage());
            return null;
        }
    }
}
