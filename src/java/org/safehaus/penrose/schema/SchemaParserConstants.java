/* Generated By:JavaCC: Do not edit this line. SchemaParserConstants.java */
/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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
package org.safehaus.penrose.schema;

public interface SchemaParserConstants {

  int EOF = 0;
  int TRAILING_COMMENT = 5;
  int LPAREN = 6;
  int RPAREN = 7;
  int LBRACE = 8;
  int RBRACE = 9;
  int LBRACKET = 10;
  int RBRACKET = 11;
  int DOT = 12;
  int DOLLAR = 13;
  int QUOTE = 14;
  int NAME = 15;
  int DESC = 16;
  int OBSOLETE = 17;
  int SUP = 18;
  int EQUALITY = 19;
  int ORDERING = 20;
  int SUBSTR = 21;
  int SYNTAX = 22;
  int SINGLE_VALUE = 23;
  int COLLECTIVE = 24;
  int NO_USER_MODIFICATION = 25;
  int USAGE = 26;
  int OBJECTCLASS = 27;
  int ATTRIBUTETYPE = 28;
  int ABSTRACT = 29;
  int STRUCTURAL = 30;
  int AUXILIARY = 31;
  int MUST = 32;
  int MAY = 33;
  int X_PARAMETER = 34;
  int USER_APPLICATIONS = 35;
  int DIRECTORY_OPERATION = 36;
  int DISTRIBUTED_OPERATION = 37;
  int DSA_OPERATION = 38;
  int DSA_OPERATION2 = 39;
  int DIGIT = 40;
  int IDENT = 41;
  int QDSTRING = 42;

  int DEFAULT = 0;

  String[] tokenImage = {
    "<EOF>",
    "\" \"",
    "\"\\t\"",
    "\"\\n\"",
    "\"\\r\"",
    "<TRAILING_COMMENT>",
    "\"(\"",
    "\")\"",
    "\"{\"",
    "\"}\"",
    "\"[\"",
    "\"]\"",
    "\".\"",
    "\"$\"",
    "\"\\\'\"",
    "\"NAME\"",
    "\"DESC\"",
    "\"OBSOLETE\"",
    "\"SUP\"",
    "\"EQUALITY\"",
    "\"ORDERING\"",
    "\"SUBSTR\"",
    "\"SYNTAX\"",
    "\"SINGLE-VALUE\"",
    "\"COLLECTIVE\"",
    "\"NO-USER-MODIFICATION\"",
    "\"USAGE\"",
    "<OBJECTCLASS>",
    "<ATTRIBUTETYPE>",
    "\"ABSTRACT\"",
    "\"STRUCTURAL\"",
    "\"AUXILIARY\"",
    "\"MUST\"",
    "\"MAY\"",
    "<X_PARAMETER>",
    "\"userApplications\"",
    "\"directoryOperation\"",
    "\"distributedOperation\"",
    "\"dSAOperation\"",
    "\"dsaOperation\"",
    "<DIGIT>",
    "<IDENT>",
    "<QDSTRING>",
  };

}
