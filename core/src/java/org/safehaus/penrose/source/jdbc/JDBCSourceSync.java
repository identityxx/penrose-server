package org.safehaus.penrose.source.jdbc;

import org.safehaus.penrose.changelog.ChangeLogUtil;
import org.safehaus.penrose.partition.FieldConfig;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class JDBCSourceSync extends org.safehaus.penrose.source.SourceSync {

    public ChangeLogUtil createChangeLogUtil() throws Exception {
        return new JDBCChangeLogUtil();
    }

    public void generateCreateTable(String tableName, Collection primaryKeyFieldConfigs) throws Exception {
        System.out.println("create table "+tableName+"_changelog (");
        System.out.println("    changeNumber integer auto_increment,");
        System.out.println("    changeTime datetime,");
        System.out.println("    changeAction varchar(10),");
        System.out.println("    changeUser varchar(10),");

        for (Iterator i=primaryKeyFieldConfigs.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            System.out.println("    "+fieldConfig.getName()+" "+fieldConfig.getType()+",");
        }

        System.out.println("    primary key (changeNumber)");
        System.out.println(");");
    }

    public void generateAddTrigger(String tableName, Collection primaryKeyFieldConfigs) throws Exception {
        System.out.println("create trigger "+tableName+"_add after insert on "+tableName);
        System.out.println("for each row insert into "+tableName+"_changelog values (");
        System.out.println("    null,");
        System.out.println("    now(),");
        System.out.println("    'ADD',");
        System.out.println("    substring_index(user(),_utf8'@',1),");

        for (Iterator i=primaryKeyFieldConfigs.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            System.out.print("    new."+fieldConfig.getName());
            if (i.hasNext()) System.out.print(",");
            System.out.println();
        }

        System.out.println(");");
    }

    public void generateModifyTrigger(String tableName, Collection primaryKeyFieldConfigs) throws Exception {
        System.out.println("delimiter |");
        System.out.println("create trigger "+tableName+"_modify after update on "+tableName);
        System.out.println("for each row begin");

        System.out.print("    if ");
        for (Iterator i=primaryKeyFieldConfigs.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            System.out.print("new."+fieldConfig.getName()+" = old."+fieldConfig.getName());
            if (i.hasNext()) System.out.print(" and ");
        }
        System.out.println(" then");

        System.out.println("        insert into "+tableName+"_changelog values (");
        System.out.println("            null,");
        System.out.println("            now(),");
        System.out.println("            'MODIFY',");
        System.out.println("            substring_index(user(),_utf8'@',1),");

        for (Iterator i=primaryKeyFieldConfigs.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            System.out.print("            new."+fieldConfig.getName());
            if (i.hasNext()) System.out.print(",");
            System.out.println();
        }

        System.out.println("        );");
        System.out.println("    else");
        System.out.println("        insert into "+tableName+"_changelog values (");
        System.out.println("            null,");
        System.out.println("            now(),");
        System.out.println("            'DELETE',");
        System.out.println("            substring_index(user(),_utf8'@',1),");

        for (Iterator i=primaryKeyFieldConfigs.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            System.out.print("            old."+fieldConfig.getName());
            if (i.hasNext()) System.out.print(",");
            System.out.println();
        }

        System.out.println("        );");
        System.out.println("        insert into "+tableName+"_changelog values (");
        System.out.println("            null,");
        System.out.println("            now(),");
        System.out.println("            'ADD',");
        System.out.println("            substring_index(user(),_utf8'@',1),");

        for (Iterator i=primaryKeyFieldConfigs.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            System.out.print("            new."+fieldConfig.getName());
            if (i.hasNext()) System.out.print(",");
            System.out.println();
        }

        System.out.println("        );");
        System.out.println("    end if;");
        System.out.println("end;|");
        System.out.println("delimiter ;");
    }

    public void generateDeleteTrigger(String tableName, Collection primaryKeyFieldConfigs) throws Exception {
        System.out.println("create trigger "+tableName+"_delete after delete on "+tableName);
        System.out.println("for each row insert into "+tableName+"_changelog values (");
        System.out.println("    null,");
        System.out.println("    now(),");
        System.out.println("    'DELETE',");
        System.out.println("    substring_index(user(),_utf8'@',1),");

        for (Iterator i=primaryKeyFieldConfigs.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            System.out.print("    old."+fieldConfig.getName());
            if (i.hasNext()) System.out.print(",");
            System.out.println();
        }

        System.out.println(");");
    }

}
