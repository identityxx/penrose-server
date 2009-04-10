/**
 * @author Endi S. Dewata
 */

#ifndef _JAVA_EXTERNAL_H
#define _JAVA_EXTERNAL_H

LDAP_BEGIN_DECL

extern BI_init       java_back_initialize;
extern BI_open       java_backend_open;
extern BI_close      java_backend_close;
extern BI_config     java_backend_config;
extern BI_destroy    java_backend_destroy;

extern BI_db_init    java_backend_db_init;
extern BI_db_open    java_backend_db_open;
extern BI_db_destroy java_backend_db_destroy;
extern BI_db_config  java_backend_db_config;
extern BI_db_close   java_backend_db_close;

extern BI_op_bind    java_backend_bind;
extern BI_op_unbind  java_backend_unbind;

extern BI_op_search  java_backend_search;
extern BI_op_compare java_backend_compare;

extern BI_op_modify  java_backend_modify;
extern BI_op_modrdn  java_backend_modrdn;
extern BI_op_add     java_backend_add;
extern BI_op_delete  java_backend_delete;

extern BI_connection_init    java_connection_init;
extern BI_connection_destroy java_connection_destroy;

// extern BI_acl_group     java_acl_group;
// extern BI_acl_attribute java_acl_attribute;

LDAP_END_DECL

#endif /* _JAVA_EXTERNAL_H */
