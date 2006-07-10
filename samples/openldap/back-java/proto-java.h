/**
 * @author Endi S. Dewata
 */

#ifndef _PROTO_JAVA_H
#define _PROTO_JAVA_H

LDAP_BEGIN_DECL

extern BI_init		java_back_initialize;

extern BI_open		java_back_open;
extern BI_close		java_back_close;

extern BI_config	java_back_config;
extern BI_destroy	java_back_destroy;

extern BI_db_init	java_back_db_init;
extern BI_db_open	java_back_db_open;
extern BI_db_destroy	java_back_db_destroy;
extern BI_db_config	java_back_db_config;

extern BI_db_close	java_back_db_close;

extern BI_op_bind	java_back_bind;
extern BI_op_unbind	java_back_unbind;

extern BI_op_search	java_back_search;
extern BI_op_compare	java_back_compare;

extern BI_op_modify	java_back_modify;
extern BI_op_modrdn	java_back_modrdn;
extern BI_op_add	java_back_add;
extern BI_op_delete	java_back_delete;

extern BI_connection_init	java_connection_init;
extern BI_connection_destroy	java_connection_destroy;

// extern BI_acl_group	java_acl_group;
// extern BI_acl_attribute	java_acl_attribute;

LDAP_END_DECL

#endif /* _PROTO_JAVA_H */

