/**
 * Endi S. Dewata
 */

#ifndef _JAVA_EXTERNAL_H
#define _JAVA_EXTERNAL_H

LDAP_BEGIN_DECL

extern BI_init	java_back_initialize;
extern BI_open	java_back_open;
extern BI_close	java_back_close;
extern BI_destroy	java_back_destroy;

extern BI_db_init	java_back_db_init;
extern BI_db_open	java_back_db_open;
extern BI_db_destroy	java_back_db_destroy;

extern BI_db_config	java_back_db_config;

extern BI_op_bind	java_back_bind;

extern BI_op_search	java_back_search;

extern BI_op_compare	java_back_compare;

extern BI_op_modify	java_back_modify;

extern BI_op_modrdn	java_back_modrdn;

extern BI_op_add	java_back_add;

extern BI_op_delete	java_back_delete;

LDAP_END_DECL

#endif /* _JAVA_EXTERNAL_H */
