/**
 * @author Endi S. Dewata
 */

#include "portable.h"
#include <stdio.h>

#include "slap.h"
#include "lber.h"

#include "java_back.h"

int java_back_add( Operation *op, SlapReply *rs ) {

    Debug( LDAP_DEBUG_TRACE, "==> java_back_add()\n", 0, 0, 0);

    JavaBackend *java_back = (JavaBackend *)op->o_bd->be_private;
    JavaVM *jvm = java_back->jvm;
    JNIEnv *env;

    Connection *conn = (Connection *)op->o_conn;
    Debug( LDAP_DEBUG_TRACE, "Connection  : %d\n", conn->c_connid, 0, 0 );

    Entry *e = (Entry *)op->ora_e;

    Debug( LDAP_DEBUG_TRACE, "Adding %s\n", e->e_name.bv_val, 0, 0);

    jint res;
  
    jobject attributes;

    Attribute *a;

    res = (*jvm)->AttachCurrentThread(jvm, (void**)&env, NULL);

    if (res < 0) {

        Debug( LDAP_DEBUG_TRACE, "<== java_back_add(): Failed attaching to JVM.\n", 0, 0, 0);

        rs->sr_err = LDAP_OPERATIONS_ERROR;
        send_ldap_result( op, rs);
        return LDAP_OPERATIONS_ERROR;
    }

    jstring dn = (*env)->NewStringUTF(env, e->e_name.bv_val);

    Debug( LDAP_DEBUG_TRACE, "BasicAttributes basicAttributes = new BasicAttributes();\n", 0, 0, 0);

    attributes = (*env)->NewObject(env, java_back->basicAttributesClass, java_back->basicAttributesConstructor);

    if (exceptionOccurred(env)) {

        Debug( LDAP_DEBUG_TRACE, "<== java_back_add(): Failed creating BasicAttributes.\n", 0, 0, 0);

        rs->sr_err = LDAP_OPERATIONS_ERROR;
        send_ldap_result( op, rs);
        return LDAP_OPERATIONS_ERROR;
    }       

    a = e->e_attrs;
    while (a) {
        jobject attribute;
        jstring attributeName;

        AttributeDescription *desc;
        BerValue *vals;

        desc = a->a_desc;

        //fprintf(stderr, "desc cname: %s\n", desc->ad_cname.bv_val);
        //fprintf(stderr, "desc tags: %s\n", desc->ad_tags.bv_val);

        //AttributeType *type = desc->ad_type;
        //fprintf(stderr, "type cname: %s\n", type->sat_cname.bv_val);
        //fprintf(stderr, "ldap type oid: %s\n", type->sat_atype.at_oid);
        //fprintf(stderr, "ldap type desc: %s\n", type->sat_atype.at_desc);

        Debug( LDAP_DEBUG_TRACE, "Attribute attribute = new BasicAttribute(\"%s\");\n", desc->ad_cname.bv_val, 0, 0);

        attributeName = (*env)->NewStringUTF(env, desc->ad_cname.bv_val);
        attribute = (*env)->NewObject(env, java_back->basicAttributeClass, java_back->basicAttributeConstructor, attributeName);

        if (exceptionOccurred(env)) {
            Debug( LDAP_DEBUG_TRACE, "<== java_back_add(): Failed creating BasicAttribute.\n", 0, 0, 0);

            rs->sr_err = LDAP_OPERATIONS_ERROR;
            send_ldap_result( op, rs);
            return LDAP_OPERATIONS_ERROR;
        }       

        vals = a->a_vals;

        while (vals && vals->bv_val) {
            jstring attributeValue;

            Debug( LDAP_DEBUG_TRACE, "attribute.add(\"%s\");\n", vals->bv_val, 0, 0);

            attributeValue = (*env)->NewStringUTF(env, vals->bv_val);
            (*env)->CallBooleanMethod(env, attribute, java_back->attributeAdd, attributeValue);

            if (exceptionOccurred(env)) {
                if (slap_debug & 1024) fprintf(stderr, "Failed to add attribute value.\n");

                Debug( LDAP_DEBUG_TRACE, "<== java_back_add(): Failed adding attribute value.\n", 0, 0, 0);

                rs->sr_err = LDAP_OPERATIONS_ERROR;
                send_ldap_result( op, rs );
                return LDAP_OPERATIONS_ERROR;
            }       

            vals++;
        }

        Debug( LDAP_DEBUG_TRACE, "attributes.put(attribute);\n", 0, 0, 0);

        (*env)->CallBooleanMethod(env, attributes, java_back->attributesPut, attribute);

        if (exceptionOccurred(env)) {

            Debug( LDAP_DEBUG_TRACE, "<== java_back_add(): Failed adding attribute to set.\n", 0, 0, 0);

            rs->sr_err = LDAP_OPERATIONS_ERROR;
            send_ldap_result( op, rs );
            return LDAP_OPERATIONS_ERROR;
        }       

        a = a->a_next;
    }

    Debug( LDAP_DEBUG_TRACE, "backend.add(dn, attributes);\n", 0, 0, 0);

    res = (*env)->CallIntMethod(env, java_back->backend, java_back->backendAdd, conn->c_connid, dn, attributes);

    if (exceptionOccurred(env)) {

        Debug( LDAP_DEBUG_TRACE, "<== java_back_add(): Failed adding entry %s.\n", dn, 0, 0);

        rs->sr_err = LDAP_OPERATIONS_ERROR;
        send_ldap_result( op, rs );
        return LDAP_OPERATIONS_ERROR;
    }       

    send_ldap_result( op, rs );

    return 0;
}

