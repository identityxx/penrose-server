/**
 * @author Endi S. Dewata
 */

#include "portable.h"
#include <stdio.h>
#include "slap.h"
#include "java-backend.h"

Entry* createEntry(JNIEnv *env, jobject searchResult);

int java_backend_search(Operation *op, SlapReply *rs) { 

    DEBUG("==> java_backend_search()\n");

    jint res = LDAP_SUCCESS;

    jlong connectionId = op->o_conn->c_connid;
    DEBUG1("Connection ID: %d\n", connectionId);

    ////////////////////////////////////////////////////////////////////////////////
    // JVM
    ////////////////////////////////////////////////////////////////////////////////

    DEBUG("Attaching to JVM.\n");

    JNIEnv *env;
    res = (*jvm)->AttachCurrentThread(jvm, (void**)&env, NULL);

    if (res < 0) {
        DEBUG("Failed attaching to JVM.\n");
        res = LDAP_OPERATIONS_ERROR;
        goto send_result;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Parameters
    ////////////////////////////////////////////////////////////////////////////////

    char *baseDn = op->o_req_dn.bv_val;
    char *filter = op->ors_filterstr.bv_val;
    jint scope = op->ors_scope;

    DEBUG1("Base DN: %s\n", baseDn);
    DEBUG1("Filter: %s\n", filter);

    switch (scope) {
    case LDAP_SCOPE_BASE:
        DEBUG1("Scope: base (%d)\n", scope);
        break;
    case LDAP_SCOPE_ONELEVEL:
        DEBUG1("Scope: onelevel (%d)\n", scope);
        break;
    case LDAP_SCOPE_SUBTREE:
        DEBUG1("Scope: subtree (%d)\n", scope);
        break;
    }

    AttributeName *attrs = op->ors_attrs;
    int attributesOnly   = op->ors_attrsonly;

    ////////////////////////////////////////////////////////////////////////////////
    // Conversion
    ////////////////////////////////////////////////////////////////////////////////

    jstring jbaseDn = (*env)->NewStringUTF(env, baseDn);
    jstring jfilter = (*env)->NewStringUTF(env, filter);

    jint sizeLimit = op->ors_slimit;
    jint timeLimit = op->ors_tlimit;

    DEBUG1("Size Limit: %d\n", sizeLimit);
    DEBUG1("Time Limit: %d\n", timeLimit);

    DEBUG("SearchControls sc = new SearchControls();\n");
    jobject searchRequest = (*env)->NewObject(env, SearchControls.class, SearchControls.constructor);

    if (searchRequest == 0) {
        DEBUG("Failed creating search controls.\n");
        res = LDAP_OPERATIONS_ERROR;
        goto send_result;
    }

    DEBUG1("sc.setSearchScope(%d);\n", scope);
    (*env)->CallVoidMethod(env, searchRequest, SearchControls.setSearchScope, scope);

    DEBUG1("sc.setCountLimit(%d);\n", sizeLimit);
    (*env)->CallVoidMethod(env, searchRequest, SearchControls.setCountLimit, sizeLimit);

    DEBUG1("sc.setTimeLimit(%d);\n", timeLimit);
    (*env)->CallVoidMethod(env, searchRequest, SearchControls.setTimeLimit, timeLimit);

    DEBUG("ArrayList attrs = new ArrayList();\n");
    jobject jattributes = (*env)->NewObject(env, ArrayList.class, ArrayList.constructor);

    AttributeName *a = attrs;
    while (a && a->an_name.bv_val) {

        char *attributeName = a->an_name.bv_val;
        jstring jattributeName = (*env)->NewStringUTF(env, attributeName);

        DEBUG1("attrs.add(\"%s\")\n", attributeName);
        (*env)->CallBooleanMethod(env, jattributes, ArrayList.add, jattributeName);

        a++;
    }

    DEBUG("String[] attributeNames = attributes.toArray();\n");
    jobjectArray attributeNames = (*env)->CallObjectMethod(env, jattributes, ArrayList.toArray);

    DEBUG("sc.setReturningAttributes(attrs);\n");
    (*env)->CallVoidMethod(env, searchRequest, SearchControls.setReturningAttributes, attributeNames);

    ////////////////////////////////////////////////////////////////////////////////
    // Session
    ////////////////////////////////////////////////////////////////////////////////

    DEBUG1("Session session = backend.getSession(%d);\n", connectionId);

    jobject session = (*env)->CallObjectMethod(env,
        backend, java_backend_Backend.getSession, connectionId
    );

    if ((*env)->ExceptionOccurred(env)) {
        DEBUG("Failed getting session.\n");
        res = LDAP_OPERATIONS_ERROR;
        goto exception_handler;
    }

    if (session == NULL) {
        DEBUG1("session = backend.createSession(%d);\n", connectionId);

        session = (*env)->CallObjectMethod(env,
            backend, java_backend_Backend.createSession, connectionId
        );

        if ((*env)->ExceptionOccurred(env)) {
            DEBUG("Failed creating session.\n");
            res = LDAP_OPERATIONS_ERROR;
            goto exception_handler;
        }

        if (session == NULL) {
            DEBUG("Failed creating session.\n");
            res = LDAP_OPERATIONS_ERROR;
            goto send_result;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////

    DEBUG2(
        "Results response = backend.search(connectionId, \"%s\", \"%s\", searchRequest);\n",
        baseDn, filter
    );

    jobject response = (*env)->CallObjectMethod(env,
        session, Session.search,
        jbaseDn, jfilter, searchRequest
    );

    if ((*env)->ExceptionOccurred(env)) {
        DEBUG("Exception occured.\n");
        res = LDAP_OPERATIONS_ERROR;
        goto exception_handler;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Search Results
    ////////////////////////////////////////////////////////////////////////////////

    DEBUG("boolean hasNext = response.hasNext();\n");
    int hasNext = (*env)->CallBooleanMethod(env, response, Results.hasNext);
    DEBUG1("hasNext: %d\n", hasNext);

    int nentries = 0;

    while (hasNext) {

        DEBUG("SearchResult sr = (SearchResult)response.next();\n");
        jobject searchResult = (*env)->CallObjectMethod(env, response, Results.next);

        Entry *entry = createEntry(env, searchResult);

        int isReferral = is_entry_referral(entry);
        DEBUG1("isReferral: %d\n", isReferral);

        if (isReferral) {

            BerVarray erefs = get_entry_referrals(op, entry);
            rs->sr_ref = referral_rewrite(erefs,
                &entry->e_name, NULL,
                scope == LDAP_SCOPE_ONELEVEL
                    ? LDAP_SCOPE_BASE
                    : LDAP_SCOPE_SUBTREE);

            //DEBUG("Sending Referral.\n");
            send_search_reference(op, rs);

            ber_bvarray_free(rs->sr_ref);
            rs->sr_ref = NULL;

        } else {

            rs->sr_entry = entry;
            rs->sr_attrs = attrs;
            rs->sr_flags = REP_ENTRY_MODIFIABLE;
            rs->sr_err = LDAP_SUCCESS;

            //DEBUG("Sending Entry.\n");
            rs->sr_err = send_search_entry(op, rs);

            if (rs->sr_err == LDAP_SIZELIMIT_EXCEEDED) {
                rs->sr_entry = NULL;
                exit;
            }
        }

        //DEBUG("Destroying Entry.\n");
        entry_free(entry);

        DEBUG("hasNext = response.hasNext();\n");
        hasNext = (*env)->CallBooleanMethod(env, response, Results.hasNext);
        //DEBUG1("hasNext: %d\n", hasNext);

        nentries++;
    }

    DEBUG("int rc = sr.getReturnCode();\n");
    res = (*env)->CallIntMethod(env, response, Results.getReturnCode);

    DEBUG1("rc: %d\n", res);

    goto send_result;

exception_handler:
    (*env)->ExceptionDescribe(env);
    (*env)->ExceptionClear(env);

send_result:
    rs->sr_err = res;
    send_ldap_result(op, rs);

detach_jvm:
    DEBUG("Detaching JVM.\n");
    (*jvm)->DetachCurrentThread(jvm);

exit:
    DEBUG("<== java_backend_compare()\n");
    return LDAP_SUCCESS;
}

Entry* createEntry(JNIEnv *env, jobject searchResult) {

    DEBUG("==> createEntry()\n");

    Entry *e = (Entry *)ch_calloc(1, sizeof(Entry));
    if (e == NULL) goto exit;

    e->e_id = NOID;

    DEBUG("String dn = sr.getName();\n");
    jstring jdn = (*env)->CallObjectMethod(env, searchResult, SearchResult.getName);
    char *dn = (*env)->GetStringUTFChars(env, jdn, 0);

    struct berval vals[2];
    vals[0].bv_val = dn;
    vals[0].bv_len = strlen(dn);

    DEBUG1("dn: %s\n", dn);

    int rc = dnPrettyNormal(NULL, &vals[0], &e->e_name, &e->e_nname, NULL);

    (*env)->ReleaseStringUTFChars(env, jdn, dn);

    if (rc != LDAP_SUCCESS) {
        DEBUG("Invalid dn.\n");
        entry_free(e);
        return NULL;
    }

    vals[1].bv_len = 0;
    vals[1].bv_val = NULL;

    DEBUG("Attributes attributes = sr.getAttributes();\n");
    jobject jattributes = (*env)->CallObjectMethod(env, searchResult, SearchResult.getAttributes);

    DEBUG("NamingEnumeration ne = attributes.getAll();\n");
    jobject jiterator = (*env)->CallObjectMethod(env, jattributes, Attributes.getAll);

    DEBUG("boolean hasMore = ne.hasMore();\n");
    int hasMore = (*env)->CallBooleanMethod(env, jiterator, NamingEnumeration.hasMore);
    //DEBUG1("hasMore: %d\n", hasMore);

    while (hasMore) {

        jsize len;
        int i;

        DEBUG("Attribute attribute = (Attribute)ne.next();\n");
    	jobject jattribute = (*env)->CallObjectMethod(env, jiterator, NamingEnumeration.next);

        jstring jname = (*env)->CallObjectMethod(env, jattribute, java_backend_Attribute.getID);
        const char *name = (char *)(*env)->GetStringUTFChars(env, jname, 0);

        AttributeDescription *ad = NULL;
        const char *text;

        rc = slap_str2ad(name, &ad, &text);
        DEBUG2("slap_str2ad(%s): %s\n", name, text);

        if( rc != LDAP_SUCCESS ) {

            DEBUG2("slap_str2ad(%s) => undefined attribute\n", name, rc);

            rc = slap_str2undef_ad( name, &ad, &text );

            if( rc != LDAP_SUCCESS ) {
                entry_free(e);
                (*env)->ReleaseStringUTFChars(env, jname, name);
                return NULL;
            }
        }

        DEBUG1("Attribute: %s\n", ad->ad_cname.bv_val);

        jobject ne2 = (*env)->CallObjectMethod(env, jattribute, java_backend_Attribute.getAll);

        int hasMore2 = (*env)->CallBooleanMethod(env, ne2, NamingEnumeration.hasMore);
        //DEBUG1("hasMore2: %d\n", hasMore2);

        while (hasMore2) {

            jobject jvalue = (*env)->CallObjectMethod(env, ne2, NamingEnumeration.next);
            jboolean isString = (*env)->IsInstanceOf(env, jvalue, String.class);

            if (isString) {

                vals[0].bv_val = (char *)(*env)->GetStringUTFChars(env, jvalue, 0);
                vals[0].bv_len = strlen(vals[0].bv_val);

                DEBUG2("%s: %s\n", name, vals[0].bv_val);

                rc = attr_merge( e, ad, vals, NULL );

                //fprintf(stderr, " - attr_merge: %d\n", (rc == LDAP_SUCCESS));

                (*env)->ReleaseStringUTFChars(env, jvalue, vals[0].bv_val);

                if( rc != 0 ) {
                    entry_free( e );
                    (*env)->ReleaseStringUTFChars(env, jname, name);
                    return NULL;
                }

            } else {

                jbyteArray bytes = (jbyteArray)jvalue;
                jint length = (*env)->GetArrayLength(env, bytes);

                vals[0].bv_val = (char *)ch_calloc(length, sizeof(char));
                vals[0].bv_len = length;

                (*env)->GetByteArrayRegion(env, bytes, 0, length, vals[0].bv_val);
            }

            hasMore2 = (*env)->CallBooleanMethod(env, ne2, NamingEnumeration.hasMore);
            //DEBUG1("hasMore2: %d\n", hasMore2);
        }

        (*env)->ReleaseStringUTFChars(env, jname, name);

        hasMore = (*env)->CallBooleanMethod(env, jiterator, NamingEnumeration.hasMore);
        //DEBUG1("hasMore: %d\n", hasMore);
    }

exit:
    DEBUG("<== createEntry()\n");
    return e;
}

