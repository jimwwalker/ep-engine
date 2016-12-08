#include <memcached/engine.h>
#include <memcached/engine_testapp.h>

#include "ep_testsuite_common.h"

static enum test_result test_namespace_separation(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {
    DocKey key1("key", DocNamespace::System);
    DocKey key2("key", DocNamespace::Collections);
    std::string value1 = "value1";
    std::string value2 = "value2";

    item *it = nullptr;
    checkeq(ENGINE_SUCCESS,
            h1->allocate(h, nullptr, &it, key1, value1.size(), 0, 0, 0, 0),
            "Failed allocate");
    uint64_t cas{};
    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, nullptr, it, &info)) {
        abort();
    }

    cb_assert(info.value[0].iov_len == value1.size());
    memcpy(info.value[0].iov_base, value1.data(), value1.size());
    h1->item_set_cas(h, nullptr, it, 0);

    checkeq(ENGINE_SUCCESS,
            h1->store(h, nullptr, it, &cas, OPERATION_ADD, DocumentState::Alive),
            "Failed store");

    h1->release(h, nullptr, it);


    checkeq(ENGINE_SUCCESS,
            h1->allocate(h, nullptr, &it, key2, value2.size(), 0, 0, 0, 0),
            "Failed allocate");

    info.nvalue = 1;
    if (!h1->get_item_info(h, nullptr, it, &info)) {
        abort();
    }

    cb_assert(info.value[0].iov_len == value2.size());
    memcpy(info.value[0].iov_base, value2.data(), value2.size());
    h1->item_set_cas(h, nullptr, it, 0);

    checkeq(ENGINE_SUCCESS,
            h1->store(h, nullptr, it, &cas, OPERATION_ADD, DocumentState::Alive),
            "Failed store");

    h1->release(h, nullptr, it);

    wait_for_flusher_to_settle(h, h1);

    checkeq(2, get_int_stat(h, h1, "ep_total_persisted"),
            "Expected ep_total_persisted equals 0");
    checkeq(2, get_int_stat(h, h1, "curr_items"),
            "Expected curr_items equals 0");

    // restart
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.get_current_testcase()->cfg,
                              true, false);
    wait_for_warmup_complete(h, h1);

    checkeq(2, get_int_stat(h, h1, "ep_total_persisted"),
            "Expected ep_total_persisted equals 0");
    checkeq(2, get_int_stat(h, h1, "curr_items"),
            "Expected curr_items equals 0");
    return SUCCESS;
}


// Test manifest //////////////////////////////////////////////////////////////

const char *default_dbname = "./ep_testsuite_collections";

BaseTestCase testsuite_testcases[] = {
        TestCase("test_namespace_separation", test_namespace_separation,
                 test_setup, teardown, "persist_doc_namespace=true",
                 prepare, cleanup)
};