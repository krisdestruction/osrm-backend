#include "util/packed_vector.hpp"
#include "util/typedefs.hpp"

#include <boost/test/test_case_template.hpp>
#include <boost/test/unit_test.hpp>

#include <random>

BOOST_AUTO_TEST_SUITE(packed_vector_test)

using namespace osrm;
using namespace osrm::util;

// Verify that the packed vector behaves as expected
BOOST_AUTO_TEST_CASE(insert_and_retrieve_packed_test)
{
    PackedVector<OSMNodeID, 33> packed_ids;
    std::vector<OSMNodeID> original_ids;

    const constexpr std::size_t num_test_cases = 399;
    const constexpr std::uint64_t max_id = (1UL << 33) - 1;

    std::mt19937 rng;
    rng.seed(1337);
    std::uniform_int_distribution<std::mt19937::result_type> dist(0, max_id);

    for (std::size_t i = 0; i < num_test_cases; i++)
    {
        OSMNodeID r{static_cast<std::uint64_t>(dist(rng))}; // max 33-bit uint

        packed_ids.push_back(r);
        original_ids.push_back(r);
    }

    for (std::size_t i = 0; i < num_test_cases; i++)
    {
        BOOST_CHECK_EQUAL(original_ids.at(i), packed_ids.at(i));
    }
}

BOOST_AUTO_TEST_CASE(packed_vector_capacity_test)
{
    PackedVector<OSMNodeID, 33> packed_vec;
    const std::size_t original_size = packed_vec.capacity();
    std::vector<OSMNodeID> dummy_vec;

    BOOST_CHECK_EQUAL(original_size, dummy_vec.capacity());

    packed_vec.reserve(100);

    BOOST_CHECK(packed_vec.capacity() >= 100);
}

BOOST_AUTO_TEST_CASE(packed_vector_10bit_small_test)
{
    PackedVector<std::uint32_t, 10> vector = {10, 5, 8, 12, 254, 4, (1 << 10) - 1, 6};
    std::vector<std::uint32_t> reference = {10, 5, 8, 12, 254, 4, (1 << 10) - 1, 6};

    BOOST_CHECK_EQUAL(vector[0], reference[0]);
    BOOST_CHECK_EQUAL(vector[1], reference[1]);
    BOOST_CHECK_EQUAL(vector[2], reference[2]);
    BOOST_CHECK_EQUAL(vector[3], reference[3]);
    BOOST_CHECK_EQUAL(vector[4], reference[4]);
    BOOST_CHECK_EQUAL(vector[5], reference[5]);
    BOOST_CHECK_EQUAL(vector[6], reference[6]);
    BOOST_CHECK_EQUAL(vector[7], reference[7]);
}

BOOST_AUTO_TEST_CASE(packed_vector_33bit_small_test)
{
    std::vector<std::uint64_t> reference = {1597322404,
                                            1939964443,
                                            2112255763,
                                            1432114613,
                                            1067854538,
                                            352118606,
                                            1782436840,
                                            1909002904,
                                            165344818};

    PackedVector<std::uint64_t, 33> vector = {1597322404,
                                              1939964443,
                                              2112255763,
                                              1432114613,
                                              1067854538,
                                              352118606,
                                              1782436840,
                                              1909002904,
                                              165344818};

    BOOST_CHECK_EQUAL(vector[0], reference[0]);
    BOOST_CHECK_EQUAL(vector[1], reference[1]);
    BOOST_CHECK_EQUAL(vector[2], reference[2]);
    BOOST_CHECK_EQUAL(vector[3], reference[3]);
    BOOST_CHECK_EQUAL(vector[4], reference[4]);
    BOOST_CHECK_EQUAL(vector[5], reference[5]);
    BOOST_CHECK_EQUAL(vector[6], reference[6]);
    BOOST_CHECK_EQUAL(vector[7], reference[7]);
}

BOOST_AUTO_TEST_CASE(packed_vector_33bit_continious)
{
    PackedVector<std::uint64_t, 33> vector;

    for (std::uint64_t i : osrm::util::irange(0, 400))
    {
        vector.push_back(i);
        BOOST_CHECK_EQUAL(vector.back(), i);
    }
}

BOOST_AUTO_TEST_SUITE_END()
