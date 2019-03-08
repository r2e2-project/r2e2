#include <cstdint>
#include <set>

#include "tests/gtest/gtest.h"
#include "util/seq_no_set.h"

using namespace pbrt;

/**
 * For checking consistency with a std::set
 */
void checkMembershipConsistencyOnInsertionSequence(SeqNoSet& set, std::set<uint64_t>& baseline, const std::vector<uint64_t>& insertions) {
    EXPECT_EQ(baseline.size(), set.size());
    for (uint64_t i : insertions) {
        baseline.insert(i);
        set.insert(i);
        EXPECT_TRUE(set.contains(i));
    }
    for (uint64_t i = 0; i < *baseline.cend(); ++i) {
        EXPECT_EQ(baseline.count(i) > 0, set.contains(i));
    }
    EXPECT_EQ(baseline.size(), set.size());
}

TEST(SeqNoSet, Construction) {
    SeqNoSet set;
    for (uint64_t i = 0; i < 100; ++i) {
        EXPECT_FALSE(set.contains(i));
    }
    EXPECT_EQ(0, set.size());
    EXPECT_EQ(0, set.numberOfItemsInMemory());
}

TEST(SeqNoSet, SparseInsertion) {
    SeqNoSet set;
    std::set<uint64_t> baseline;
    checkMembershipConsistencyOnInsertionSequence(set, baseline, std::vector<uint64_t>{45,30,15,2});
    EXPECT_EQ(4, set.numberOfItemsInMemory());
}

TEST(SeqNoSet, DenseInsertion) {
    SeqNoSet set;
    std::set<uint64_t> baseline;
    checkMembershipConsistencyOnInsertionSequence(set, baseline, std::vector<uint64_t>{0,1,2,3});
    EXPECT_EQ(0, set.numberOfItemsInMemory());
}

TEST(SeqNoSet, DensifyInsertion) {
    SeqNoSet set;
    std::set<uint64_t> baseline;
    checkMembershipConsistencyOnInsertionSequence(set, baseline, std::vector<uint64_t>{1,3,4,6,7,8,9});
    EXPECT_EQ(7, set.numberOfItemsInMemory());
    checkMembershipConsistencyOnInsertionSequence(set, baseline, std::vector<uint64_t>{0});
    EXPECT_EQ(6, set.numberOfItemsInMemory());
    checkMembershipConsistencyOnInsertionSequence(set, baseline, std::vector<uint64_t>{2});
    EXPECT_EQ(4, set.numberOfItemsInMemory());
    checkMembershipConsistencyOnInsertionSequence(set, baseline, std::vector<uint64_t>{5});
    EXPECT_EQ(0, set.numberOfItemsInMemory());
    checkMembershipConsistencyOnInsertionSequence(set, baseline, std::vector<uint64_t>{15});
    EXPECT_EQ(1, set.numberOfItemsInMemory());
}
