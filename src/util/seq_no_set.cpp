#include "util/seq_no_set.h"

using namespace std;

namespace pbrt {

SeqNoSet::SeqNoSet() : set_{}, smallest_not_in_set_{0} {}

bool SeqNoSet::contains(uint64_t value) const {
    if (value < smallest_not_in_set_) {
        return true;
    } else if (value == smallest_not_in_set_) {
        return false;
    } else {
        return set_.count(value) > 0;
    }
}

void SeqNoSet::insert(uint64_t value) {
    if (value < smallest_not_in_set_) {
        return;
    } else if (value == smallest_not_in_set_) {
        auto it_to_smallest = set_.begin();
        smallest_not_in_set_ += 1;
        while (*it_to_smallest == smallest_not_in_set_) {
            it_to_smallest = set_.erase(it_to_smallest);
            smallest_not_in_set_ += 1;
        }
    } else {
        set_.insert(value);
    }
}

uint64_t SeqNoSet::size() const { return smallest_not_in_set_ + set_.size(); }

uint64_t SeqNoSet::numberOfItemsInMemory() const { return set_.size(); }

}  // namespace pbrt
