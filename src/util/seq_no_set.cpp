#include "util/seq_no_set.h"

using namespace std;

namespace r2t2 {

bool SeqNoSet::contains(const uint64_t value) const {
    if (value < smallest_not_in_set_) {
        return true;
    } else if (value == smallest_not_in_set_) {
        return false;
    } else {
        return set_.count(value) > 0;
    }
}

void SeqNoSet::insert(const uint64_t value) {
    if (value < smallest_not_in_set_) {
        return;
    } else if (value == smallest_not_in_set_) {
        auto it_to_smallest = set_.begin();
        auto it_to_end = set_.end();
        smallest_not_in_set_ += 1;
        while (it_to_smallest != set_.end() &&
               *it_to_smallest == smallest_not_in_set_) {
            it_to_smallest = set_.erase(it_to_smallest);
            smallest_not_in_set_ += 1;
        }
    } else {
        set_.insert(value);
    }
}

void SeqNoSet::insertAllBelow(const uint64_t value) {
    if (value <= smallest_not_in_set_) {
        return;
    } else {
        smallest_not_in_set_ = value;
        auto it_to_smallest = set_.begin();
        while (it_to_smallest != set_.end() &&
               *it_to_smallest <= smallest_not_in_set_) {
            if (*it_to_smallest == smallest_not_in_set_) {
                smallest_not_in_set_++;
            }

            it_to_smallest = set_.erase(it_to_smallest);
        }
    }
}

uint64_t SeqNoSet::size() const { return smallest_not_in_set_ + set_.size(); }

uint64_t SeqNoSet::numberOfItemsInMemory() const { return set_.size(); }

}  // namespace r2t2
