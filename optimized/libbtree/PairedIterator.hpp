#pragma once
#include <iterator>
#include <algorithm>

/**
 * @brief Value type for PairedIterator - represents a key-value pair
 */
template<typename KeyType, typename ValueType>
struct PairedValue {
    KeyType key;
    ValueType value;
    
    // Comparison based on key
    bool operator<(const PairedValue& other) const {
        return key < other.key;
    }
};

/**
 * @brief Reference type for PairedIterator - holds pointers to key and value
 */
template<typename KeyIter, typename ValueIter>
struct PairedReference {
    using KeyType = typename std::iterator_traits<KeyIter>::value_type;
    using ValueType = typename std::iterator_traits<ValueIter>::value_type;
    using ValueTypeAlias = PairedValue<KeyType, ValueType>;
    
    KeyIter keyIt;
    ValueIter valueIt;
    
    PairedReference(KeyIter k, ValueIter v) : keyIt(k), valueIt(v) {}
    
    // Copy constructor
    PairedReference(const PairedReference& other) : keyIt(other.keyIt), valueIt(other.valueIt) {}
    
    // Assignment from another reference (for copy operations)
    PairedReference& operator=(const PairedReference& other) {
        *keyIt = *other.keyIt;
        *valueIt = *other.valueIt;
        return *this;
    }
    
    // Assignment from another reference (for move operations)
    PairedReference& operator=(PairedReference&& other) {
        *keyIt = std::move(*other.keyIt);
        *valueIt = std::move(*other.valueIt);
        return *this;
    }
    
    // Assignment from value type (for move operations)
    PairedReference& operator=(ValueTypeAlias&& val) {
        *keyIt = std::move(val.key);
        *valueIt = std::move(val.value);
        return *this;
    }
    
    // Assignment from value type (for copy operations)
    PairedReference& operator=(const ValueTypeAlias& val) {
        *keyIt = val.key;
        *valueIt = val.value;
        return *this;
    }
    
    // Conversion to value type (for move operations)
    operator ValueTypeAlias() && {
        return {std::move(*keyIt), std::move(*valueIt)};
    }
    
    // Swap function - called by std::iter_swap
    friend void swap(PairedReference a, PairedReference b) {
        std::swap(*a.keyIt, *b.keyIt);
        std::swap(*a.valueIt, *b.valueIt);
    }
};

// Comparison operators for all combinations used by std::sort
template<typename KeyIter, typename ValueIter>
bool operator<(const PairedReference<KeyIter, ValueIter>& a, const PairedReference<KeyIter, ValueIter>& b) {
    return *a.keyIt < *b.keyIt;
}

template<typename KeyIter, typename ValueIter>
bool operator<(const PairedReference<KeyIter, ValueIter>& a, const PairedValue<typename std::iterator_traits<KeyIter>::value_type, typename std::iterator_traits<ValueIter>::value_type>& b) {
    return *a.keyIt < b.key;
}

template<typename KeyIter, typename ValueIter>
bool operator<(const PairedValue<typename std::iterator_traits<KeyIter>::value_type, typename std::iterator_traits<ValueIter>::value_type>& a, const PairedReference<KeyIter, ValueIter>& b) {
    return a.key < *b.keyIt;
}

/**
 * @brief Iterator that allows sorting two vectors simultaneously
 * 
 * This iterator enables std::sort to work on keys while automatically
 * applying the same swaps to corresponding values in a parallel vector.
 * 
 * @tparam KeyIter Iterator type for keys (e.g., std::vector<KeyType>::iterator)
 * @tparam ValueIter Iterator type for values (e.g., std::vector<ValueType>::iterator)
 */
template<typename KeyIter, typename ValueIter>
class PairedIterator {
public:
    KeyIter keyIt;
    ValueIter valueIt;
    
public:
    // Standard iterator type definitions
    using iterator_category = std::random_access_iterator_tag;
    using value_type = PairedValue<typename std::iterator_traits<KeyIter>::value_type, typename std::iterator_traits<ValueIter>::value_type>;
    using difference_type = typename std::iterator_traits<KeyIter>::difference_type;
    using pointer = value_type*;
    using reference = PairedReference<KeyIter, ValueIter>;
    
    /**
     * @brief Constructor
     * @param k Iterator pointing to key position
     * @param v Iterator pointing to corresponding value position
     */
    PairedIterator(KeyIter k, ValueIter v) : keyIt(k), valueIt(v) {}
    
    // Dereference operators - return reference object
    reference operator*() { return PairedReference<KeyIter, ValueIter>(keyIt, valueIt); }
    reference operator*() const { return PairedReference<KeyIter, ValueIter>(keyIt, valueIt); }
    
    // Pre-increment: move both iterators forward
    PairedIterator& operator++() { 
        ++keyIt; 
        ++valueIt; 
        return *this; 
    }
    
    // Post-increment
    PairedIterator operator++(int) {
        PairedIterator temp = *this;
        ++(*this);
        return temp;
    }
    
    // Pre-decrement: move both iterators backward
    PairedIterator& operator--() { 
        --keyIt; 
        --valueIt; 
        return *this; 
    }
    
    // Post-decrement
    PairedIterator operator--(int) {
        PairedIterator temp = *this;
        --(*this);
        return temp;
    }
    
    // Arithmetic operators
    PairedIterator operator+(difference_type n) const { 
        return PairedIterator(keyIt + n, valueIt + n); 
    }
    
    PairedIterator operator-(difference_type n) const { 
        return PairedIterator(keyIt - n, valueIt - n); 
    }
    
    PairedIterator& operator+=(difference_type n) {
        keyIt += n;
        valueIt += n;
        return *this;
    }
    
    PairedIterator& operator-=(difference_type n) {
        keyIt -= n;
        valueIt -= n;
        return *this;
    }
    
    // Distance between iterators
    difference_type operator-(const PairedIterator& other) const { 
        return keyIt - other.keyIt; 
    }
    
    // Comparison operators (based on key iterator position)
    bool operator==(const PairedIterator& other) const { 
        return keyIt == other.keyIt; 
    }
    
    bool operator!=(const PairedIterator& other) const { 
        return keyIt != other.keyIt; 
    }
    
    bool operator<(const PairedIterator& other) const { 
        return keyIt < other.keyIt; 
    }
    
    bool operator<=(const PairedIterator& other) const { 
        return keyIt <= other.keyIt; 
    }
    
    bool operator>(const PairedIterator& other) const { 
        return keyIt > other.keyIt; 
    }
    
    bool operator>=(const PairedIterator& other) const { 
        return keyIt >= other.keyIt; 
    }
    
    // Random access operator
    reference operator[](difference_type n) { 
        return PairedReference<KeyIter, ValueIter>(keyIt + n, valueIt + n); 
    }
    
    reference operator[](difference_type n) const { 
        return PairedReference<KeyIter, ValueIter>(keyIt + n, valueIt + n); 
    }
    
};

/**
 * @brief Helper function to create PairedIterator easily
 * 
 * @tparam KeyIter Key iterator type
 * @tparam ValueIter Value iterator type
 * @param keyIt Key iterator
 * @param valueIt Value iterator
 * @return PairedIterator instance
 */
template<typename KeyIter, typename ValueIter>
PairedIterator<KeyIter, ValueIter> make_paired_iterator(KeyIter keyIt, ValueIter valueIt) {
    return PairedIterator<KeyIter, ValueIter>(keyIt, valueIt);
}