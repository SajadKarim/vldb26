#pragma once
#include <cmath>
#include <iostream>

// Bloom Filter False Positive Rate Analysis
class BloomFilterAnalysis 
{
public:
    // Calculate theoretical false positive rate
    static double calculateFalsePositiveRate(size_t numBits, size_t numElements, size_t numHashFunctions) {
        if (numElements == 0) return 0.0;
        
        // Probability that a bit is not set by one hash function for one element
        double p_not_set_one = 1.0 - (1.0 / numBits);
        
        // Probability that a bit is not set after all hash functions for all elements
        double p_not_set_all = std::pow(p_not_set_one, numHashFunctions * numElements);
        
        // Probability that a bit is set
        double p_set = 1.0 - p_not_set_all;
        
        // False positive rate: probability that all k bits are set for a non-member
        return std::pow(p_set, numHashFunctions);
    }
    
    // Find optimal number of hash functions
    static size_t findOptimalHashFunctions(size_t numBits, size_t numElements) {
        if (numElements == 0) return 1;
        
        double optimal = (static_cast<double>(numBits) / numElements) * std::log(2.0);
        return std::max(1UL, static_cast<size_t>(std::round(optimal)));
    }
    
    // Performance vs accuracy analysis
    static void analyzeConfiguration(size_t numBits, size_t numElements) {
        std::cout << "\n=== Bloom Filter Analysis ===\n";
        std::cout << "Filter size: " << numBits << " bits\n";
        std::cout << "Expected elements: " << numElements << "\n";
        std::cout << "Optimal hash functions: " << findOptimalHashFunctions(numBits, numElements) << "\n\n";
        
        std::cout << "Hash Functions | False Positive Rate | Relative Performance\n";
        std::cout << "---------------|--------------------|-----------------\n";
        
        for (size_t k = 1; k <= 6; ++k) {
            double fpr = calculateFalsePositiveRate(numBits, numElements, k);
            double relative_perf = 1.0 / k;  // Inverse relationship with hash count
            
            std::cout << "      " << k << "        |       " 
                      << std::fixed << std::setprecision(4) << (fpr * 100) << "%      |       "
                      << std::setprecision(2) << (relative_perf * 100) << "%\n";
        }
    }
};