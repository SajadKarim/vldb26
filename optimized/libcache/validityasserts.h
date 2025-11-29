#pragma once

#ifdef __ENABLE_ASSERTS__
#include <iostream>
#define ASSERT(expr) \
        if (!(expr)) { \
            std::cerr << "Assertion failed: " << #expr << " at " << __FILE__ << ":" << __LINE__ << std::endl; \
            std::abort(); \
        }
#else //__ENABLE_ASSERTS__
#define ASSERT(expr) ((void)0) // Disable asserts if ENABLE_ASSERTS is not defined
#endif //__ENABLE_ASSERTS__