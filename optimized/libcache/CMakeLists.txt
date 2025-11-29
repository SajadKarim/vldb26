add_library(libcache
            CacheErrorCodes.h
            FileStorage.hpp
            LRUCache.hpp
            LRUCacheObject.hpp
            A2QCache.hpp
            A2QCacheObject.hpp
	    CLOCKCache.hpp
	    CLOCKCacheObject.hpp
            NoCache.hpp
            NoCacheObject.hpp
            ObjectFatUID.h
            VolatileStorage.hpp
            PMemStorage.hpp
            PMemWAL.hpp
            FileWAL.hpp
            validityasserts.h
            SIMDBitmapAllocator.hpp
            MMapStorage.hpp
            IOURingStorage.hpp
)
set_target_properties(libcache PROPERTIES LINKER_LANGUAGE CXX)

target_link_libraries(libcache PUBLIC haldendb_compiler_flags)
