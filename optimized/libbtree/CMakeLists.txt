add_library(libbtree
        BPlusStore.hpp
        DataNode.hpp
        DataNodeROpt.hpp
        DataNodeCOpt.hpp
        DataNodeCROpt.hpp
        ErrorCodes.h
        IndexNode.hpp
        IndexNodeROpt.hpp
        IndexNodeCOpt.hpp
        IndexNodeCROpt.hpp
        BEpsilonStore.hpp
        DataNodeWithBuffer.hpp
        DataNodeWithBufferROpt.hpp
        DataNodeWithBufferRWOpt.hpp
        IndexNodeWithBuffer.hpp
        IndexNodeWithBufferROpt.hpp
        IndexNodeWithBufferRWOpt.hpp
        TypeUID.h
)


target_include_directories(libbtree PUBLIC
            "${PROJECT_SOURCE_DIR}/../libcache"
)

set_target_properties(libbtree PROPERTIES LINKER_LANGUAGE CXX)

target_link_libraries(libbtree PUBLIC haldendb_compiler_flags)
