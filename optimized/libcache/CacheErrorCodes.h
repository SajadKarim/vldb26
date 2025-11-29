#pragma once

enum class CacheErrorCode {
    Success,
    Error,
    ChildSplitCalledOnLeafNode,
    KeyDoesNotExist,
    OutOfStorage,
    IOError,
    Unsupported,
};
