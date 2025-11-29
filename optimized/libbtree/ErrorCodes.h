#pragma once

enum class ErrorCode {
    Success,
    Error,
    InsertFailed,
    SplitFailed,
    ChildSplitCalledOnLeafNode,
    KeyDoesNotExist,
    KeyAlreadyExists,
    DontSplit,
    BufferFull,
    SplitPostponed, //insetead of donot split
    NeedToBeMerged,
};
