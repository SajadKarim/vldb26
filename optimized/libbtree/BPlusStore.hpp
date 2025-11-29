#pragma once
#include <memory>
#include <iostream>
#include <stack>
#include <optional>
#include <mutex>
#include <shared_mutex>
#include <syncstream>
#include <thread>
#include <cmath>
#include <exception>
#include <variant>
#include <unordered_map>
#include "CacheErrorCodes.h"
#include "ErrorCodes.h"
#include <tuple>
#include <vector>
#include <stdexcept>
#include <thread>
#include <iostream>
#include <fstream>
#include <algorithm>
#include "validityasserts.h"

#ifdef __TREE_WITH_CACHE__
#include "CLOCKCache.hpp"
#endif

#define ENABLE_ASSERTS
using namespace std::chrono_literals;

#ifdef __TREE_WITH_CACHE__
template <typename Traits>
class BPlusStore 
#else //__TREE_WITH_CACHE__
template <typename Traits>
class BPlusStore
#endif //__TREE_WITH_CACHE__
{
public:
    using KeyType = typename Traits::KeyType;
    using ValueType = typename Traits::ValueType;
    using CacheType = typename Traits::CacheType;

    using ObjectType = typename Traits::ObjectType;
    using ObjectTypePtr = typename CacheType::ObjectTypePtr;
    using ObjectUIDType = typename Traits::ObjectUIDType;

    using DataNodeType = typename Traits::DataNodeType;
    using IndexNodeType = typename Traits::IndexNodeType;

    using OpDeleteInfo = typename CacheType::OpDeleteInfo;

private:

#ifndef __PROD__
    uint32_t m_nDegree;
#endif //__PROD__

    std::shared_ptr<CacheType> m_ptrCache;

    ObjectTypePtr m_ptrRootNode;
    ObjectUIDType m_uidRootNode;

#ifdef __CONCURRENT__
    mutable std::shared_mutex m_mutex;
#endif //__CONCURRENT__

public:
    ~BPlusStore()
    {
        m_ptrCache.reset();

        if (m_ptrRootNode != nullptr) delete m_ptrRootNode;
    }

    template<typename... CacheArgs>
    BPlusStore(uint32_t nDegree, CacheArgs... args)
        : m_ptrRootNode(nullptr)
        //, m_uidRootNode(std::nullopt)
    {
#ifndef __PROD__
        m_nDegree = nDegree;
#endif //__PROD__

        m_ptrCache = std::make_shared<CacheType>(args...);
    }

    template <typename DefaultNodeType>
    void init()
    {
#ifdef __TREE_WITH_CACHE__
        m_ptrCache->init(this);
#endif //__TREE_WITH_CACHE__

        m_ptrCache->template createObjectOfType<DefaultNodeType>(m_uidRootNode, m_ptrRootNode, m_nDegree);
    }

#ifdef __CACHE_COUNTERS__
    std::shared_ptr<CacheType> getCache() const
    {
        return m_ptrCache;
    }
#endif

    ErrorCode insert(const KeyType& key, const ValueType& value)
    {
        ErrorCode nResult = ErrorCode::Error;

        KeyType pivotKey;
        ObjectUIDType uidChildSplitRHSNode;
        ObjectTypePtr ptrChildSplitRHSNode = nullptr;

        ObjectTypePtr ptrLastNode = nullptr;
        ObjectTypePtr ptrCurrentNode = nullptr;

        uint16_t nNodeSplitPosBegin = 0, nNodeSplitPosEnd = 0;

        std::vector<std::pair<ObjectTypePtr, ObjectTypePtr>, std::allocator<std::pair<ObjectTypePtr, ObjectTypePtr>>> vtNodes;
        vtNodes.reserve(20);
        //std::vector<std::pair<ObjectTypePtr, ObjectTypePtr>> vtNodes; // reserve!!! + aligned!!

#ifdef __TREE_WITH_CACHE__
        int nNodeLevel = 0;
#ifdef __SELECTIVE_UPDATE__
        bool hasNewNodes = false;
#endif
#endif //__TREE_WITH_CACHE__

#ifdef __CONCURRENT__
        std::vector<std::unique_lock<std::shared_mutex>> vtLocks;
        vtLocks.emplace_back(std::unique_lock<std::shared_mutex>(m_mutex));
        //vtLocks.emplace_back(std::unique_lock<std::shared_mutex>(m_ptrRootNode->m_mtx));
        vtLocks.emplace_back([&]() {
            std::unique_lock<std::shared_mutex> lock(m_ptrRootNode->m_mtx);
#ifdef __TREE_WITH_CACHE__
            m_ptrRootNode->m_nUseCounter.fetch_add(1, std::memory_order_relaxed);
#endif //__TREE_WITH_CACHE__
            return lock;
        }());
#else //__CONCURRENT__
        if constexpr (CacheType::MARK_INUSE_FLAG) m_ptrRootNode->m_bInUse = true;
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
        m_ptrCache->log(0, key, value);
        //m_ptrRootNode->use_count++;
#endif //__TREE_WITH_CACHE__

        ptrCurrentNode = m_ptrRootNode;
        
        vtNodes.push_back(std::pair<ObjectTypePtr, ObjectTypePtr>(nullptr, nullptr));   //split at the root node, place for new root

        do
        {
#ifdef __TREE_WITH_CACHE__
            nNodeLevel++;
#endif //__TREE_WITH_CACHE__

#ifdef __CONCURRENT__
            ASSERT(!ptrCurrentNode->m_mtx.try_lock());
#endif //__CONCURRENT__

            if (ptrCurrentNode->m_nCoreObjectType == IndexNodeType::UID)
            {
                IndexNodeType* ptrIndexNode = reinterpret_cast<IndexNodeType*>(ptrCurrentNode->m_ptrCoreObject);

                if (!ptrIndexNode->canTriggerSplit())
                {
#ifdef __CONCURRENT__
                    vtLocks.erase(vtLocks.begin(), vtLocks.end() - 1);
#endif //__CONCURRENT__

                    nNodeSplitPosEnd = vtNodes.size();
                }

                vtNodes.push_back(std::pair<ObjectTypePtr, ObjectTypePtr>(ptrCurrentNode, nullptr));

                ptrLastNode = ptrCurrentNode;   // todo vtnodes last idx...??

#ifdef __TREE_WITH_CACHE__
#ifdef __SELECTIVE_UPDATE__
#ifdef __CONCURRENT__
                if (ptrIndexNode->template getChild<CacheType>(m_ptrCache, key, ptrCurrentNode, vtLocks, hasNewNodes))
#else //__CONCURRENT__
                if (ptrIndexNode->template getChild<CacheType>(m_ptrCache, key, ptrCurrentNode, hasNewNodes))
#endif //__CONCURRENT__
                {
                    ptrLastNode->m_bDirty = true;
                }
#else // !__SELECTIVE_UPDATE__
#ifdef __CONCURRENT__
                if (ptrIndexNode->template getChild<CacheType>(m_ptrCache, key, ptrCurrentNode, vtLocks))
#else //__CONCURRENT__
                if (ptrIndexNode->template getChild<CacheType>(m_ptrCache, key, ptrCurrentNode))
#endif //__CONCURRENT__
                {
                    ptrLastNode->m_bDirty = true;
                }
#endif // __SELECTIVE_UPDATE__

                //m_ptrRootNode->use_count++;

#else //__TREE_WITH_CACHE__
#ifdef __CONCURRENT__
                ptrIndexNode->getChild(key, ptrCurrentNode, vtLocks);
#else //__CONCURRENT__
                ptrIndexNode->getChild(key, ptrCurrentNode);
#endif //__CONCURRENT__
#endif //__TREE_WITH_CACHE__
            }
            else
            {
                DataNodeType* ptrDataNode = reinterpret_cast<DataNodeType*>(ptrCurrentNode->m_ptrCoreObject);

                nResult = ptrDataNode->insert(key, value);
                if (nResult != ErrorCode::Success)
                {
#ifdef __CONCURRENT__
                    vtLocks.clear();
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
                    vtNodes.push_back(std::pair<ObjectTypePtr, ObjectTypePtr>(ptrCurrentNode, nullptr)); // Cache might need to reorder.
#endif //__TREE_WITH_CACHE__

#ifdef __TREE_WITH_CACHE__
#ifdef __SELECTIVE_UPDATE__
                    m_ptrCache->updateObjectsAccessMetadata(nNodeLevel, vtNodes, hasNewNodes);
#else
                    m_ptrCache->updateObjectsAccessMetadata(nNodeLevel, vtNodes);
#endif // __SELECTIVE_UPDATE__
                    //all nodes->use_count--;
                    vtNodes.clear();
#endif //__TREE_WITH_CACHE__

                    return nResult;
                    // nNodeSplitPosBegin = 0;
                }

#ifdef __TREE_WITH_CACHE__
                ptrCurrentNode->m_bDirty = true;
#endif //__TREE_WITH_CACHE__

                if (ptrDataNode->requireSplit())
                {
                    if (ptrDataNode->template split<CacheType, ObjectTypePtr>(m_ptrCache, uidChildSplitRHSNode, ptrChildSplitRHSNode, pivotKey) != ErrorCode::Success)
                    {
                        // TODO: Should update be performed on cloned objects first?
                        std::cout << "Critical State: Failed to perform insert operation to the IndexNode." << std::endl;
                        throw new std::logic_error(".....");   // TODO: critical log.
                    }
#ifdef __SELECTIVE_UPDATE__
                    hasNewNodes = true;  // New node created from split
#endif

#if defined(__CONCURRENT__) && defined(__TREE_WITH_CACHE__)
                    ptrChildSplitRHSNode->m_nUseCounter.fetch_add(1, std::memory_order_relaxed);
#else //__CONCURRENT__ && __TREE_WITH_CACHE__
                    if constexpr (CacheType::MARK_INUSE_FLAG) ptrChildSplitRHSNode->m_bInUse = true;
#endif //__CONCURRENT__ && __TREE_WITH_CACHE__

                    vtNodes.push_back(std::pair<ObjectTypePtr, ObjectTypePtr>(ptrCurrentNode, ptrChildSplitRHSNode));

                    nNodeSplitPosBegin = vtNodes.size() - 1;
                }
                else
                {
#ifdef __CONCURRENT__
                    vtLocks.clear();
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
                    vtNodes.push_back(std::pair<ObjectTypePtr, ObjectTypePtr>(ptrCurrentNode, nullptr)); // Cache might need to reorder.
#endif //__TREE_WITH_CACHE__

#ifdef __TREE_WITH_CACHE__
#ifdef __SELECTIVE_UPDATE__
                    m_ptrCache->updateObjectsAccessMetadata(nNodeLevel, vtNodes, hasNewNodes);
#else
                    m_ptrCache->updateObjectsAccessMetadata(nNodeLevel, vtNodes);
#endif // __SELECTIVE_UPDATE__
                    //all nodes->use_count--;
                    vtNodes.clear();
#endif //__TREE_WITH_CACHE__

                    return ErrorCode::Success;
                    // nNodeSplitPosBegin = 0;
                }
                break;
            }
        } while (true);

        int nIdx = nNodeSplitPosBegin;
        for (; nIdx > 1; --nIdx)
        {
            ptrChildSplitRHSNode = vtNodes[nIdx].second;
            ptrCurrentNode = vtNodes[nIdx - 1].first;

            ASSERT(vtNodes[nIdx].second != nullptr);

#ifdef __CONCURRENT__
            ASSERT(vtLocks.size() > 1);
            ASSERT (!ptrCurrentNode->m_mtx.try_lock());
            ASSERT(!vtNodes[nIdx].first->m_mtx.try_lock());
#endif //__CONCURRENT__

            IndexNodeType* ptrIndexNode = reinterpret_cast<IndexNodeType*>(ptrCurrentNode->m_ptrCoreObject);

            if (ptrIndexNode->insert(pivotKey, uidChildSplitRHSNode, ptrChildSplitRHSNode) != ErrorCode::Success)
            {
                // TODO: Should update be performed on cloned objects first?
                std::cout << "Critical State: Failed to perform insert operation to the IndexNode." << std::endl;
                throw new std::logic_error(".....");   // TODO: critical log.
            }

#ifdef __TREE_WITH_CACHE__
            ptrCurrentNode->m_bDirty = true;
#endif //__TREE_WITH_CACHE__

            if (nIdx == nNodeSplitPosEnd + 1)
            {
#ifdef __CONCURRENT__
                ASSERT(vtLocks.size() == 2); 
#endif //__CONCURRENT__
                break;
            }

#ifdef __CONCURRENT__
            vtLocks.pop_back(); // release child lock
            ASSERT (vtLocks.size() >= 2);  // skip if do not have lock for the parent node.
#endif //__CONCURRENT__

            if (ptrIndexNode->requireSplit())
            {
                if (ptrIndexNode->template split<CacheType>(m_ptrCache, uidChildSplitRHSNode, vtNodes[nIdx - 1].second, pivotKey) != ErrorCode::Success)
                {
                    // TODO: Should update be performed on cloned objects first?
                    std::cout << "Critical State: Failed to split DataNode." << std::endl;
                    throw new std::logic_error(".....");   // TODO: critical log.
                }
#ifdef __SELECTIVE_UPDATE__
                hasNewNodes = true;  // New node created from split
#endif

#if defined(__CONCURRENT__) && defined(__TREE_WITH_CACHE__)
                vtNodes[nIdx - 1].second->m_nUseCounter.fetch_add(1, std::memory_order_relaxed);
#else //__CONCURRENT__ && __TREE_WITH_CACHE__
                if constexpr (CacheType::MARK_INUSE_FLAG) vtNodes[nIdx - 1].second->m_bInUse = true;
#endif //__CONCURRENT__ && __TREE_WITH_CACHE__
            }
            else
            {
                break;
            }
        }

        if (nIdx == 1 && vtNodes[1].second != nullptr)
        {
#ifdef __CONCURRENT__
            ASSERT(!m_mutex.try_lock());
#endif //__CONCURRENT__

            m_ptrCache->template createObjectOfType<IndexNodeType>(m_uidRootNode, m_ptrRootNode, m_nDegree, pivotKey, vtNodes[1].first->m_uid, vtNodes[1].first, vtNodes[1].second->m_uid, vtNodes[1].second);
#ifdef __SELECTIVE_UPDATE__
            hasNewNodes = true;  // New root node created
#endif
            vtNodes[0].first = m_ptrRootNode;

#if defined(__CONCURRENT__) && defined(__TREE_WITH_CACHE__)
            vtNodes[nIdx - 1].first->m_nUseCounter.fetch_add(1, std::memory_order_relaxed);
#else //__CONCURRENT__ && __TREE_WITH_CACHE__
            if constexpr (CacheType::MARK_INUSE_FLAG) vtNodes[nIdx - 1].first->m_bInUse = true;
#endif //__CONCURRENT__ && __TREE_WITH_CACHE__

#ifdef __TREE_WITH_CACHE__
            nNodeLevel++;
#endif //__TREE_WITH_CACHE__
        }

#ifdef __CONCURRENT__
        vtLocks.clear();
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
#ifdef __SELECTIVE_UPDATE__
        m_ptrCache->updateObjectsAccessMetadata(nNodeLevel, vtNodes, hasNewNodes);
#else
        m_ptrCache->updateObjectsAccessMetadata(nNodeLevel, vtNodes);
#endif // __SELECTIVE_UPDATE__
        //all nodes->use_count--;
        vtNodes.clear();
#endif //__TREE_WITH_CACHE__

        return nResult;
    }

    ErrorCode search(const KeyType& key, ValueType& value)
    {
        ErrorCode nResult = ErrorCode::Error;

        ObjectTypePtr ptrLastNode = nullptr;
        ObjectTypePtr ptrCurrentNode = nullptr;

#ifdef __CONCURRENT__
        std::vector<std::shared_lock<std::shared_mutex>> vtLocks;
        vtLocks.emplace_back(std::shared_lock<std::shared_mutex>(m_mutex));
        //vtLocks.emplace_back(std::shared_lock<std::shared_mutex>(m_ptrRootNode->m_mtx));
        vtLocks.emplace_back([&]() {
            std::shared_lock<std::shared_mutex> lock(m_ptrRootNode->m_mtx);
#ifdef __TREE_WITH_CACHE__
            m_ptrRootNode->m_nUseCounter.fetch_add(1, std::memory_order_relaxed);
#endif //__TREE_WITH_CACHE__
            return lock;
            }());
#else //__CONCURRENT__
        if constexpr (CacheType::MARK_INUSE_FLAG) m_ptrRootNode->m_bInUse = true;
#endif //__CONCURRENT__

        ptrCurrentNode = m_ptrRootNode;

#ifdef __TREE_WITH_CACHE__
        int16_t nNodeLevel = 0;
#ifdef __SELECTIVE_UPDATE__
        bool hasNewNodes = false;
#endif
        
        std::vector<ObjectTypePtr, std::allocator<ObjectTypePtr>> vtNodes;
        vtNodes.reserve(20);
#endif //__TREE_WITH_CACHE__

        do
        {
#ifdef __CONCURRENT__
            vtLocks.erase(vtLocks.begin(), vtLocks.end() - 1); 
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
            nNodeLevel++;
            //all nodes->use_count--;
            vtNodes.push_back(ptrCurrentNode);
#endif //__TREE_WITH_CACHE__

            if(ptrCurrentNode->m_nCoreObjectType == IndexNodeType::UID)
            {
                IndexNodeType* ptrIndexNode = reinterpret_cast<IndexNodeType*>(ptrCurrentNode->m_ptrCoreObject);

                ptrLastNode = ptrCurrentNode;   // todo vtnodes last idx...??

#ifdef __TREE_WITH_CACHE__
#ifdef __SELECTIVE_UPDATE__
#ifdef __CONCURRENT__
                if (ptrIndexNode->template getChild_readonly<CacheType>(m_ptrCache, key, ptrCurrentNode, vtLocks, hasNewNodes))
#else //__CONCURRENT__
                if (ptrIndexNode->template getChild_readonly<CacheType>(m_ptrCache, key, ptrCurrentNode, hasNewNodes))
#endif //__CONCURRENT__
                {
                    ptrLastNode->m_bDirty = true;
                }
#else // !__SELECTIVE_UPDATE__
#ifdef __CONCURRENT__
                if (ptrIndexNode->template getChild_readonly<CacheType>(m_ptrCache, key, ptrCurrentNode, vtLocks))
#else //__CONCURRENT__
                if (ptrIndexNode->template getChild_readonly<CacheType>(m_ptrCache, key, ptrCurrentNode))
#endif //__CONCURRENT__
                {
                    ptrLastNode->m_bDirty = true;
                }
#endif // __SELECTIVE_UPDATE__

                //all nodes->use_count--;

#else //__TREE_WITH_CACHE__
#ifdef __CONCURRENT__
                ptrIndexNode->getChild(key, ptrCurrentNode, vtLocks);
#else //__CONCURRENT__
                ptrIndexNode->getChild(key, ptrCurrentNode);
#endif //__CONCURRENT__
#endif //__TREE_WITH_CACHE__
            }
            else
            {
                DataNodeType* ptrDataNode = reinterpret_cast<DataNodeType*>(ptrCurrentNode->m_ptrCoreObject);

                nResult = ptrDataNode->getValue(key, value);

                break;
            }

        } while (true);

#ifdef __CONCURRENT__
        vtLocks.clear();
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
#ifdef __SELECTIVE_UPDATE__
        m_ptrCache->updateObjectsAccessMetadata(nNodeLevel, vtNodes, hasNewNodes);
#else
        m_ptrCache->updateObjectsAccessMetadata(nNodeLevel, vtNodes);
#endif // __SELECTIVE_UPDATE__
        //all nodes->use_count--;
        vtNodes.clear();
#endif //__TREE_WITH_CACHE__

        return nResult;
    }

    ErrorCode remove(const KeyType& key)
    {
        ErrorCode nResult = ErrorCode::Error;

        ObjectTypePtr ptrLastNode = nullptr;
        ObjectTypePtr ptrCurrentNode = nullptr;
        ObjectTypePtr ptrNodeToDiscard = nullptr;

        uint16_t nNodeSplitPosBegin = 0, nNodeSplitPosEnd = 0;
        std::vector<OpDeleteInfo> vtNodes;
        vtNodes.reserve(20);        
        //std::vector<std::pair<ObjectTypePtr, ObjectTypePtr>> vtNodes; // reserve!!! + aligned!!

#ifdef __TREE_WITH_CACHE__
        int nNodeLevel = 0;
#ifdef __SELECTIVE_UPDATE__
        bool hasNewNodes = false;
#endif
#endif //__TREE_WITH_CACHE__

#ifdef __CONCURRENT__
        std::vector<std::unique_lock<std::shared_mutex>> vtLocks;
        vtLocks.emplace_back(std::unique_lock<std::shared_mutex>(m_mutex));
        //vtLocks.emplace_back(std::unique_lock<std::shared_mutex>(m_ptrRootNode->m_mtx));
        vtLocks.emplace_back([&]() {
            std::unique_lock<std::shared_mutex> lock(m_ptrRootNode->m_mtx);
#ifdef __TREE_WITH_CACHE__
            m_ptrRootNode->m_nUseCounter.fetch_add(1, std::memory_order_relaxed);
#endif //__TREE_WITH_CACHE__
            return lock;
            }());
#else //__CONCURRENT__
        if constexpr (CacheType::MARK_INUSE_FLAG) m_ptrRootNode->m_bInUse = true;
#endif //__CONCURRENT__
        ptrCurrentNode = m_ptrRootNode;

#ifdef __TREE_WITH_CACHE__
        //all nodes->use_count--;
        ValueType oEmpty;
        m_ptrCache->log(1, key, oEmpty);
#endif //__TREE_WITH_CACHE__

        do
        {
#ifdef __TREE_WITH_CACHE__
            nNodeLevel++;
#endif //__TREE_WITH_CACHE__

            if (ptrCurrentNode->m_nCoreObjectType == IndexNodeType::UID)
            {
                IndexNodeType* ptrIndexNode = reinterpret_cast<IndexNodeType*>(ptrCurrentNode->m_ptrCoreObject);

                if (!ptrIndexNode->canTriggerMerge())
                {
#ifdef __CONCURRENT__
                    vtLocks.erase(vtLocks.begin(), vtLocks.end() - 1);
#endif //__CONCURRENT__

                    nNodeSplitPosEnd = vtNodes.size();
                }

                vtNodes.emplace_back(ptrCurrentNode, nullptr, nullptr);

                ptrLastNode = ptrCurrentNode;

#ifdef __TREE_WITH_CACHE__
#ifdef __SELECTIVE_UPDATE__
#ifdef __CONCURRENT__
                if (ptrIndexNode->template getChild<CacheType>(m_ptrCache, key, ptrCurrentNode, vtLocks, hasNewNodes))
#else //__CONCURRENT__
                if (ptrIndexNode->template getChild<CacheType>(m_ptrCache, key, ptrCurrentNode, hasNewNodes))
#endif //__CONCURRENT__
                {
                    ptrLastNode->m_bDirty = true;
                }
#else // !__SELECTIVE_UPDATE__
#ifdef __CONCURRENT__
                if (ptrIndexNode->template getChild<CacheType>(m_ptrCache, key, ptrCurrentNode, vtLocks))
#else //__CONCURRENT__
                if (ptrIndexNode->template getChild<CacheType>(m_ptrCache, key, ptrCurrentNode))
#endif //__CONCURRENT__
                {
                    ptrLastNode->m_bDirty = true;
                }
#endif // __SELECTIVE_UPDATE__

#else //__TREE_WITH_CACHE__
#ifdef __CONCURRENT__
                ptrIndexNode->getChild(key, ptrCurrentNode, vtLocks);
#else //__CONCURRENT__
                ptrIndexNode->getChild(key, ptrCurrentNode);
#endif //__CONCURRENT__
#endif //__TREE_WITH_CACHE__
            }
            else
            {
                DataNodeType* ptrDataNode = reinterpret_cast<DataNodeType*>(ptrCurrentNode->m_ptrCoreObject);

                nResult = ptrDataNode->remove(key);
                if (nResult != ErrorCode::Success)
                {
                    nNodeSplitPosBegin = 0;

                    break;
                }

#ifdef __TREE_WITH_CACHE__
                ptrCurrentNode->m_bDirty = true;
#endif //__TREE_WITH_CACHE__

                if (ptrDataNode->requireMerge() && ptrLastNode != nullptr)
                {
                    IndexNodeType* ptrParentNode = reinterpret_cast<IndexNodeType*>(ptrLastNode->m_ptrCoreObject);

#ifdef __TREE_WITH_CACHE__
                    ObjectTypePtr ptrAffectedChildSibling = nullptr;
#ifdef __SELECTIVE_UPDATE__
                    ptrParentNode->template rebalanceDataNode<CacheType>(m_ptrCache, ptrDataNode, key, ptrNodeToDiscard, ptrAffectedChildSibling, hasNewNodes);
#else //__SELECTIVE_UPDATE__
                    ptrParentNode->template rebalanceDataNode<CacheType>(m_ptrCache, ptrDataNode, key, ptrNodeToDiscard, ptrAffectedChildSibling);
#endif //__SELECTIVE_UPDATE__                    
#else //__TREE_WITH_CACHE__
                    ptrParentNode->template rebalanceDataNode<CacheType>(m_ptrCache, ptrDataNode, key, ptrNodeToDiscard);
#endif //__TREE_WITH_CACHE__

#ifdef __TREE_WITH_CACHE__
                    ptrLastNode->m_bDirty = true;
                    ptrCurrentNode->m_bDirty = true;
#endif //__TREE_WITH_CACHE__

#ifdef __CONCURRENT__
                    vtLocks.pop_back();
#endif //__CONCURRENT__

                    if (ptrNodeToDiscard != nullptr)
                    {
                        if (ptrCurrentNode == ptrNodeToDiscard)
                        {
                            ptrCurrentNode = nullptr;
                        }

#ifdef __TREE_WITH_CACHE__
                        ASSERT(ptrAffectedChildSibling != ptrNodeToDiscard);
#endif //__TREE_WITH_CACHE__


#ifndef __TREE_WITH_CACHE__
                        m_ptrCache->remove(ptrNodeToDiscard);
#else //__TREE_WITH_CACHE__
#ifdef __SELECTIVE_UPDATE__
            hasNewNodes = true;  // New root node created
#endif
                        ptrNodeToDiscard->m_bMarkDelete = true;
#endif //__TREE_WITH_CACHE__
                    }

                    nNodeSplitPosBegin = vtNodes.size() - 1;

#ifdef __TREE_WITH_CACHE__
                    vtNodes.emplace_back(ptrCurrentNode, ptrAffectedChildSibling, ptrNodeToDiscard);
#else //__TREE_WITH_CACHE__
                    vtNodes.emplace_back(ptrCurrentNode, nullptr, nullptr);
#endif //__TREE_WITH_CACHE__
                }
                else
                {
#ifdef __CONCURRENT__
                    vtLocks.clear();
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
                    vtNodes.emplace_back(ptrCurrentNode, nullptr, nullptr); // Cache might need to be 
#else //__TREE_WITH_CACHE__
                    vtNodes.clear();    // All set
#endif //__TREE_WITH_CACHE__

                    nNodeSplitPosBegin = 0;

#ifdef __TREE_WITH_CACHE__
#ifdef __SELECTIVE_UPDATE__
                    m_ptrCache->updateObjectsAccessMetadata(nNodeLevel, vtNodes, hasNewNodes);
#else
                    m_ptrCache->updateObjectsAccessMetadata(nNodeLevel, vtNodes);
#endif // __SELECTIVE_UPDATE__
                    vtNodes.clear();
#endif //__TREE_WITH_CACHE__

                    return nResult;
                }

                break;
            }
        } while (true);

        int nIdx = nNodeSplitPosBegin;
        for (; nIdx > nNodeSplitPosEnd; --nIdx)
        {
            ptrCurrentNode = vtNodes[nIdx].m_ptrPrimary;
            ptrLastNode = vtNodes[nIdx - 1].m_ptrPrimary;

            bool bReleaseLock = true;
            //ptrNodeToDiscard = nullptr;

            IndexNodeType* ptrParentIndexNode = reinterpret_cast<IndexNodeType*>(ptrLastNode->m_ptrCoreObject);
            IndexNodeType* ptrChildIndexNode = reinterpret_cast<IndexNodeType*>(ptrCurrentNode->m_ptrCoreObject);

#ifdef __CONCURRENT__
            ASSERT(vtLocks.size() >= 2);  // skip if do not have lock for the parent node.
#endif //__CONCURRENT__

            if (ptrChildIndexNode->requireMerge())
            {
#ifdef __TREE_WITH_CACHE__
#ifdef __SELECTIVE_UPDATE__
                ptrParentIndexNode->template rebalanceIndexNode<CacheType>(m_ptrCache, ptrChildIndexNode, key, vtNodes[nIdx].m_ptrToDiscard, vtNodes[nIdx].m_ptrAffectedSibling, hasNewNodes);
#else //__SELECTIVE_UPDATE__
                ptrParentIndexNode->template rebalanceIndexNode<CacheType>(m_ptrCache, ptrChildIndexNode, key, vtNodes[nIdx].m_ptrToDiscard, vtNodes[nIdx].m_ptrAffectedSibling);
#endif //__SELECTIVE_UPDATE__
#else //__TREE_WITH_CACHE__
                ptrParentIndexNode->rebalanceIndexNode(m_ptrCache, ptrChildIndexNode, key, ptrNodeToDiscard);
#endif //__TREE_WITH_CACHE__

#ifdef __TREE_WITH_CACHE__
                ptrCurrentNode->m_bDirty = true;
                ptrLastNode->m_bDirty = true;
#endif //__TREE_WITH_CACHE__

                if (vtNodes[nIdx].m_ptrToDiscard != nullptr)
                {
#ifdef __CONCURRENT__
                    vtLocks.pop_back();
                    bReleaseLock = false;
#endif //__CONCURRENT__

                    if (vtNodes[nIdx].m_ptrPrimary == vtNodes[nIdx].m_ptrToDiscard)
                    {
                        vtNodes[nIdx].m_ptrPrimary = nullptr;
                    }

#ifndef __TREE_WITH_CACHE__
                    ASSERT(vtNodes[nIdx].first != ptrNodeToDiscard);
                    m_ptrCache->remove(vtNodes[nIdx].m_ptrToDiscard);
#else //__TREE_WITH_CACHE__
#ifdef __SELECTIVE_UPDATE__
            hasNewNodes = true;  // New root node created
#endif

                    vtNodes[nIdx].m_ptrToDiscard->m_bMarkDelete = true;
#endif //__TREE_WITH_CACHE__

                }
            }
            else
            {
                break;
            }

#ifdef __CONCURRENT__
            if (bReleaseLock)
            {
                vtLocks.pop_back();
            }
#endif //__CONCURRENT__

        }

        if (vtNodes.size() > 0 && nIdx == 0 && vtNodes[nIdx].m_ptrPrimary->m_nCoreObjectType == IndexNodeType::UID)
        {
            IndexNodeType* ptrInnerNode = reinterpret_cast<IndexNodeType*>(vtNodes[nIdx].m_ptrPrimary->m_ptrCoreObject);
            if (ptrInnerNode->getKeysCount() == 0)
            {
#ifdef __CONCURRENT__
                ASSERT(vtLocks.size() >= 2);  // skip if do not have lock for the parent node.
                ASSERT(!m_mutex.try_lock());
#endif //__CONCURRENT__


#ifdef __CONCURRENT__
                vtLocks.pop_back();
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
                ptrInnerNode->suppressCurrentLevel(m_uidRootNode, m_ptrRootNode);
#else //__TREE_WITH_CACHE__
                ptrInnerNode->suppressCurrentLevel(m_uidRootNode, m_ptrRootNode);
#endif //__TREE_WITH_CACHE__

#ifdef __TREE_WITH_CACHE__
                nNodeLevel--;
#endif //__TREE_WITH_CACHE__


#ifndef __TREE_WITH_CACHE__
                ASSERT(vtNodes[nIdx].m_ptrAffectedSibling == nullptr);
                m_ptrCache->remove(vtNodes[nIdx].m_ptrPrimary);
#else //__TREE_WITH_CACHE__
#ifdef __SELECTIVE_UPDATE__
            hasNewNodes = true;  // New root node created
#endif

                ASSERT(vtNodes[nIdx].m_ptrAffectedSibling == nullptr);
                
                vtNodes[nIdx].m_ptrPrimary->m_bMarkDelete = true;
                vtNodes[nIdx].m_ptrToDiscard = vtNodes[nIdx].m_ptrPrimary;
                vtNodes[nIdx].m_ptrPrimary = nullptr;
#endif //__TREE_WITH_CACHE__

                if (vtNodes[nIdx + 1].m_ptrPrimary != nullptr) ASSERT (m_ptrRootNode == vtNodes[nIdx + 1].m_ptrPrimary);
                if (vtNodes[nIdx + 1].m_ptrAffectedSibling != nullptr) ASSERT(m_ptrRootNode == vtNodes[nIdx + 1].m_ptrAffectedSibling);

#ifdef __TREE_WITH_CACHE__
                m_ptrRootNode->m_bDirty = true;// Not needed!
#endif //__TREE_WITH_CACHE__
            }
        }

#ifdef __CONCURRENT__
        vtLocks.clear();
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
#ifdef __SELECTIVE_UPDATE__
        m_ptrCache->updateObjectsAccessMetadata(nNodeLevel, vtNodes, hasNewNodes);
#else
        m_ptrCache->updateObjectsAccessMetadata(nNodeLevel, vtNodes);
#endif // __SELECTIVE_UPDATE__
        vtNodes.clear();
#endif //__TREE_WITH_CACHE__

        return nResult;
    }

#ifdef __TREE_WITH_CACHE__
    ErrorCode flush()
    {
        m_ptrCache->flush();

        //if (m_ptrRootNode == nullptr)
        //{
        //    m_ptrCache->getObject(m_nDegree, m_uidRootNode, m_ptrRootNode);
        //}
        //else if (m_ptrRootNode->m_ptrCoreObject == nullptr)
        //{
        //    if (m_ptrRootNode->m_uidUpdated != std::nullopt)
        //    {
        //        m_uidRootNode = *m_ptrRootNode->m_uidUpdated;
        //        m_ptrCache->getCoreObject(m_nDegree, m_uidRootNode, m_ptrRootNode);
        //    }
        //    else
        //    {
        //        m_ptrCache->getObject(m_nDegree, m_uidRootNode, m_ptrRootNode);
        //    }
        //}

        return ErrorCode::Success;
    }

    void getObjectsCountInCache(size_t& nObjects)
    {
        return m_ptrCache->getObjectsCountInCache(nObjects);
    }
#endif //__TREE_WITH_CACHE__

#ifndef __SELECTIVE_UPDATE__
    void print(std::ofstream & os)
    {
        int nSpace = 7;

        std::string prefix;

        os << prefix << "|" << std::endl;
        os << prefix << "|" << std::string(nSpace, '-').c_str() << "(root)";

        os << std::endl;

        std::vector<ObjectTypePtr> vtAccessedNodes;
        vtAccessedNodes.push_back(m_ptrRootNode);

        if (m_ptrRootNode->m_nCoreObjectType == IndexNodeType::UID)
        {
            IndexNodeType* ptrIndexNode = reinterpret_cast<IndexNodeType*>(m_ptrRootNode->m_ptrCoreObject);

#ifdef __TREE_WITH_CACHE__
            if (ptrIndexNode->template print<CacheType, ObjectTypePtr>(os, m_ptrCache, 0, prefix))
            {
                m_ptrRootNode->m_bDirty = true;
            }
#else //__TREE_WITH_CACHE__
            ptrIndexNode->template print<CacheType, ObjectTypePtr>(os, m_ptrCache, 0, prefix);
#endif //__TREE_WITH_CACHE__
        }
        else
        {
            DataNodeType* ptrDataNode = reinterpret_cast<DataNodeType*>(m_ptrRootNode->m_ptrCoreObject);

            ptrDataNode->print(os, 0, prefix);
        }

#ifdef __TREE_WITH_CACHE__
        //m_ptrCache->updateObjectsAccessMetadata(vtAccessedNodes);
        vtAccessedNodes.clear();
#endif //__TREE_WITH_CACHE__
    }
#endif //__SELECTIVE_UPDATE__
};
