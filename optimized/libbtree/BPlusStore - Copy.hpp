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

private:

#ifndef __PROD__
    uint32_t m_nDegree;
#endif //__PROD__

    std::shared_ptr<CacheType> m_ptrCache;

    ObjectTypePtr m_ptrRootNode;
    std::optional<ObjectUIDType> m_uidRootNode;

#ifdef __CONCURRENT__
    mutable std::shared_mutex m_mutex;
#endif //__CONCURRENT__

public:
    ~BPlusStore()
    {
        m_ptrCache.reset();
    }

    template<typename... CacheArgs>
    BPlusStore(uint32_t nDegree, CacheArgs... args)
        : m_ptrRootNode(nullptr)
        , m_uidRootNode(std::nullopt)
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

    ErrorCode insert(const KeyType& key, const ValueType& value)
    {
        ErrorCode nResult = ErrorCode::Error;

#ifdef __TREE_WITH_CACHE__
        bool bSkipLRUUpdate = false;
        std::vector<ObjectTypePtr> vtAccessedNodes;
        std::vector<int> vtAccessedNodes__;
#endif //__TREE_WITH_CACHE__

        ObjectUIDType uidLastNode, uidCurrentNode;
        ObjectTypePtr ptrLastNode = nullptr, ptrCurrentNode = nullptr;

        KeyType pivotKey;
        std::optional<ObjectUIDType> uidRHSChildNode, uidLHSChildNode;
        ObjectTypePtr ptrRHSChildNode = nullptr, ptrLHSChildNode = nullptr;

        std::vector<std::pair<ObjectUIDType, ObjectTypePtr>> vtNodes;

#ifdef __CONCURRENT__
        std::vector<std::unique_lock<std::shared_mutex>> vtLocks;
        vtLocks.emplace_back(std::unique_lock<std::shared_mutex>(m_mutex));
#ifdef __TREE_WITH_CACHE__
        m_ptrCache->log(0, key, value);
#endif //__TREE_WITH_CACHE__
#else //__CONCURRENT__
#ifdef __TREE_WITH_CACHE__
        m_ptrCache->log(0, key, value);
#endif //__TREE_WITH_CACHE__
#endif //__CONCURRENT__

        uidCurrentNode = m_uidRootNode.value();
        ptrCurrentNode = m_ptrRootNode;

#ifdef __CONCURRENT__
        vtLocks.emplace_back(std::unique_lock<std::shared_mutex>(ptrCurrentNode->m_mtx));
#endif //__CONCURRENT__
        int level = 0;
        do
        {
            level++;
#ifdef __TREE_WITH_CACHE__
            vtAccessedNodes.push_back(ptrCurrentNode);
            vtAccessedNodes__.push_back(level);
            if constexpr (std::is_same_v<CacheType, LRUCache<Traits>>) 
            {
                if (ptrCurrentNode->m_ptrNext == nullptr) { bSkipLRUUpdate = true; }
            }
            else if constexpr (std::is_same_v<CacheType, CLOCKCache<Traits>>)
            {
                //ptrCurrentNode->m_nFlushPriority = level;
            }

#endif //__TREE_WITH_CACHE__

            if (ptrCurrentNode->m_nCoreObjectType == IndexNodeType::UID)
            {
                vtNodes.push_back(std::pair<ObjectUIDType, ObjectTypePtr>(uidCurrentNode, ptrCurrentNode));

                IndexNodeType* ptrIndexNode = reinterpret_cast<IndexNodeType*>(ptrCurrentNode->m_ptrCoreObject);

#ifndef __PROD__
                if (!ptrIndexNode->canTriggerSplit())
#else //__PROD__
                if (!ptrIndexNode->canTriggerSplit())
#endif //__PROD__
                {
#ifdef __CONCURRENT__
                    vtLocks.erase(vtLocks.begin(), vtLocks.end() - 2);
#endif //__CONCURRENT__
                    vtNodes.erase(vtNodes.begin(), vtNodes.end() - 1);
                }
                else
                {
#ifdef __TREE_WITH_CACHE__
                    vtAccessedNodes.push_back(nullptr);
                    vtAccessedNodes__.push_back(0);
#endif //__TREE_WITH_CACHE__
                }

                uidLastNode = uidCurrentNode;
                ptrLastNode = ptrCurrentNode;

#ifdef __TREE_WITH_CACHE__
#ifdef __CONCURRENT__
                if (ptrIndexNode->template getChild<CacheType>(m_ptrCache, key, uidCurrentNode, ptrCurrentNode, vtLocks))
#else //__CONCURRENT__
                if (ptrIndexNode->template getChild<CacheType>(m_ptrCache, key, uidCurrentNode, ptrCurrentNode))
#endif //__CONCURRENT__
                {
                    ptrLastNode->m_bDirty = true;
                }
#else //__TREE_WITH_CACHE__
#ifdef __CONCURRENT__
                ptrIndexNode->getChild(key, uidCurrentNode, ptrCurrentNode, vtLocks);
#else //__CONCURRENT__
                ptrIndexNode->getChild(key, uidCurrentNode, ptrCurrentNode);
#endif //__CONCURRENT__
                
#endif //__TREE_WITH_CACHE__
            }
            else
            {
                DataNodeType* ptrDataNode = reinterpret_cast<DataNodeType*>(ptrCurrentNode->m_ptrCoreObject);

                if (ptrDataNode->insert(key, value) != ErrorCode::Success)
                {
#ifdef __CONCURRENT__
                    vtLocks.clear();
#endif //__CONCURRENT__
                    vtNodes.clear();

                    return ErrorCode::InsertFailed;
                }


#ifdef __TREE_WITH_CACHE__
                ptrCurrentNode->m_bDirty = true;
#endif //__TREE_WITH_CACHE__

                nResult = ErrorCode::Success;

#ifndef __PROD__
                if (ptrDataNode->requireSplit())
#else //__PROD__
                if (ptrDataNode->requireSplit())
#endif //__PROD__
                {
                    ErrorCode errCode = ptrDataNode->template split<CacheType, ObjectTypePtr>(m_ptrCache, uidRHSChildNode, ptrRHSChildNode, pivotKey);

                    if (errCode != ErrorCode::Success)
                    {
                        std::cout << "Critical State: Failed to split DataNode." << std::endl;
                        throw new std::logic_error("Critical State: Failed to split DataNode.");   // TODO: critical log.
                    }

                    uidLHSChildNode = uidCurrentNode;
                    ptrLHSChildNode = ptrCurrentNode;

#ifdef __TREE_WITH_CACHE__
                    vtAccessedNodes.push_back(ptrRHSChildNode);
                    vtAccessedNodes__.push_back(level);
                    if constexpr (std::is_same_v<CacheType, CLOCKCache<Traits>>)
                    {
                        //ptrRHSChildNode->m_nFlushPriority = level;
                    }
                    
		            bSkipLRUUpdate = true;  // A new node is added!
#endif //__TREE_WITH_CACHE__
                }
                else
                {
#ifdef __CONCURRENT__
                    vtLocks.clear();
#endif //__CONCURRENT__
                    vtNodes.clear();
                }

                break;
            }
        } while (true);

        while (vtNodes.size() > 0)
        {
            uidCurrentNode = vtNodes.back().first;
            ptrCurrentNode = vtNodes.back().second;

            IndexNodeType* ptrIndexNode = reinterpret_cast<IndexNodeType*>(ptrCurrentNode->m_ptrCoreObject);

            if (ptrIndexNode->insert(pivotKey, *uidRHSChildNode, ptrRHSChildNode) != ErrorCode::Success)
            {
                // TODO: Should update be performed on cloned objects first?
                std::cout << "Critical State: Failed to perform insert operation to the IndexNode." << std::endl;
                throw new std::logic_error(".....");   // TODO: critical log.
            }

#ifdef __TREE_WITH_CACHE__
            ptrCurrentNode->m_bDirty = true;
#endif //__TREE_WITH_CACHE__

            uidRHSChildNode = std::nullopt;
            ptrRHSChildNode = nullptr;

#ifndef __PROD__
            if (ptrIndexNode->requireSplit())
#else //__PROD__
            if (ptrIndexNode->requireSplit())
#endif //__PROD__
            {
                if (ptrIndexNode->template split<CacheType>(m_ptrCache, uidRHSChildNode, ptrRHSChildNode, pivotKey) != ErrorCode::Success)
                {
                    // TODO: Should update be performed on cloned objects first?
                    std::cout << "Critical State: Failed to split DataNode." << std::endl;
                    throw new std::logic_error(".....");   // TODO: critical log.
                }

#ifdef __TREE_WITH_CACHE__

#ifdef __ENABLE_ASSERTS__
                bool bTest = false;
#endif //__ENABLE_ASSERTS__
                for (auto it = vtAccessedNodes.rbegin(); it != vtAccessedNodes.rend(); it++)
                {
                    if (*it == nullptr)
                    {
#ifdef __ENABLE_ASSERTS__
                        bTest = true;
#endif //__ENABLE_ASSERTS__
                        if constexpr (std::is_same_v<CacheType, CLOCKCache<Traits>>)
                        {
                            ptrRHSChildNode->m_nFlushPriority = ptrCurrentNode->m_nFlushPriority;
                            
                            auto odx = std::distance(vtAccessedNodes.begin(), it.base()) - 1;
                            ASSERT(vtAccessedNodes[odx - 1]->m_uid == ptrCurrentNode->m_uid);
                            vtAccessedNodes__[odx] = vtAccessedNodes__[odx - 1];
                        }
                        
                        *it = ptrRHSChildNode;
                        bSkipLRUUpdate = true;
                        break;
                    }
                }

#ifdef __ENABLE_ASSERTS__
                if (!bTest)
                {
                    std::cout << "Critical State: Failed to push the new IndexNode (i.e. created due to the split operation) to the list to ensure Nodes' order in the Cache." << std::endl;
                    throw new std::logic_error(".....");   // TODO: critical log.
                }
#endif //__ENABLE_ASSERTS__
#endif //__TREE_WITH_CACHE__
            }

            uidLHSChildNode = uidCurrentNode;
            ptrLHSChildNode = ptrCurrentNode;

#ifdef __CONCURRENT__
            vtLocks.pop_back();
#endif //__CONCURRENT__

            vtNodes.pop_back();
        }

        if (uidCurrentNode == m_uidRootNode && ptrLHSChildNode != nullptr && ptrRHSChildNode != nullptr)
        {
            m_uidRootNode = std::nullopt;
            m_ptrCache->template createObjectOfType<IndexNodeType>(m_uidRootNode, m_ptrRootNode, m_nDegree, pivotKey, *uidLHSChildNode, ptrLHSChildNode, *uidRHSChildNode, ptrRHSChildNode);

#ifdef __TREE_WITH_CACHE__
            bSkipLRUUpdate = true;

            if constexpr (std::is_same_v<CacheType, CLOCKCache<Traits>>)
            {
                //level++;
                m_ptrRootNode->m_nFlushPriority = ptrRHSChildNode->m_nFlushPriority - 1;
                ASSERT(vtAccessedNodes[0]->m_uid == ptrLHSChildNode->m_uid);
                vtAccessedNodes__.insert(vtAccessedNodes__.begin(), vtAccessedNodes__[0] - 1);
            }
            
            vtAccessedNodes.insert(vtAccessedNodes.begin(), m_ptrRootNode);
#endif //__TREE_WITH_CACHE__
        }

#ifdef __CONCURRENT__
        vtLocks.clear();
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
        if constexpr (std::is_same_v<CacheType, LRUCache<Traits>>)
        {
            if (bSkipLRUUpdate) m_ptrCache->updateObjectsAccessMetadata(vtAccessedNodes);
        }
        else if constexpr (std::is_same_v<CacheType, CLOCKCache<Traits>>)
        {
            m_ptrCache->updateObjectsAccessMetadata(vtAccessedNodes, level + 1, vtAccessedNodes__);
        }

        //        
        vtAccessedNodes.clear();
#endif //__TREE_WITH_CACHE__



        return nResult;
    }

    ErrorCode search(const KeyType& key, ValueType& value)
    {
        ErrorCode nResult = ErrorCode::Error;

#ifdef __TREE_WITH_CACHE__
        std::vector<ObjectTypePtr> vtAccessedNodes;
        std::vector<int> vtAccessedNodes__;
#endif //__TREE_WITH_CACHE__

#ifdef __CONCURRENT__
        std::vector<std::unique_lock<std::shared_mutex>> vtLocks;
        vtLocks.emplace_back(std::unique_lock<std::shared_mutex>(m_mutex));
#endif //__CONCURRENT__

        ObjectTypePtr ptrCurrentNode = m_ptrRootNode;
        ObjectUIDType uidCurrentNode = m_uidRootNode.value();

#ifdef __CONCURRENT__
        vtLocks.emplace_back(std::unique_lock<std::shared_mutex>(ptrCurrentNode->m_mtx));
#endif //__CONCURRENT__

        bool bSkipLRUUpdate = false;
        int level = 0;
        do
        {
            level++;
#ifdef __CONCURRENT__
            vtLocks.erase(vtLocks.begin(), vtLocks.end() - 2); 
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
            vtAccessedNodes.push_back(ptrCurrentNode);
            vtAccessedNodes__.push_back(level);
            if constexpr (std::is_same_v<CacheType, LRUCache<Traits>>)
            {
                if (ptrCurrentNode->m_ptrNext == nullptr) bSkipLRUUpdate = true;
            } else if constexpr (std::is_same_v<CacheType, CLOCKCache<Traits>>)
            {
                //ptrCurrentNode->m_nFlushPriority = level;
            }
	     
#endif //__TREE_WITH_CACHE__

            if(ptrCurrentNode->m_nCoreObjectType == IndexNodeType::UID)
            {
                IndexNodeType* ptrIndexNode = reinterpret_cast<IndexNodeType*>(ptrCurrentNode->m_ptrCoreObject);

#ifdef __TREE_WITH_CACHE__
#ifdef __CONCURRENT__
                if (ptrIndexNode->template getChild<CacheType>(m_ptrCache, key, uidCurrentNode, ptrCurrentNode, vtLocks))
#else //__CONCURRENT__
                if (ptrIndexNode->template getChild<CacheType>(m_ptrCache, key, uidCurrentNode, ptrCurrentNode))
#endif //__CONCURRENT__
                {
                    vtAccessedNodes[vtAccessedNodes.size() - 1]->m_bDirty = true;
                }
#else //__TREE_WITH_CACHE__
#ifdef __CONCURRENT__
                ptrIndexNode->getChild(key, uidCurrentNode, ptrCurrentNode, vtLocks);
#else //__CONCURRENT__
                ptrIndexNode->getChild(key, uidCurrentNode, ptrCurrentNode);
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
        if constexpr (std::is_same_v<CacheType, LRUCache<Traits>>)
        {
            if (bSkipLRUUpdate) m_ptrCache->updateObjectsAccessMetadata(vtAccessedNodes);
        }
        else if constexpr (std::is_same_v<CacheType, CLOCKCache<Traits>>)
        {
            m_ptrCache->updateObjectsAccessMetadata(vtAccessedNodes, level + 1, vtAccessedNodes__);
        }

        vtAccessedNodes.clear();
#endif //__TREE_WITH_CACHE__


        return nResult;
    }

    ErrorCode remove(const KeyType& key)
    {
        ErrorCode nResult = ErrorCode::Error;

#ifdef __TREE_WITH_CACHE__
        std::vector<ObjectTypePtr> vtAccessedNodes;
        std::vector<int> vtAccessedNodes__;
#endif //__TREE_WITH_CACHE__

        ObjectUIDType uidLastNode, uidCurrentNode;
        ObjectTypePtr ptrLastNode = nullptr, ptrCurrentNode = nullptr;

        std::vector<std::pair<ObjectUIDType, ObjectTypePtr>> vtNodes;

#ifdef __CONCURRENT__
        std::vector<std::unique_lock<std::shared_mutex>> vtLocks;
        vtLocks.emplace_back(std::unique_lock<std::shared_mutex>(m_mutex));
#ifdef __TREE_WITH_CACHE__
        ValueType oEmpty;
        m_ptrCache->log(1, key, oEmpty);
#endif //__TREE_WITH_CACHE__
#else //__CONCURRENT__
#ifdef __TREE_WITH_CACHE__
        ValueType oEmpty;
        m_ptrCache->log(1, key, oEmpty);
#endif //__TREE_WITH_CACHE__
#endif //__CONCURRENT__

        uidCurrentNode = m_uidRootNode.value();
        ptrCurrentNode = m_ptrRootNode;

#ifdef __CONCURRENT__
        vtLocks.emplace_back(std::unique_lock<std::shared_mutex>(ptrCurrentNode->m_mtx));
#endif //__CONCURRENT__
    
        bool bSkipLRUUpdate = false;
        int level = 0;
        do
        {
            level++;

#ifdef __TREE_WITH_CACHE__
            vtAccessedNodes.push_back(ptrCurrentNode);
            vtAccessedNodes__.push_back(level);
            if constexpr (std::is_same_v<CacheType, LRUCache<Traits>>)
            {
                if (ptrCurrentNode->m_ptrNext == nullptr) bSkipLRUUpdate = true;
            }
            else if constexpr (std::is_same_v<CacheType, CLOCKCache<Traits>>)
            {
                //ptrCurrentNode->m_nFlushPriority = level;
            }

#endif //__TREE_WITH_CACHE__

            if (ptrCurrentNode->m_nCoreObjectType == IndexNodeType::UID)
            {
                vtNodes.push_back(std::pair<ObjectUIDType, ObjectTypePtr>(uidCurrentNode, ptrCurrentNode));

                IndexNodeType* ptrIndexNode = reinterpret_cast<IndexNodeType*>(ptrCurrentNode->m_ptrCoreObject);

#ifndef __PROD__
                if (!ptrIndexNode->canTriggerMerge())
#else //__PROD__
                if (!ptrIndexNode->canTriggerMerge())
#endif //__PROD__
                {
#ifdef __CONCURRENT__
                    // Although lock to the last node is enough. 
                    // However, the preceeding one is maintainig for the root node so that "m_uidRootNode" can be updated safely if the root node is needed to be deleted.
                    vtLocks.erase(vtLocks.begin(), vtLocks.end() - 2);
#endif //__CONCURRENT__
                    vtNodes.erase(vtNodes.begin(), vtNodes.end() - 1);
                }
                else
                {
#ifdef __TREE_WITH_CACHE__
                    vtAccessedNodes.push_back(nullptr);
                    vtAccessedNodes__.push_back(level);
#endif //__TREE_WITH_CACHE__
                }

                uidLastNode = uidCurrentNode;
                ptrLastNode = ptrCurrentNode;

#ifdef __TREE_WITH_CACHE__
#ifdef __CONCURRENT__
                if (ptrIndexNode->template getChild<CacheType>(m_ptrCache, key, uidCurrentNode, ptrCurrentNode, vtLocks))
#else //__CONCURRENT__
                if (ptrIndexNode->template getChild<CacheType>(m_ptrCache, key, uidCurrentNode, ptrCurrentNode))
#endif //__CONCURRENT__
                {
                    ptrLastNode->m_bDirty = true;
                }
#else //__TREE_WITH_CACHE__
#ifdef __CONCURRENT__
                ptrIndexNode->getChild(key, uidCurrentNode, ptrCurrentNode, vtLocks);
#else //__CONCURRENT__
                ptrIndexNode->getChild(key, uidCurrentNode, ptrCurrentNode);
#endif //__CONCURRENT__
                
#endif //__TREE_WITH_CACHE__
            }
            else
            {
                DataNodeType* ptrDataNode = reinterpret_cast<DataNodeType*>(ptrCurrentNode->m_ptrCoreObject);

                if (ptrDataNode->remove(key) != ErrorCode::Success)
                {

#ifdef __CONCURRENT__
                    vtLocks.clear();
#endif //__CONCURRENT__
                    vtNodes.clear();
                    
                    return ErrorCode::KeyDoesNotExist;
                }

#ifdef __TREE_WITH_CACHE__
                ptrCurrentNode->m_bDirty = true;
#endif //__TREE_WITH_CACHE__

                nResult = ErrorCode::Success;

#ifndef __PROD__
                if (ptrDataNode->requireMerge())
#else //__PROD__
                if (ptrDataNode->requireMerge())
#endif //__PROD__
                {
                    vtNodes.push_back(std::pair<ObjectUIDType, ObjectTypePtr>(uidCurrentNode, ptrCurrentNode));

                    if (ptrLastNode != nullptr)
                    {
                        std::optional<ObjectUIDType> uidObjectToDelete = std::nullopt;
                        ObjectTypePtr ptrObjectToDelete = nullptr;

                        IndexNodeType* ptrParentNode = reinterpret_cast<IndexNodeType*>(ptrLastNode->m_ptrCoreObject);
#ifdef __TREE_WITH_CACHE__
                        std::optional<ObjectUIDType> uidAffectedChildSibling = std::nullopt;
                        ObjectTypePtr ptrAffectedChildSibling = nullptr;

#ifndef __PROD__
                        ptrParentNode->template rebalanceDataNode<CacheType>(m_ptrCache, uidCurrentNode, ptrDataNode, key, uidObjectToDelete, ptrObjectToDelete, uidAffectedChildSibling, ptrAffectedChildSibling);
#else //__PROD__
                        ptrParentNode->template rebalanceDataNode<CacheType>(m_ptrCache, uidCurrentNode, ptrDataNode, key, uidObjectToDelete, ptrObjectToDelete, uidAffectedChildSibling, ptrAffectedChildSibling);
#endif //__PROD__

#else //__TREE_WITH_CACHE__
#ifndef __PROD__
                        ptrParentNode->template rebalanceDataNode<CacheType>(m_ptrCache, uidCurrentNode, ptrDataNode, key, uidObjectToDelete, ptrObjectToDelete);
#else //__PROD__
                        ptrParentNode->template rebalanceDataNode<CacheType>(m_ptrCache, uidCurrentNode, ptrDataNode, key, uidObjectToDelete, ptrObjectToDelete);
#endif //__PROD__
#endif //__TREE_WITH_CACHE__

#ifdef __TREE_WITH_CACHE__
                        ptrLastNode->m_bDirty = true;
                        ptrCurrentNode->m_bDirty = true;
#endif //__TREE_WITH_CACHE__


#ifdef __CONCURRENT__
                        vtLocks.pop_back();
#endif //__CONCURRENT__
                        vtNodes.pop_back();

                        if (uidObjectToDelete)
                        {
                            m_ptrCache->remove(ptrObjectToDelete);
                        }

#ifdef __TREE_WITH_CACHE__
                        vtAccessedNodes.push_back(ptrAffectedChildSibling);
                        ASSERT(vtAccessedNodes__[vtAccessedNodes__.size() - 1] == level);
                        vtAccessedNodes__.push_back(level);
                        if constexpr (std::is_same_v<CacheType, CLOCKCache<Traits>>)
                        {
                            ptrAffectedChildSibling->m_nFlushPriority = ptrCurrentNode->m_nFlushPriority;
                            ptrAffectedChildSibling->m_nFlushPriority = 0;
                        }
                        bSkipLRUUpdate = true;
#endif //__TREE_WITH_CACHE__
                    }
                }
                else
                {
#ifdef __CONCURRENT__
                    vtLocks.clear();
#endif //__CONCURRENT__
                    vtNodes.clear();
                }

                break;
            }
        } while (true);

        if (vtNodes.size() > 0)
        {
            ObjectUIDType uidChildNode = vtNodes.back().first;
            ObjectTypePtr ptrChildNode = vtNodes.back().second;

            vtNodes.pop_back();

            while (vtNodes.size() > 0)
            {
                //uidCurrentNode = vtNodes.back().first;
                ptrCurrentNode = vtNodes.back().second;

                bool bReleaseLock = true;
                ObjectTypePtr ptrObjectToDelete = nullptr;
                std::optional<ObjectUIDType> uidObjectToDelete = std::nullopt;

                IndexNodeType* ptrParentIndexNode = reinterpret_cast<IndexNodeType*>(ptrCurrentNode->m_ptrCoreObject);

                IndexNodeType* ptrChildIndexNode = reinterpret_cast<IndexNodeType*>(ptrChildNode->m_ptrCoreObject);

                if (ptrChildIndexNode->requireMerge())
                {
#ifdef __TREE_WITH_CACHE__
                    std::optional<ObjectUIDType> uidAffectedChildSibling = std::nullopt;
                    ObjectTypePtr ptrAffectedChildSibling = nullptr;

#ifndef __PROD__
                    ptrParentIndexNode->template rebalanceIndexNode<CacheType>(m_ptrCache, uidChildNode, ptrChildIndexNode, key, uidObjectToDelete, ptrObjectToDelete, uidAffectedChildSibling, ptrAffectedChildSibling);
#else //__PROD__
                    ptrParentIndexNode->template rebalanceIndexNode<CacheType>(m_ptrCache, uidChildNode, ptrChildIndexNode, key, uidObjectToDelete, ptrObjectToDelete, uidAffectedChildSibling, ptrAffectedChildSibling);
#endif //__PROD__

#else //__TREE_WITH_CACHE__
#ifdef __PROD__
                    ptrParentIndexNode->rebalanceIndexNode(m_ptrCache, uidChildNode, ptrChildIndexNode, key, uidObjectToDelete, ptrObjectToDelete);
#else //__PROD__
                    ptrParentIndexNode->rebalanceIndexNode(m_ptrCache, uidChildNode, ptrChildIndexNode, key, uidObjectToDelete, ptrObjectToDelete);
#endif //__PROD__
#endif //__TREE_WITH_CACHE__

#ifdef __TREE_WITH_CACHE__
                    ptrCurrentNode->m_bDirty = true;
                    ptrChildNode->m_bDirty = true;
		            bSkipLRUUpdate = true;

                    if constexpr (std::is_same_v<CacheType, CLOCKCache<Traits>>)
                    {
                        ptrAffectedChildSibling->m_nFlushPriority = ptrChildNode->m_nFlushPriority;
                    }

#ifdef __ENABLE_ASSERTS__
                    bool bTest = false;
#endif //__ENABLE_ASSERTS__

                    for (auto it = vtAccessedNodes.rbegin(); it != vtAccessedNodes.rend(); it++)
                    {
                        if (*it == nullptr)
                        {
#ifdef __ENABLE_ASSERTS__
                            bTest = true;
#endif //__ENABLE_ASSERTS__
                            auto odx = std::distance(vtAccessedNodes.begin(), it.base()) - 1;
                            ASSERT(vtAccessedNodes[odx - 1]->m_uid == ptrChildNode->m_uid);
                            ASSERT(vtAccessedNodes__[odx] = vtAccessedNodes__[odx - 1]);

                            *it = ptrAffectedChildSibling;
                            break;
                        }
                    }

#ifdef __ENABLE_ASSERTS__
                    if (!bTest)
                    {
                        std::cout << "Critical State: Failed to push the new IndexNode (i.e. created due to the merge operation) to the list to ensure Nodes' order in the Cache." << std::endl;
                        throw new std::logic_error(".....");   // TODO: critical log.
                    }
#endif //__ENABLE_ASSERTS__
#endif //__TREE_WITH_CACHE__

                    if (uidObjectToDelete)
                    {
#ifdef __CONCURRENT__
                        vtLocks.pop_back();
                        bReleaseLock = false;
#endif //__CONCURRENT__
                        bSkipLRUUpdate = true;
                        m_ptrCache->remove(ptrObjectToDelete);
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

                uidChildNode = vtNodes.back().first;
                ptrChildNode = vtNodes.back().second;
                vtNodes.pop_back();
            }

            if (ptrChildNode != nullptr && m_uidRootNode == uidChildNode)
            {
                if (ptrChildNode->m_nCoreObjectType == IndexNodeType::UID)
                {
                    IndexNodeType* ptrInnerNode = reinterpret_cast<IndexNodeType*>(ptrChildNode->m_ptrCoreObject);
                    if (ptrInnerNode->getKeysCount() == 0)
                    {
#ifdef __CONCURRENT__
                        vtLocks.pop_back();
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
                        ptrInnerNode->getFirstChildDetails(*m_uidRootNode, m_ptrRootNode);
#else //__TREE_WITH_CACHE__
                        ptrInnerNode->getFirstChildDetails(*m_uidRootNode, m_ptrRootNode);
#endif //__TREE_WITH_CACHE__



                        m_ptrCache->remove(ptrChildNode);
                        bSkipLRUUpdate = true;
#ifdef __TREE_WITH_CACHE__
                        ptrChildNode->m_bDirty = true;// Not needed!
#endif //__TREE_WITH_CACHE__


#ifdef __TREE_WITH_CACHE__

#ifdef __ENABLE_ASSERTS__
                        bool bTest = false;
#endif //__ENABLE_ASSERTS__
                        for (auto it = vtAccessedNodes.rbegin(); it != vtAccessedNodes.rend(); it++)
                        {
                            if (*it == nullptr)
                            {
#ifdef __ENABLE_ASSERTS__
                                bTest = true;
#endif //__ENABLE_ASSERTS__
                                *it = m_ptrRootNode;
                                auto odx = std::distance(vtAccessedNodes.begin(), it.base()) - 1;   //check this sometimes add duplicat!!!!!
                                //if (vtAccessedNodes[odx + 1]->m_uid == m_ptrRootNode->m_uid)
                                //    vtAccessedNodes__[odx] = vtAccessedNodes__[odx + 1] - 1;
                                //else
                                    ASSERT(vtAccessedNodes__[odx] = vtAccessedNodes__[odx - 1]); //fix this
                                break;
                            }
                        }

#ifdef __ENABLE_ASSERTS__
                        if (!bTest)
                        {
                            std::cout << "Critical State: Failed to push the new IndexNode (i.e. created due to the merge operation) to the list to ensure Nodes' order in the Cache." << std::endl;
                            throw new std::logic_error(".....");   // TODO: critical log.
                        }
#endif //__ENABLE_ASSERTS__
#endif //__TREE_WITH_CACHE__

                    }
                }
            }
        }

#ifdef __CONCURRENT__
        vtLocks.clear();
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
        if constexpr (std::is_same_v<CacheType, LRUCache<Traits>>)
        {
            if (bSkipLRUUpdate) m_ptrCache->updateObjectsAccessMetadata(vtAccessedNodes);
        }
        else if constexpr (std::is_same_v<CacheType, CLOCKCache<Traits>>)
        {
           m_ptrCache->updateObjectsAccessMetadata(vtAccessedNodes, level + 1, vtAccessedNodes__);
        }

        vtAccessedNodes.clear();
#endif //__TREE_WITH_CACHE__



        return nResult;
    }

#ifdef __TREE_WITH_CACHE__
    ErrorCode flush()
    {
        m_ptrCache->flush();

        if (m_ptrRootNode == nullptr)
        {
            m_ptrCache->getObject(m_nDegree, *m_uidRootNode, m_ptrRootNode);
        }
        else if (m_ptrRootNode->m_ptrCoreObject == nullptr)
        {
            if (m_ptrRootNode->m_uidUpdated != std::nullopt)
            {
                m_uidRootNode = *m_ptrRootNode->m_uidUpdated;
                m_ptrCache->getCoreObject(m_nDegree, *m_uidRootNode, m_ptrRootNode);
            }
            else
            {
                m_ptrCache->getObject(m_nDegree, *m_uidRootNode, m_ptrRootNode);
            }
        }

        return ErrorCode::Success;
    }

    void getObjectsCountInCache(size_t& nObjects)
    {
        return m_ptrCache->getObjectsCountInCache(nObjects);
    }
#endif //__TREE_WITH_CACHE__

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
};
