#include "indexmanager.h"
#include "record.h"
#include "tablemanager.h"
IndexManager::IndexManager(const char* filename)
{
    fileManager = new FileManager();
    bufPageManager = new BufPageManager(fileManager);
    if (!fileManager->openFile(filename, current_fileID))
        assert(false);
    int index0;
    BufType data = bufPageManager->getPage(current_fileID, 0, index0);
    bufPageManager->access(index0);
    memcpy(&current_fileHeader, data, sizeof(Index_FileHeader));
    recordparser = new RecordParser(current_fileHeader.type, current_fileHeader.maxlen, current_fileHeader.col_num, nullptr);
}
IndexManager::~IndexManager()
{
    int index0;
    BufType data0 = bufPageManager->getPage(current_fileID, 0, index0);
    memcpy(data0, &current_fileHeader, sizeof(Index_FileHeader));
    bufPageManager->markDirty(index0);
    bufPageManager->close();
    delete recordparser;
    if (fileManager->closeFile(current_fileID))
        assert(false);
    delete bufPageManager;
    delete fileManager;
}
bool IndexManager::CreateIndex(const char* filename, int num, const int* cols, const TableHeader* tableheader)
{
    FileManager* fileManager = new FileManager();
    BufPageManager* bufPageManager = new BufPageManager(fileManager);
    if (!fileManager->createFile(filename)) {
        delete bufPageManager;
        delete fileManager;
        return false;
    }
    int fileID;
    if (!fileManager->openFile(filename, fileID)) {
        delete bufPageManager;
        delete fileManager;
        return false;
    }

    Index_FileHeader header;
    header.col_num = num;
    int tot_len = ceil(num, 32);
    for (int i = 0; i < num; i++) {
        header.cols[i] = cols[i];
        header.type[i] = tableheader->col_type[cols[i]];
        header.maxlen[i] = tableheader->col_length[cols[i]];
        tot_len += tableheader->col_aligned_len[cols[i]];
    }
    header.m = (PAGE_SIZE - sizeof(Index_PageHeader)) / (sizeof(int) * (2 + tot_len));
    header.childStart = sizeof(Index_PageHeader) / sizeof(int);
    header.keyStart = header.childStart + header.m;
    header.m--;
    header.keyLen = tot_len;
    header.rootPage = 1;
    header.nodeNum = 2;
    int index;
    BufType buf = bufPageManager->getPage(fileID, 0, index);
    memcpy(buf, &header, sizeof(Index_FileHeader));
    bufPageManager->markDirty(index);
    bufPageManager->writeBack(index);

    Index_PageHeader pageHeader;
    pageHeader.isLeaf = true;
    pageHeader.keyNum = pageHeader.parent = 0;
    pageHeader.next = pageHeader.prev = 0;
    int pageIndex;
    BufType pageBuf = bufPageManager->getPage(fileID, 1, pageIndex);
    memcpy(pageBuf, &pageHeader, sizeof(Index_PageHeader));
    bufPageManager->markDirty(pageIndex);
    bufPageManager->writeBack(pageIndex);

    fileManager->closeFile(fileID);
    delete bufPageManager;
    delete fileManager;
    return true;
}
bool IndexManager::DeleteIndex(const char* filename)
{
    system(("rm " + std::string(filename)).c_str());
    return true;
}

Index_Node* IndexManager::getNode(int id)
{
    Index_Node* node = new Index_Node();
    BufType data = bufPageManager->getPage(current_fileID, id, node->index);
    bufPageManager->access(node->index);
    node->header = (Index_PageHeader*)data;
    node->child = data + current_fileHeader.childStart;
    node->key = data + current_fileHeader.keyStart;
    return node;
}
Record* IndexManager::getRecord(Index_Node* node, int i)
{
    BufType key = node->key + i * (current_fileHeader.keyLen + 1);
    int rid = *key;
    return recordparser->toRecord(rid, key + 1);
}
void IndexManager::setRecord(Index_Node* node, int i, Record* record)
{
    BufType key = node->key + i * (current_fileHeader.keyLen + 1);
    memcpy(key, recordparser->toData(record, true), (current_fileHeader.keyLen + 1) * sizeof(int));
}

void IndexManager::setRecord(Index_Node* node, int i, Index_Node* node_src, int i_src)
{
    BufType key = node->key + i * (current_fileHeader.keyLen + 1);
    BufType key_src = node_src->key + i_src * (current_fileHeader.keyLen + 1);
    memcpy(key, key_src, (current_fileHeader.keyLen + 1) * sizeof(int));
}
void IndexManager::setRecord(Index_Node* node, int i, BufType src)
{
    BufType key = node->key + i * (current_fileHeader.keyLen + 1);
    memcpy(key, src, (current_fileHeader.keyLen + 1) * sizeof(int));
}
int IndexManager::getChildIndex(int child, Index_Node* parent)
{
    for (int i = 0; i < parent->header->keyNum; i++) {
        if (parent->child[i] == child)
            return i;
    }
    return -1;
}

Record* IndexManager::truncate(Record* fullversion)
{
    Record* record = new Record;
    record->iscopy = 0;
    record->rid = fullversion->rid;
    record->num = current_fileHeader.col_num;
    record->type = current_fileHeader.type;
    record->datas = new BufType[current_fileHeader.col_num];
    for (int i = 0; i < current_fileHeader.col_num; i++) {
        record->datas[i] = fullversion->datas[current_fileHeader.cols[i]];
    }
    return record;
}
void IndexManager::Insert(Record* fullversion)
{
    Record* record = truncate(fullversion);
    BufType record_binary = recordparser->toData(record, true);
    int id = current_fileHeader.rootPage;
    while (1) {
        Index_Node* node = getNode(id);
        if (!node->header->isLeaf) {
            bufPageManager->access(node->index);
            for (int i = node->header->keyNum - 1; i >= 0; i--) {
                Record* record_i = getRecord(node, i);
                if (i == 0 || recordparser->compare(record_i, record, true) < 0) {
                    id = node->child[i];
                    delete record_i;
                    break;
                }
                delete record_i;
            }
            delete node;
        } else {
            bool flag = false;
            for (int i = node->header->keyNum - 1; i >= 0; i--) {
                Record* record_i = getRecord(node, i);
                if (recordparser->compare(record_i, record, true) < 0) {
                    flag = true;
                    setRecord(node, i + 1, record_binary);
                    node->child[i + 1] = 0;
                    delete record_i;
                    break;
                }
                delete record_i;
                setRecord(node, i + 1, node, i);
            }
            if (!flag) {
                setRecord(node, 0, record_binary);
                node->child[0] = 0;
            }
            node->header->keyNum++;
            while (node->header->keyNum > current_fileHeader.m) {
                Index_Node* parentNode;
                int parent = node->header->parent;
                if (parent == 0) {
                    parent = current_fileHeader.nodeNum++;
                    current_fileHeader.rootPage = parent;
                    parentNode = getNode(parent);
                    parentNode->header->keyNum = 1;
                    parentNode->header->isLeaf = false;
                    parentNode->header->parent = 0;
                    setRecord(parentNode, 0, node, 0);
                    parentNode->child[0] = id;
                    node->header->parent = parent;
                } else
                    parentNode = getNode(parent);
                int whichChild = getChildIndex(id, parentNode);
                if (whichChild == -1)
                    assert(false);
                for (int i = parentNode->header->keyNum; i > whichChild + 1; i--) {
                    parentNode->child[i] = parentNode->child[i - 1];
                    setRecord(parentNode, i, parentNode, i - 1);
                }

                parentNode->header->keyNum++;
                parentNode->child[whichChild + 1] = current_fileHeader.nodeNum++;
                int newID = parentNode->child[whichChild + 1];
                Index_Node* newNode = getNode(newID);
                newNode->header->isLeaf = node->header->isLeaf;
                newNode->header->keyNum = node->header->keyNum - node->header->keyNum / 2;
                newNode->header->parent = parent;
                node->header->keyNum /= 2;
                for (int i = 0; i < newNode->header->keyNum; i++) {
                    newNode->child[i] = node->child[i + node->header->keyNum];
                    if (!newNode->header->isLeaf && newNode->child[i] != 0) {
                        Index_Node* childNode = getNode(newNode->child[i]);
                        childNode->header->parent = newID;
                        bufPageManager->markDirty(childNode->index);
                        delete childNode;
                    }
                    setRecord(newNode, i, node, i + node->header->keyNum);
                }
                if (node->header->isLeaf) {
                    newNode->header->next = node->header->next;
                    newNode->header->prev = id;
                    if (node->header->next != 0) {
                        Index_Node* nextLeaf = getNode(node->header->next);
                        nextLeaf->header->prev = newID;
                        bufPageManager->markDirty(nextLeaf->index);
                        delete nextLeaf;
                    }
                    node->header->next = newID;
                }
                setRecord(parentNode, whichChild, node, 0);
                setRecord(parentNode, whichChild + 1, newNode, 0);
                bufPageManager->markDirty(node->index);
                bufPageManager->markDirty(newNode->index);
                bufPageManager->markDirty(parentNode->index);
                id = parent;
                delete node, newNode;
                node = parentNode;
            }
            bufPageManager->markDirty(node->index);
            while (id != current_fileHeader.rootPage) {
                Index_Node* parentNode = getNode(node->header->parent);
                int whichChild = getChildIndex(id, parentNode);
                if (whichChild == -1) {
                    assert(false);
                }
                setRecord(parentNode, whichChild, node, 0);
                bufPageManager->markDirty(parentNode->index);
                id = node->header->parent;
                delete node;
                node = parentNode;
            }
            delete node;
            break;
        }
    }
    delete record;
    delete[] record_binary;
}
void IndexManager::Delete(Record* fullversion)
{
    Record* record = truncate(fullversion);
    int id = current_fileHeader.rootPage;
    while (1) {
        Index_Node* node = getNode(id);
        if (!node->header->isLeaf) {
            bufPageManager->access(node->index);
            for (int i = node->header->keyNum - 1; i >= 0; i--) {
                Record* record_i = getRecord(node, i);
                if (i == 0 || recordparser->compare(record_i, record, true) <= 0) {
                    id = node->child[i];
                    delete record_i;
                    break;
                }
                delete record_i;
            }
            delete node;
        } else {
            int pos = -1;
            for (int i = node->header->keyNum - 1; i >= 0; i--) {
                Record* record_i = getRecord(node, i);
                if (recordparser->compare(record_i, record, true) <= 0) {
                    pos = i;
                    delete record_i;
                    break;
                }
                delete record_i;
            }
            if (pos == -1) {
                bufPageManager->access(node->index);
                delete node;
                assert(false);
            }
            node->header->keyNum--;
            for (int i = pos; i < node->header->keyNum; i++) {
                setRecord(node, i, node, i + 1);
            }
            if (node->header->keyNum == 0) {
                int prevLeaf = node->header->prev, nextLeaf = node->header->next;
                if (prevLeaf) {
                    Index_Node* prevNode = getNode(prevLeaf);
                    prevNode->header->next = nextLeaf;
                    bufPageManager->markDirty(prevNode->index);
                    delete prevNode;
                }
                if (nextLeaf) {
                    Index_Node* nextNode = getNode(nextLeaf);
                    nextNode->header->prev = prevLeaf;
                    bufPageManager->markDirty(nextNode->index);
                    delete nextNode;
                }
                while (node->header->keyNum == 0 && id != current_fileHeader.rootPage) {
                    int parent = node->header->parent;
                    Index_Node* parentNode = getNode(parent);
                    int pos = getChildIndex(id, parentNode);
                    parentNode->header->keyNum--;
                    for (int i = pos; i < parentNode->header->keyNum; i++) {
                        parentNode->child[i] = parentNode->child[i + 1];
                        setRecord(parentNode, i, parentNode, i + 1);
                    }
                    bufPageManager->markDirty(node->index);
                    delete node;
                    node = parentNode;
                    id = parent;
                }
                if (id == current_fileHeader.rootPage && node->header->keyNum == 0) {
                    node->header->isLeaf = 1;
                    node->header->next = node->header->prev = 0;
                }
            }
            while (id != current_fileHeader.rootPage && pos == 0) {
                int parent = node->header->parent;
                Index_Node* parentNode = getNode(parent);
                int pos = getChildIndex(id, parentNode);
                setRecord(parentNode, pos, node, 0);
                bufPageManager->markDirty(node->index);
                delete node;
                node = parentNode;
                id = parent;
            }
            bufPageManager->markDirty(node->index);
            delete node;
            break;
        }
    }
    delete record;
}
bool IndexManager::LowerBound(BufType record)
{
    int id = current_fileHeader.rootPage;
    while (1) {
        Index_Node* node = getNode(id);
        bufPageManager->access(node->index);
        if (!node->header->isLeaf) {
            bool flag = false;
            for (int i = node->header->keyNum - 1; i >= 0; i--) {
                Record* record_i = getRecord(node, i);
                if (recordparser->compare(record_i->type[0], record_i->datas[0], record) < 0) {
                    id = node->child[i];
                    flag = true;
                    delete record_i;
                    break;
                }
                delete record_i;
            }
            if (!flag)
                id = node->child[0];
            delete node;
        } else {
            current_node = id;
            current_key = -1;
            for (int i = 0; i < node->header->keyNum; i++) {
                Record* record_i = getRecord(node, i);
                if (recordparser->compare(record_i->type[0], record_i->datas[0], record) >= 0) {
                    current_key = i;
                    delete record_i;
                    break;
                }
                delete record_i;
            }
            if (current_key == -1) {
                current_node = node->header->next;
                current_key = 0;
            }
            delete node;
            if (current_node == 0)
                return false;
            break;
        }
    }
    return true;
}
bool IndexManager::Exists(Record* fullversion, int exclude_rid)
{
    Record* record = truncate(fullversion);
    bool res = ExistsPart(record, exclude_rid);
    delete record;
    return res;
}
bool IndexManager::ExistsPart(Record* partversion, int exclude_rid)
{
    int id = current_fileHeader.rootPage;
    while (1) {
        Index_Node* node = getNode(id);
        bufPageManager->access(node->index);
        if (!node->header->isLeaf) {
            bool flag = false;
            for (int i = node->header->keyNum - 1; i >= 0; i--) {
                Record* record_i = getRecord(node, i);
                if (recordparser->compare(record_i, partversion, false) < 0) {
                    id = node->child[i];
                    flag = true;
                    delete record_i;
                    break;
                }
                delete record_i;
            }
            if (!flag)
                id = node->child[0];
            delete node;
        } else {
            current_node = id;
            current_key = -1;
            for (int i = 0; i < node->header->keyNum; i++) {
                Record* record_i = getRecord(node, i);
                if (recordparser->compare(record_i, partversion, false) >= 0) {
                    current_key = i;
                    delete record_i;
                    break;
                }
                delete record_i;
            }
            if (current_key == -1) {
                current_node = node->header->next;
                current_key = 0;
            }
            delete node;
            if (current_node == 0) {
                return false;
            }
            break;
        }
    }

    while (1) {
        Record* record_current = GetCurrent();
        int res = recordparser->compare(record_current, partversion, false);
        if (res > 0) {
            delete record_current;
            break;
        } else if (res == 0 && record_current->rid != exclude_rid) {
            delete record_current;
            return true;
        } else {
            delete record_current;
            if (!Next())
                break;
        }
    }
    return false;
}
bool IndexManager::CheckNotNull(Record* fullversion)
{
    Record* record = truncate(fullversion);
    bool flag = true;
    for (int i = 0; i < current_fileHeader.col_num; i++) {
        if (record->datas[i] == nullptr) {
            flag = false;
            break;
        }
    }
    delete record;
    return flag;
}
bool IndexManager::UpperBound(BufType record)
{
    int id = current_fileHeader.rootPage;
    while (1) {
        Index_Node* node = getNode(id);
        bufPageManager->access(node->index);
        if (!node->header->isLeaf) {
            bool flag = false;
            for (int i = node->header->keyNum - 1; i >= 0; i--) {
                Record* record_i = getRecord(node, i);
                if (recordparser->compare(record_i->type[0], record_i->datas[0], record) <= 0) {
                    id = node->child[i];
                    flag = true;
                    delete record_i;
                    break;
                }
                delete record_i;
            }
            if (!flag)
                id = node->child[0];
            delete node;
        } else {
            current_node = id;
            current_key = -1;
            for (int i = 0; i < node->header->keyNum; i++) {
                Record* record_i = getRecord(node, i);
                if (recordparser->compare(record_i->type[0], record_i->datas[0], record) > 0) {
                    current_key = i;
                    delete record_i;
                    break;
                }
                delete record_i;
            }
            if (current_key == -1) {
                current_node = node->header->next;
                current_key = 0;
            }
            delete node;
            if (current_node == 0)
                return false;
            break;
        }
    }
    return true;
}
bool IndexManager::Prev()
{
    if (current_node == 0)
        return false;
    Index_Node* node = getNode(current_node);
    bufPageManager->access(node->index);
    if (current_key == 0) {
        if (node->header->prev == 0) {
            delete node;
            return false;
        }
        current_node = node->header->prev;
        delete node;
        node = getNode(current_node);
        bufPageManager->access(current_node);
        current_key = node->header->keyNum - 1;
    } else
        current_key--;
    delete node;
    return true;
}
bool IndexManager::Next()
{
    if (current_node == 0)
        return false;
    Index_Node* node = getNode(current_node);
    bufPageManager->access(node->index);
    if (current_key + 1 == node->header->keyNum) {
        if (node->header->next == 0) {
            delete node;
            return false;
        }
        current_node = node->header->next;
        current_key = 0;
    } else
        current_key++;
    delete node;
    return true;
}
Record* IndexManager::GetCurrent()
{
    Index_Node* node = getNode(current_node);
    Record* record = getRecord(node, current_key);
    delete node;
    return record;
}

void IndexManager::get_rid_gt(BufType data, std::set<int>& rids)
{
    Record* record;
    if (UpperBound(data)) {
        while (1) {
            record = GetCurrent();
            rids.insert(record->rid);
            delete record;
            if (!Next())
                break;
        }
    }
}
void IndexManager::get_rid_lt(BufType data, std::set<int>& rids)
{
    Record* record;
    if (LowerBound(data)) {
        while (Prev()) {
            record = GetCurrent();
            rids.insert(record->rid);
            delete record;
        }
    } else {
        get_rid(rids);
    }
}
void IndexManager::get_rid_gte(BufType data, std::set<int>& rids)
{
    Record* record;
    if (LowerBound(data)) {
        while (1) {
            record = GetCurrent();
            rids.insert(record->rid);
            delete record;
            if (!Next())
                break;
        }
    }
}
void IndexManager::get_rid_lte(BufType data, std::set<int>& rids)
{
    Record* record;
    if (UpperBound(data)) {
        while (Prev()) {
            record = GetCurrent();
            rids.insert(record->rid);
            delete record;
        }
    } else {
        get_rid(rids);
    }
}
void IndexManager::get_rid_eq(BufType data, std::set<int>& rids)
{
    Record* record;
    if (LowerBound(data)) {
        while (1) {
            record = GetCurrent();
            if (recordparser->compare(record->type[0], record->datas[0], data) != 0) {
                delete record;
                break;
            }
            rids.insert(record->rid);
            delete record;
            if (!Next())
                break;
        }
    }
}
void IndexManager::get_rid_neq(BufType data, std::set<int>& rids)
{
    get_rid_lt(data, rids);
    get_rid_gt(data, rids);
}

void IndexManager::get_rid(std::set<int>& rids)
{
    int id = current_fileHeader.rootPage;
    while (1) {
        Index_Node* node = getNode(id);
        bufPageManager->access(node->index);
        if (!node->header->isLeaf) {
            id = node->child[0];
            delete node;
        } else {
            current_node = id;
            current_key = 0;
            if (node->header->keyNum == 0) {
                delete node;
                return;
            }
            break;
        }
    }
    while (1) {
        Record* record = GetCurrent();
        rids.insert(record->rid);
        delete record;
        if (!Next())
            break;
    }
}