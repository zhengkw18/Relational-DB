#include "recordmanager.h"
#include "tablemanager.h"
#include "utils.h"
#include <assert.h>

RecordManager::RecordManager(const char* filename, VarcharManager* varcharmanager)
{
    fileManager = new FileManager();
    bufPageManager = new BufPageManager(fileManager);
    if (!fileManager->openFile(filename, current_fileID))
        assert(false);
    int index0;
    BufType data = bufPageManager->getPage(current_fileID, 0, index0);
    bufPageManager->access(index0);
    memcpy(&current_fileHeader, data, sizeof(Record_FileHeader));
    recordparser = new RecordParser(current_fileHeader.col_type, current_fileHeader.col_length, current_fileHeader.col_num, varcharmanager);
}

RecordManager::~RecordManager()
{
    int index0;
    BufType data = bufPageManager->getPage(current_fileID, 0, index0);
    memcpy(data, &current_fileHeader, sizeof(Record_FileHeader));
    bufPageManager->markDirty(index0);
    bufPageManager->close();
    if (fileManager->closeFile(current_fileID))
        assert(false);
    delete recordparser;
    delete bufPageManager;
    delete fileManager;
}

bool RecordManager::CreateFile(const char* filename, const TableHeader* tableheader)
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
    Record_FileHeader fileheader;
    fileheader.col_num = tableheader->col_num;
    for (int i = 0; i < tableheader->col_num; i++) {
        fileheader.col_type[i] = tableheader->col_type[i];
        fileheader.col_length[i] = tableheader->col_length[i];
    }
    fileheader.recordSize = tableheader->record_len;
    fileheader.recordPerPage = (PAGE_SIZE - sizeof(Record_PageHeader)) * 8 / (fileheader.recordSize * 32 + 1) - 1;
    fileheader.firstFreePage = 0;
    fileheader.pageNum = 1;
    fileheader.bitmapSize = ceil(fileheader.recordPerPage, 32);
    fileheader.bitmapStart = sizeof(Record_PageHeader) / sizeof(int);
    int index;
    BufType buf = bufPageManager->getPage(fileID, 0, index);
    memcpy(buf, &fileheader, sizeof(Record_FileHeader));
    bufPageManager->markDirty(index);
    bufPageManager->close();
    fileManager->closeFile(fileID);
    delete bufPageManager;
    delete fileManager;
    return true;
}
bool RecordManager::DeleteFile(const char* filename)
{
    system(("rm " + std::string(filename)).c_str());
    return true;
}

Record* RecordManager::GetRecord(int rid)
{
    int pageID = rid / current_fileHeader.recordPerPage + 1;
    int slotID = rid % current_fileHeader.recordPerPage;
    if (pageID >= current_fileHeader.pageNum)
        return nullptr;
    int index;
    BufType buf = bufPageManager->getPage(current_fileID, pageID, index);
    bufPageManager->access(index);
    BufType bitmap = buf + current_fileHeader.bitmapStart;
    if (!getBit(bitmap, slotID))
        return nullptr;
    BufType record = bitmap + current_fileHeader.bitmapSize + slotID * current_fileHeader.recordSize;
    return recordparser->toRecord(rid, record);
}
void RecordManager::InsertRecord(int& rid, const Record* data)
{
    int pageID, slotID;
    if (current_fileHeader.firstFreePage == 0) {
        // 新页未开辟
        current_fileHeader.firstFreePage = current_fileHeader.pageNum++;
        int newindex;
        BufType newpage = bufPageManager->getPage(current_fileID, current_fileHeader.firstFreePage, newindex);
        memset(newpage, 0, PAGE_SIZE);
        bufPageManager->markDirty(newindex);
    }
    int index;
    pageID = current_fileHeader.firstFreePage;
    BufType buf = bufPageManager->getPage(current_fileID, pageID, index);
    BufType bitmap = buf + current_fileHeader.bitmapStart;
    slotID = getNextBit0(bitmap, current_fileHeader.recordPerPage, 0);
    BufType record = bitmap + current_fileHeader.bitmapSize + slotID * current_fileHeader.recordSize;
    BufType data_binary = recordparser->toData(data, false);
    memcpy(record, data_binary, current_fileHeader.recordSize * sizeof(uint));
    delete[] data_binary;
    setBit(bitmap, slotID, 1);
    Record_PageHeader* pageheader = (Record_PageHeader*)buf;
    pageheader->recordNum++;
    if (pageheader->recordNum == current_fileHeader.recordPerPage)
        current_fileHeader.firstFreePage = pageheader->nextFreePage;
    bufPageManager->markDirty(index);
    rid = (pageID - 1) * current_fileHeader.recordPerPage + slotID;
}
bool RecordManager::UpdateRecord(int rid, const Record* data)
{
    int pageID = rid / current_fileHeader.recordPerPage + 1;
    int slotID = rid % current_fileHeader.recordPerPage;
    if (pageID >= current_fileHeader.pageNum)
        return false;
    int index;
    BufType buf = bufPageManager->getPage(current_fileID, pageID, index);
    BufType bitmap = buf + current_fileHeader.bitmapStart;
    if (!getBit(bitmap, slotID))
        return false;
    BufType record = bitmap + current_fileHeader.bitmapSize + slotID * current_fileHeader.recordSize;
    recordparser->DeleteVarchar(record);
    BufType data_binary = recordparser->toData(data, false);
    memcpy(record, data_binary, current_fileHeader.recordSize * sizeof(uint));
    delete[] data_binary;
    bufPageManager->markDirty(index);
    return true;
}
bool RecordManager::DeleteRecord(int rid)
{
    int pageID = rid / current_fileHeader.recordPerPage + 1;
    int slotID = rid % current_fileHeader.recordPerPage;
    int index;
    BufType buf = bufPageManager->getPage(current_fileID, pageID, index);
    BufType bitmap = buf + current_fileHeader.bitmapStart;
    if (!getBit(bitmap, slotID))
        return false;
    BufType record = bitmap + current_fileHeader.bitmapSize + slotID * current_fileHeader.recordSize;
    recordparser->DeleteVarchar(record);
    setBit(bitmap, slotID, 0);
    Record_PageHeader* pageheader = (Record_PageHeader*)buf;
    pageheader->recordNum--;
    if (pageheader->recordNum + 1 == current_fileHeader.recordPerPage) {
        pageheader->nextFreePage = current_fileHeader.firstFreePage;
        current_fileHeader.firstFreePage = pageID;
    }
    bufPageManager->markDirty(index);
    return true;
}

bool RecordManager::SearchNext()
{
    while (current_pageID < current_fileHeader.pageNum) {
        int index;
        BufType buf = bufPageManager->getPage(current_fileID, current_pageID, index);
        bufPageManager->access(index);
        BufType bitmap = buf + current_fileHeader.bitmapStart;
        current_slotID = getNextBit1(bitmap, current_fileHeader.recordPerPage, current_slotID);
        if (current_slotID == -1) {
            current_pageID++;
            current_slotID = 0;
        } else {
            return true;
        }
    }
    return false;
}

bool RecordManager::InitializeIteration()
{
    current_pageID = 1;
    current_slotID = 0;
    return SearchNext();
}
Record* RecordManager::CurrentRecord()
{
    int pageID = current_pageID;
    int slotID = current_slotID;
    int rid = (pageID - 1) * current_fileHeader.recordPerPage + slotID;
    return GetRecord(rid);
}
int RecordManager::current_rid()
{
    int pageID = current_pageID;
    int slotID = current_slotID;
    int rid = (pageID - 1) * current_fileHeader.recordPerPage + slotID;
    return rid;
}

bool RecordManager::NextRecord()
{
    if (current_slotID + 1 == current_fileHeader.recordPerPage) {
        current_pageID++;
        current_slotID = 0;
    } else {
        current_slotID++;
    }
    return SearchNext();
}
RecordParser* RecordManager::GetParser()
{
    return recordparser;
}