#include "varcharmanager.h"
#include <assert.h>
VarcharManager::VarcharManager(const char* filename)
{
    fileManager = new FileManager();
    bufPageManager = new BufPageManager(fileManager);
    if (!fileManager->openFile(filename, current_fileID))
        assert(false);
    int index0;
    BufType data = bufPageManager->getPage(current_fileID, 0, index0);
    bufPageManager->access(index0);
    memcpy(&current_fileHeader, data, sizeof(Varchar_FileHeader));
}
VarcharManager::~VarcharManager()
{
    bufPageManager->close();
    if (fileManager->closeFile(current_fileID))
        assert(false);
    delete bufPageManager;
    delete fileManager;
}
bool VarcharManager::CreateFile(const char* filename)
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
    Varchar_FileHeader fileheader;
    fileheader.firstFreePage = 0;
    fileheader.pageNum = 1;
    resetBitmap(fileheader.bitmap, MAX_VARCHAR_PAGE_NUM);
    int index;
    BufType buf = bufPageManager->getPage(fileID, 0, index);
    memcpy(buf, &fileheader, sizeof(Varchar_FileHeader));
    bufPageManager->markDirty(index);
    bufPageManager->close();
    fileManager->closeFile(fileID);
    delete bufPageManager;
    delete fileManager;
    return true;
}
bool VarcharManager::DeleteFile(const char* filename)
{
    system(("rm " + std::string(filename)).c_str());
    return true;
}
void VarcharManager::AddVarchar(int& pageID, const BufType data, int len)
{
    if (current_fileHeader.firstFreePage == 0) {
        current_fileHeader.firstFreePage = current_fileHeader.pageNum++;
        int index0, newindex;
        BufType page0 = bufPageManager->getPage(current_fileID, 0, index0);
        memcpy(page0, &current_fileHeader, sizeof(Varchar_FileHeader));
        bufPageManager->markDirty(index0);
        BufType newpage = bufPageManager->getPage(current_fileID, current_fileHeader.firstFreePage, newindex);
        Varchar_PageHeader pageheader;
        pageheader.dataLen = 0;
        pageheader.hasNext = 0;
        pageheader.next = 0;
        memcpy(newpage, &pageheader, sizeof(Varchar_PageHeader));
        bufPageManager->markDirty(newindex);
    }
    int index;
    pageID = current_fileHeader.firstFreePage;
    setBit(current_fileHeader.bitmap, pageID, 1);
    current_fileHeader.firstFreePage = getNextBit0(current_fileHeader.bitmap, current_fileHeader.pageNum, pageID + 1);
    if (current_fileHeader.firstFreePage == -1)
        current_fileHeader.firstFreePage = 0;
    int index0;
    BufType page0 = bufPageManager->getPage(current_fileID, 0, index0);
    memcpy(page0, &current_fileHeader, sizeof(Varchar_FileHeader));
    bufPageManager->markDirty(index0);

    BufType buf = bufPageManager->getPage(current_fileID, pageID, index);
    Varchar_PageHeader* pageheader = (Varchar_PageHeader*)buf;
    int len_align = ceil(len, 4);
    pageheader->dataLen = len;
    pageheader->next = 0;
    if (len_align <= VARCHAR_MAX_LEN_PER_PAGE) {
        pageheader->hasNext = 0;
        BufType varchar = buf + sizeof(Varchar_PageHeader) / sizeof(int);
        memcpy(varchar, data, len);
    } else {
        pageheader->hasNext = 1;
        BufType varchar = buf + sizeof(Varchar_PageHeader) / sizeof(int);
        memcpy(varchar, data, 4 * VARCHAR_MAX_LEN_PER_PAGE);
        int nextpageID;
        AddVarchar(nextpageID, data + VARCHAR_MAX_LEN_PER_PAGE, len - 4 * VARCHAR_MAX_LEN_PER_PAGE);
        pageheader->next = nextpageID;
    }
    bufPageManager->markDirty(index);
}
bool VarcharManager::ReadVarchar(int pageID, BufType data, int& len)
{
    if (!getBit(current_fileHeader.bitmap, pageID))
        return false;
    int index;
    BufType buf = bufPageManager->getPage(current_fileID, pageID, index);
    bufPageManager->access(index);
    Varchar_PageHeader* pageheader = (Varchar_PageHeader*)buf;
    len = pageheader->dataLen;
    BufType varchar = buf + sizeof(Varchar_PageHeader) / sizeof(int);
    if (pageheader->hasNext) {
        memcpy(data, varchar, 4 * VARCHAR_MAX_LEN_PER_PAGE);
        int nextlen;
        ReadVarchar(pageheader->next, data + VARCHAR_MAX_LEN_PER_PAGE, nextlen);
    } else {
        memcpy(data, varchar, len);
    }
}
bool VarcharManager::DeleteVarchar(int pageID)
{
    int index;
    setBit(current_fileHeader.bitmap, pageID, 0);
    if (current_fileHeader.firstFreePage > pageID)
        current_fileHeader.firstFreePage = pageID;
    int index0;
    BufType page0 = bufPageManager->getPage(current_fileID, 0, index0);
    memcpy(page0, &current_fileHeader, sizeof(Varchar_FileHeader));
    bufPageManager->markDirty(index0);

    BufType buf = bufPageManager->getPage(current_fileID, pageID, index);
    Varchar_PageHeader* pageheader = (Varchar_PageHeader*)buf;
    if (pageheader->hasNext) {
        DeleteVarchar(pageheader->next);
    }
    pageheader->dataLen = 0;
    pageheader->hasNext = 0;
    pageheader->next = 0;
    bufPageManager->markDirty(index);
}