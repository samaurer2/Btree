/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	//construct the outIndexName
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	outIndexName  = idxStr.str();
	
	//Initialize BTreeIndex members
	this->bufMgr = bufMgrIn;
	this->attributeType = attrType;
	this->attrByteOffset = attrByteOffset;
	
	PageId id;
	Page* page;
	
	//Open the indexFile of type BlobFile
	if(file->exists(outIndexName)) {
		
		//open existing Blobfile
		BlobFile blob = BlobFile::open(outIndexName);
		this->file = &blob;
		//Unneccessary?? Are the arguements always correct, or is the metadata correct?
		struct IndexMetaInfo* meta = (struct IndexMetaInfo*) &blob;
		this->rootPageNum = meta->rootPageNo;
		this->attrByteOffset = meta->attrByteOffset;
		this->attributeType = meta->attrType;
		
		//root page read into buffer manager
		bufMgr->readPage(file, file->getFirstPageNo(), page);
	} else {
		
		//open new Blobfile
		file = new BlobFile(outIndexName, true);		
		
		//IndexMetaInfo page
		bufMgrIn->allocPage(file, id, page);
		this->headerPageNum = id;
		struct IndexMetaInfo* dex = (struct IndexMetaInfo*) (page);
		//initialize dex fields and btree variables
		dex->attrByteOffset = attrByteOffset;
		dex->attrType = attrType;
		dex->rootPageNo = Page::INVALID_NUMBER;
		this->rootPageNum = Page::INVALID_NUMBER;
		this->attrByteOffset = dex->attrByteOffset;
		this->attrByteOffset = dex->attrType;
		relationName.copy(dex->relationName, 20, 0);

		//insert all records into tree here
		FileScan fscan(relationName, bufMgrIn);
		try {
			RecordId scanRid;
			while(true) {
				fscan.scanNext(scanRid);
				std::string recordStr = fscan.getRecord();
				const char *record = recordStr.c_str();
				int key = *((int*)(record + attrByteOffset));
				insertEntry(&key, scanRid);
				dex->rootPageNo = rootPageNum;	
			}
		}
		catch(const EndOfFileException &e)
		{
			std::cout << "Read all records" << std::endl;
		}

	}

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

PageKeyPair<PageId> BTreeIndex::insertLeaf(const void *key, const RecordId rid, PageId pid) {
	

	 Page* currPage;
	 bufMgr->readPage(file, pid, currPage);
	 struct LeafNodeInt* node = (struct LeafNodeInt*)(currPage);

	 
	 if /*not full*/(node->ridArray[INTARRAYLEAFSIZE-1].page_number == Page::INVALID_NUMBER)
	 {
		int tempKey = *((int*)key);
		RecordId tempRid = rid;
		//empty slot, insert return
		std::cout<<"not full"<<std::endl;
		for(int i = 0; i < INTARRAYLEAFSIZE; i++) {
			if (node->ridArray[i].page_number == Page::INVALID_NUMBER) {
				node->keyArray[i] = tempKey;
				node->ridArray[i] = tempRid;
				PageKeyPair<PageId> pair;
				pair.set(-1, Page::INVALID_NUMBER);
				bufMgr->unPinPage(file, pid, true);
				return pair;
				}
			//non-empy slot, keep searching
			else if (node->keyArray[i] < tempKey)
			{
				continue;
			}
			//non-empty slot, replace and shift
			else if (node->keyArray[i] > tempKey)
			{
				int tempKey2= node->keyArray[i];
				RecordId tempRid2 = node->ridArray[i];
				node->keyArray[i] = tempKey;
				node->ridArray[i] = tempRid;
				tempKey = tempKey2;
				tempRid = tempRid2;
			}
		}
	}
	else /*full*/
	{
		Page* low = currPage;
		Page* high;
		PageId highId;
		std::cout<<"full"<<std::endl;
		//alloc and initialized new page
		bufMgr->allocPage(file, highId, high);
		struct LeafNodeInt* highNode = (struct LeafNodeInt*)(high);
		for (size_t i = 0; i < INTARRAYLEAFSIZE; i++)
		{	
			highNode->ridArray[i].page_number = Page::INVALID_NUMBER;
			highNode->ridArray[i].slot_number = Page::INVALID_SLOT;
		}
		//find median and split the array into low and high
		int median = node->keyArray[INTARRAYLEAFSIZE/2];
		for (size_t i = 0, j = 0; i < INTARRAYLEAFSIZE; i++)
		{	
			//below median value do nothing
			if(node->keyArray[i] < median) 
			{
				continue;
			}
			//higher than median, copy value into high node array
			//then set low node value to default value
			else 
			{
				highNode->keyArray[j] = node->keyArray[i];
				highNode->ridArray[j] = node->ridArray[i];
				node->ridArray[i].page_number = Page::INVALID_NUMBER;
				node->ridArray[i].slot_number = Page::INVALID_SLOT;
				node->keyArray[i] = 0;
				j++;
			}
		}
		//set sibling pointer
		node->rightSibPageNo = highId;

		//insert key value into appropriate array, low or high
		if /*insert low*/(*((int*)key) < median)
		{
			int tempKey = *((int*)key);
			RecordId tempRid = rid;
			//empty slot, insert return
			for(int i = 0; i < INTARRAYLEAFSIZE; i++) {
				if (node->ridArray[i].page_number == Page::INVALID_NUMBER) {
					node->keyArray[i] = tempKey;
					node->ridArray[i] = tempRid;
					break;
					}
				//non-empy slot, keep searching
				else if (node->keyArray[i] < tempKey)
				{
					continue;
				}
				//non-empty slot, replace and shift
				else if (node->keyArray[i] > tempKey)
				{
					int tempKey2= node->keyArray[i];
					RecordId tempRid2 = node->ridArray[i];
					node->keyArray[i] = tempKey;
					node->ridArray[i] = tempRid;
					tempKey = tempKey2;
					tempRid = tempRid2;
				}
			}
		}
		else /*insert high*/
		{
			int tempKey = *((int*)key);
			RecordId tempRid = rid;
			//empty slot, insert return
			for(int i = 0; i < INTARRAYLEAFSIZE; i++) {
				if (highNode->ridArray[i].page_number == Page::INVALID_NUMBER) {
					highNode->keyArray[i] = tempKey;
					highNode->ridArray[i] = tempRid;
					break;
				}
				//non-empy slot, keep searching
				else if (highNode->keyArray[i] < tempKey)
				{
					continue;
				}
				//non-empty slot, replace and shift
				else if (highNode->keyArray[i] > tempKey)
				{
					int tempKey2= node->keyArray[i];
					RecordId tempRid2 = node->ridArray[i];
					highNode->keyArray[i] = tempKey;
					highNode->ridArray[i] = tempRid;
					tempKey = tempKey2;
					tempRid = tempRid2;
				}
			}
		}
		
		
		
		PageKeyPair<PageId> pair;
		pair.set(median, highId);
		bufMgr->unPinPage(file, pid, true);
		bufMgr->unPinPage(file, highId, true);
		return pair;		
	}
}

PageKeyPair<PageId> BTreeIndex::insertInternal(const void *key, const RecordId rid, PageId pid) 
{
	Page* currPage;
	bufMgr->readPage(file, pid, currPage);
	
	struct NonLeafNodeInt* node = (struct NonLeafNodeInt*)(currPage);
	PageKeyPair<PageId> pair;

	if /*non-leaf*/(node->level == 1) 
	{
		pair = insertInternal(key, rid, pid);
	}
	else /*leaf*/
	{
		pair = insertLeaf(key, rid, pid);
	}
	
	return pair;
}

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{	
	PageId id;
	Page* currPage;
	
	if (rootPageNum == Page::INVALID_NUMBER)
	{
		bufMgr->allocPage(file, id, currPage);
		rootPageNum = id;
		struct LeafNodeInt* node =(struct LeafNodeInt*)(currPage);
		for (size_t i = 0; i < INTARRAYLEAFSIZE; i++)
		{	
			node->ridArray[i].page_number = Page::INVALID_NUMBER;
			node->ridArray[i].slot_number = Page::INVALID_SLOT;
		}

	}
	
	PageKeyPair<PageId> pair = insertInternal(key, rid, rootPageNum);
	if (pair.pageNo != Page::INVALID_NUMBER)
	{

	}
	
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{

}

}
