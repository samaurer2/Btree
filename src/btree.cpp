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
		struct IndexMetaInfo* dex = (struct IndexMetaInfo*) (page);
		//initialize dex fields and btree variables
		dex->attrByteOffset = attrByteOffset;
		dex->attrType = attrType;
		dex->rootPageNo = Page::INVALID_NUMBER;
		this->rootPageNum = dex->rootPageNo;
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

void BTreeIndex::insertLeaf(const void *key, const RecordId rid, PageId pid) {
	
	Page* currPage;
	bufMgr->readPage(file, pid, currPage);
	struct LeafNodeInt* node = (struct LeafNodeInt*)(currPage);
	
	if/*not full*/(node->ridArray[leafOccupancy-1].page_number == Page::INVALID_NUMBER)
	{
		int tempKey = *((int*)key);
		RecordId tempRid = rid;
		std::cout<< tempKey <<" "<< tempRid.page_number<< " "<< tempRid.slot_number <<std::endl;
		for(int i = 0; i < leafOccupancy; i++) {
			if (node->ridArray[i].page_number == Page::INVALID_NUMBER) 				{
				node->keyArray[i] = tempKey;
				node->ridArray[i] = tempRid;
				return;
				}
			else if (node->keyArray[i] < tempKey)
			{
				continue;
			}
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
		/* code */
	}
	
	


}
void BTreeIndex::insertInternal(const void *key, const RecordId rid, PageId pid) {
}
void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{	
	PageId pid;
	Page* currPage;
	
	if(rootPageNum == Page::INVALID_NUMBER) {
		bufMgr->allocPage(file, pid, currPage);
		rootPageNum = pid;
		insertLeaf(key, rid, pid);
		return;
	}
	
	bufMgr->readPage(file, rootPageNum, currPage);
	pid = rootPageNum;
	
	struct NonLeafNodeInt* node = (struct NonLeafNodeInt*)(currPage);
	if/*leaf*/ (node->level != -1)
	{
		insertLeaf(key, rid, pid);
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
