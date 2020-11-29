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
		bufMgr->readPage(file, id, page);
	} else {
		
		//open new Blobfile
		file = new BlobFile(outIndexName, true);		
		
		//IndexMetaInfo page
		bufMgrIn->allocPage(file, id, page);
		
		struct IndexMetaInfo* dex = (struct IndexMetaInfo*) (page);
		dex->attrByteOffset = attrByteOffset;
		dex->attrType = attrType;
		relationName.copy(dex->relationName, 20, 0);
		


		//Allocate page 2 which should be the root of a new index
		bufMgrIn->allocPage(file, id, page);
		dex->rootPageNo = id;
		
		//some default values for new root page which is a leaf node
		struct LeafNodeInt *node = (struct LeafNodeInt *)(page);
		for(int i = 0; i < leafOccupancy; ++i) {
			node->keyArray[i] = 0;
			node->ridArray[i].page_number = Page::INVALID_NUMBER;
			node->ridArray[i].slot_number = Page::INVALID_SLOT;
		}
		node->rightSibPageNo = Page::INVALID_NUMBER;
		node->ridArray[leafOccupancy-1].page_number = Page::INVALID_NUMBER;
		
		//insert all records into tree here
		FileScan fscan(relationName, bufMgrIn);
		try {
			RecordId scanRid;
			while(true) {
				fscan.scanNext(scanRid);
				std::string recordStr = fscan.getRecord();
				const char *record = recordStr.c_str();
				int key = *((int*)(record + attrByteOffset));
				//std::cout << "PageNo: " << scanRid.page_number << " SlotNo: "<<scanRid.slot_number << " Key: "<< key <<std::endl;
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

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	PageId pid = rootPageNum;
	Page* currPage;
	bufMgr->readPage(file, pid, currPage);
	struct NonLeafNodeInt *node = (struct NonLeafNodeInt *)(currPage);
	
	//leaf-node
	if(node->level != -1) {
		struct LeafNodeInt *node = (struct LeafNodeInt *)(currPage);
		
		//check is array is full
		//not full
		if (node->ridArray[leafOccupancy-1].page_number == Page::INVALID_NUMBER){
			std::cout << "not full" << std::endl;
			//insert key/rid pair into arrays in a sorted manner
			int tempKey = *((int*)key);
			RecordId tempRid = rid;
			for(int i = 0; i < leafOccupancy; i++) {
				if (node->keyArray[i] == 0) 
				{
					std::cout<< tempRid.page_number<< " "<< tempRid.slot_number <<std::endl;
					node->keyArray[i] = tempKey;
					node->ridArray[i] = tempRid;
					break;
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
		
		//full
		else  {
			std::cout << "full" << std::endl;
		}
	}
	//non-leaf node
	else {
		node = (struct NonLeafNodeInt *)(currPage);
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
