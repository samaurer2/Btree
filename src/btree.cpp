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
	
	//Open the indexFile of type BlobFile
	if(file->exists(outIndexName)) {
		//open existing Blobfile
		BlobFile blob = BlobFile::open(outIndexName);
		this->file = &blob;
		//Unneccessary?? Are the arguements always correct, or is the metadata correct?
		IndexMetaInfo* meta = (IndexMetaInfo*) &blob;
		this->rootPageNum = meta->rootPageNo;
		this->attrByteOffset = meta->attrByteOffset;
		this->attributeType = meta->attrType;
	} else {
		//open new Blobfile
		file = new BlobFile(outIndexName, true);
		PageId id;
		Page* page;
		
		//IndexMetaInfo page
		bufMgrIn->allocPage(file, id, page);
		IndexMetaInfo* dex = (IndexMetaInfo*) (&page);
		
		//Fill IndexMetaInfo
		dex->attrByteOffset = attrByteOffset;
		dex->attrType = attrType;
		relationName.copy(dex->relationName, 20, 0);


		//Allocate page 2 which should be the root of a new index
		bufMgrIn->allocPage(file, id, page);
		dex->rootPageNo = id;
		
		//insert all records into tree here
		FileScan fscan(relationName, bufMgrIn);
		try {
			RecordId scanRid;
			while(true) {
				fscan.scanNext(scanRid);
				std::string recordStr = fscan.getRecord();
				const char *record = recordStr.c_str();
				int key = *((int*)(record + attrByteOffset));
				std::cout << "PageNo: " << scanRid.page_number << "  SlotNo: "<<scanRid.slot_number << " Key: "<< key <<std::endl;
			}
		}
		catch(const EndOfFileException &e)
		{
			std::cout << "Read all records" << std::endl;
		}

	}

	FileScan fscan(relationName, bufMgr);

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
