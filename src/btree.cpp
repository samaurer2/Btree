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
		std::cout << "Alloc Page IndexMetaInfo: " << id<<std::endl;
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
			std::cout << "End Constructor" << std::endl;
		}

	}

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
  if(scanExecuting){
    endScan();
  }
  bufMgr->unPinPage(file, currentPageNum, false);

  bufMgr->flushFile(file);
  delete file;


}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

PageKeyPair<int> BTreeIndex::insertLeaf(const void *key, const RecordId rid, PageId pid) {
	
	Page* currPage;
	bufMgr->readPage(file, pid, currPage);
 	struct LeafNodeInt* node = (struct LeafNodeInt*)(currPage);
	PageKeyPair<int> pair;


	//leaf start out not full	
	int tempKey = *((int*)key);
	RecordId tempRid = rid;
	
	//search array for appropriate slot to insert
	for(int i = 0; i < INTARRAYLEAFSIZE; i++) {
		
		//empty slot, insert return
		if (node->ridArray[i].page_number == Page::INVALID_NUMBER) 
		{
			node->keyArray[i] = tempKey;
			node->ridArray[i] = tempRid;
			break;
		}
			
		//non-empy slot, keep searching
		else if (node->keyArray[i] < tempKey)
			continue;
		
		//non-empty slot, replace and shift
		else if (node->keyArray[i] > tempKey)
		{
			int tempKey2 = node->keyArray[i];
			RecordId tempRid2 = node->ridArray[i];
			node->keyArray[i] = tempKey;
			node->ridArray[i] = tempRid;
			tempKey = tempKey2;
			tempRid = tempRid2;
		}
	}
	//check if after the insert if the leaf is full or not
	if /*full*/(node->ridArray[INTARRAYLEAFSIZE-1].page_number != Page::INVALID_NUMBER)
	{
		//immediately perform split
		Page* high;
		PageId highId;
		std::cout<<"Splitting leaf: "<< pid<<std::endl;
	
		//alloc and initialized new page
		bufMgr->allocPage(file, highId, high);
		struct LeafNodeInt* highNode = (struct LeafNodeInt*)(high);
		highNode->type = LEAF;
		std::cout<<"New leaf: "<< highId<<std::endl;
	
		//set sibling pointer
		highNode->rightSibPageNo = node->rightSibPageNo;
		node->rightSibPageNo = highId;
		
		//default record values
		for (size_t i = 0; i < INTARRAYLEAFSIZE; i++)
		{	
			highNode->ridArray[i].page_number = Page::INVALID_NUMBER;
			highNode->ridArray[i].slot_number = Page::INVALID_SLOT;
		}
	
		//find median and split the array into low and high
		int median = node->keyArray[INTARRAYLEAFSIZE/2];
		
		//loop over the full array, copying high values into new node
		//and setting the higher low values to default after copying to new node
		for (size_t i = 0, j = 0; i < INTARRAYLEAFSIZE; i++)
		{	
			//below or equal median value do nothing
			if(node->keyArray[i] < median) 
			{
				continue;
			}
			//higher than median, copy value into high node array
			//then set low node value to default value
			else if(node->keyArray[i] >= median)
			{
				highNode->keyArray[j] = node->keyArray[i];
				highNode->ridArray[j] = node->ridArray[i];
				node->ridArray[i].page_number = Page::INVALID_NUMBER;
				node->ridArray[i].slot_number = Page::INVALID_SLOT;
				node->keyArray[i] = 0;
				j++;
			}
		}
		
		pair.set(highId, median);
		bufMgr->unPinPage(file,pid,true);
		bufMgr->unPinPage(file,highId,true);
		int i = 0;
		while(node->ridArray[i].page_number != Page::INVALID_NUMBER)
		{
			i++;
		}
		int j = 0;
		while(highNode->ridArray[j].page_number != Page::INVALID_NUMBER)
		{
			j++;
		}
		std::cout<<"pageNo: "<<pid<<" low values: "<<node->keyArray[0]<<" - "<<node->keyArray[i-1] <<" median: " <<median<<" NewPid: "<<highId<<" high values: "<< highNode->keyArray[0]<<" - "<<highNode->keyArray[j-1]<<std::endl;
		return pair;			
	}
	else/* not full*/
	{
		//return
		pair.set(Page::INVALID_NUMBER, -1);
		bufMgr->unPinPage(file, pid, true);
		return pair;
	}	
}

PageKeyPair<int> BTreeIndex::insertNonLeaf(const void *key, const RecordId rid, PageId pid) 
{
	Page* currPage;
	bufMgr->readPage(file, pid, currPage);
	struct NonLeafNodeInt* node = (struct NonLeafNodeInt*)(currPage);
	PageKeyPair<int> pair;
	
	//determine if we are actually in a leaf or nonleaf node
	if (node->type == LEAF)
	{
		pair  = insertLeaf(key, rid, pid);
		return pair;
	}
	else
	{	
		//search internal array for node
		for (size_t i = 0; i < INTARRAYNONLEAFSIZE; i++)
		{
			
			//find page to enter
			if (i == INTARRAYNONLEAFSIZE || node->pageNoArray[i+1] == Page::INVALID_NUMBER ||
				node->keyArray[i] > *((int*)key))
			{
				//if page would be a leaf node
				if (node->level == 1)
				{
					pair = insertLeaf(key, rid, node->pageNoArray[i]);
					break;
				}
				//recurse to another non leaf node
				else
				{
					pair = insertNonLeaf(key, rid, node->pageNoArray[i]);
					break;
				}				
			}
		}		
	}
	
	//check if low level split occured
	if /*no split*/(pair.pageNo == Page::INVALID_NUMBER)
	{
		return pair;
	}
	else/*split occured*/
	{
		int tempKey = pair.key;
		PageId tempPid = pair.pageNo;
		std::cout<<" Page: "<< tempPid<<" Insert Key: "<<tempKey<<std::endl;
		for (size_t i = 0; i < INTARRAYNONLEAFSIZE; i++)
		{
			//empty slot, insert return
			if (node->keyArray[i] == 0)
			{
				node->keyArray[i] = tempKey;
				node->pageNoArray[i+1] = tempPid;
				pair.set(Page::INVALID_NUMBER, -1);
				bufMgr->unPinPage(file,pid,true);
				// std::cout<<"Non leaf array"<<std::endl;
				// for (size_t i = 0; i < 16; i++)
				// {
				// 	std::cout<<" PageNo: "<<node->pageNoArray[i]<<" Key: "<<node->keyArray[i]<<std::endl;
				// }
				return pair;
			}
			//non-empty keep searching
			else if (node->keyArray[i] < tempKey)
				continue;
			
			//non-empty, insert and shift
			else if(node->keyArray[i] > tempKey)
			{
				int tempKey2 = node->keyArray[i];
				PageId tempPid2 = node->pageNoArray[i+1];
				node->keyArray[i]= tempKey;
				node->pageNoArray[i+1]=tempPid;
				tempKey = tempKey2;
				tempPid = tempPid2;
			}
			
		}
			
		if/*full, split again*/(node->pageNoArray[INTARRAYNONLEAFSIZE] != Page::INVALID_NUMBER)
		{
			Page* newPage;
			PageId newId;
			bufMgr->allocPage(file, newId, newPage);

			struct NonLeafNodeInt* newNode = (struct NonLeafNodeInt*)(newPage);
			for (size_t i = 0; i < INTARRAYNONLEAFSIZE; i++)
			{
				newNode->pageNoArray[i] = Page::INVALID_NUMBER;
			}
			
			int medianIndex = INTARRAYNONLEAFSIZE/2;
			int pushUpValue = node->keyArray[medianIndex];
			node->keyArray[medianIndex] = 0;
			
			for (size_t i = medianIndex+1, j = 0; i < INTARRAYNONLEAFSIZE; i++)
			{
				newNode->keyArray[j] = node->keyArray[i];
				newNode->pageNoArray[j]= node->keyArray[i];
				node->keyArray[i] = 0;
				node->pageNoArray[i] = Page::INVALID_NUMBER;
				++j;
			}
			newNode->level = node->level;
			pair.set(newId, pushUpValue);
			bufMgr->unPinPage(file, pid, true);
			bufMgr->unPinPage(file, newId, true);
			return pair;
		}
		return pair;
	}
}
void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{	
	PageId id;
	Page* root;

	if/*No root*/ (rootPageNum == Page::INVALID_NUMBER)
	{
		bufMgr->allocPage(file, id, root);
		std::cout << "Alloc Page New Root: " << id <<std::endl;
		rootPageNum = id;
		struct LeafNodeInt* node =(struct LeafNodeInt*)(root);
		node->type = LEAF;

		for (size_t i = 0; i < INTARRAYLEAFSIZE; i++)
		{	
			node->ridArray[i].page_number = Page::INVALID_NUMBER;
			node->ridArray[i].slot_number = Page::INVALID_SLOT;
		}
	}
	PageKeyPair<int> pair;
	pair = insertNonLeaf(key, rid, rootPageNum);
	
	if/*root splits*/ (pair.pageNo != Page::INVALID_NUMBER)
	{	
		std::cout<<"Splitting root: "<<rootPageNum<<std::endl;
		Page* newRoot;
		PageId newRootId;
		bufMgr->readPage(file, rootPageNum, root);
		bufMgr->allocPage(file, newRootId, newRoot);
		struct NonLeafNodeInt* rootNode = (struct NonLeafNodeInt*)(root);		
		struct NonLeafNodeInt* newRootnode = (struct NonLeafNodeInt*)(newRoot);

		if (rootNode->type == LEAF)
		{
			newRootnode->level=1;
			newRootnode->type = NONLEAF;
		}
		else
		{
			newRootnode->level = 0;
			newRootnode->type = NONLEAF;
		}
		
		
		for (size_t i = 0; i < INTARRAYNONLEAFSIZE; i++)
		{
			newRootnode->pageNoArray[i] = Page::INVALID_NUMBER;
			newRootnode->pageNoArray[i] = 0;
		}
		newRootnode->pageNoArray[INTARRAYNONLEAFSIZE] = Page::INVALID_NUMBER;

		newRootnode->keyArray[0] = pair.key;
		newRootnode->pageNoArray[0] = rootPageNum;
		newRootnode->pageNoArray[1]= pair.pageNo;
		rootPageNum = newRootId;
		std::cout<<"PageNo: "<<newRootnode->pageNoArray[0]<<" Key: "<< newRootnode->keyArray[0]<<std::endl;
		std::cout<<"PageNo: "<<newRootnode->pageNoArray[1]<<" Key: "<< newRootnode->keyArray[1]<<std::endl;
		bufMgr->unPinPage(file, newRootnode->pageNoArray[0], true);
		std::cout<<"New Root: "<<rootPageNum<<std::endl;
		Page* p;
		bufMgr->readPage(file,newRootnode->pageNoArray[0],p);
		struct LeafNodeInt* n = (struct LeafNodeInt*)(p);
		std::cout<<"rightSibPageNo: "<< n->rightSibPageNo<<std::endl;
		bufMgr->unPinPage(file, newRootnode->pageNoArray[0], false);	
	}
	
}
/*
*	sets BTreeIndex::currentPageNum to the page in the realation containing low op value
*/
void BTreeIndex::findPage(PageId pid)
{
	Page* currPage;
	bufMgr->readPage(file, pid, currPage);
	struct NonLeafNodeInt* node = (struct NonLeafNodeInt*)(currPage);
	currentPageNum = pid;

	for (size_t i = 0; i < INTARRAYNONLEAFSIZE; i++)
	{
		if (i == INTARRAYNONLEAFSIZE || node->pageNoArray[i+1] == Page::INVALID_NUMBER || node->keyArray[i] >= lowValInt)
		{
			if (node->level == 1)
			{			
				currentPageNum = node->pageNoArray[i];
				bufMgr->unPinPage(file, pid, false);
				return;
			}
			else
			{
				findPage(node->pageNoArray[i]);
				bufMgr->unPinPage(file, pid, false);
				return;
			}	
			
		}
		
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
	lowOp = lowOpParm;
	highOp = highOpParm;
	lowValInt = *((int*)lowValParm);
	highValInt = *((int*)highValParm);
	//To Do: error checking the opcode/params

	if (rootPageNum != 2)
		findPage(rootPageNum);
	
	scanExecuting = true;
	bufMgr->readPage(file, currentPageNum, currentPageData);

	struct LeafNodeInt* node =(struct LeafNodeInt*)(currentPageData);

	for (size_t i = 0; i < INTARRAYLEAFSIZE; i++)
	{
		if ((lowOp == GTE) && (node->keyArray[i] >= lowValInt))
		{
			nextEntry = i;
			return;
		}
		else if ((lowOp == GT) && (node->keyArray[i] > lowValInt))
		{
			nextEntry = i;
			return;
		}	
	}	
	throw NoSuchKeyFoundException();
	 


}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
	//error checking if called when scan not started
	if (scanExecuting)
	{
		struct LeafNodeInt* node = (struct LeafNodeInt*)(currentPageData);
		if (node->ridArray[nextEntry].page_number == Page::INVALID_NUMBER)
		{
			nextEntry = INTARRAYLEAFSIZE;
		}
		

		if (nextEntry == INTARRAYLEAFSIZE)
		{
			bufMgr->unPinPage(file, currentPageNum, false);
			if (node->rightSibPageNo != Page::INVALID_NUMBER)
			{
				bufMgr->readPage(file,node->rightSibPageNo, currentPageData);
				currentPageNum = node->rightSibPageNo;
				nextEntry = 0;				
			}
			else
			{
				bufMgr->unPinPage(file,currentPageNum,false);
				endScan();
			}				
		}
		if (((highOp == LTE && (node->keyArray[nextEntry] <= highValInt)))
		|| (highOp == LT && (node->keyArray[nextEntry] < highValInt)))
		{
			outRid = node->ridArray[nextEntry];
			nextEntry++;
		}
		else
		{
			throw IndexScanCompletedException();
		}
	}
	else
		throw ScanNotInitializedException();
	
	
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
	if (!scanExecuting)
		throw ScanNotInitializedException();
	scanExecuting = false;
	currentPageData = NULL;
	currentPageNum = Page::INVALID_NUMBER;
}

}
