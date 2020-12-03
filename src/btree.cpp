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
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

PageKeyPair<int> BTreeIndex::insertLeaf(const void *key, const RecordId rid, PageId pid) {
	

	Page* currPage;
	bufMgr->readPage(file, pid, currPage);
	struct LeafNodeInt* node = (struct LeafNodeInt*)(currPage);
	 
	 if /*not full*/(node->ridArray[INTARRAYLEAFSIZE-1].page_number == Page::INVALID_NUMBER)
	 {
		int tempKey = *((int*)key);
		RecordId tempRid = rid;
		
		//search array for appropriate slot to insert
		for(int i = 0; i < INTARRAYLEAFSIZE; i++) {
			
			//empty slot, insert return
			if (node->ridArray[i].page_number == Page::INVALID_NUMBER) 
			{
				node->keyArray[i] = tempKey;
				node->ridArray[i] = tempRid;
				
				//no new pageNo, no median key to copy up
				PageKeyPair<int> pair;
				pair.set(Page::INVALID_NUMBER, -1);
				
				bufMgr->unPinPage(file, pid, true);
				return pair;
			}
			
			//non-empy slot, keep searching
			else if (node->keyArray[i] < tempKey)
				continue;
			
			//non-empty slot, replace and shift
			else
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
		std::cout<<"Leaf full. Pid: "<<pid<<std::endl;
		//alloc and initialized new page
		bufMgr->allocPage(file, highId, high);
		std::cout << "Alloc Page Split Leaf: " <<highId<<std::endl;
		struct LeafNodeInt* highNode = (struct LeafNodeInt*)(high);
		
		//set sibling pointer
		std::cout << "Old Sibling: " <<node->rightSibPageNo<<std::endl;
		highNode->rightSibPageNo = node->rightSibPageNo;
		node->rightSibPageNo = highId;
		std::cout << "New Sibling: " <<node->rightSibPageNo<<std::endl;
		
		for (size_t i = 0; i < INTARRAYLEAFSIZE; i++)
		{	
			highNode->ridArray[i].page_number = Page::INVALID_NUMBER;
			highNode->ridArray[i].slot_number = Page::INVALID_SLOT;
		}
		//find median and split the array into low and high
		int median = node->keyArray[INTARRAYLEAFSIZE/2];
		std::cout<<"Median: "<<median<<std::endl;
		for (size_t i = 0, j = 0; i < INTARRAYLEAFSIZE; i++)
		{	
			//below or equal median value do nothing
			if(node->keyArray[i] <= median) 
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
		// for(int i = 0; i < INTARRAYLEAFSIZE; ++i)
		// {
		// 	if(node->keyArray[i] != 0 && highNode->keyArray[i] != 0 )
		// 	{
		// 	std::cout<<"low: "<< node->keyArray[i]<<" high: "<< highNode->keyArray[i]<<std::endl;
		// 	}
		// }

		//insert key value into appropriate array, low or high
		if /*insert low*/(*((int*)key) < median)
		{
			int tempKey = *((int*)key);
			RecordId tempRid = rid;
			
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
			
			for(int i = 0; i < INTARRAYLEAFSIZE; i++) {
				
				//empty slot, insert return
				if (highNode->ridArray[i].page_number == Page::INVALID_NUMBER) 
				{
					highNode->keyArray[i] = tempKey;
					highNode->ridArray[i] = tempRid;
					break;
				}
				//non-empy slot, keep searching
				else if (highNode->keyArray[i] < tempKey)
					continue;
				
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
		
		//return PageNo of new page along with median value
		PageKeyPair<int> pair;
		pair.set(highId, median);
		int i = 0;
		while(node->keyArray[i]!=0)
		{
			i++;
		}
		//std::cout<<"highest low value: "<<node->keyArray[i-1] <<" median: " <<median<<"lowest high value: "<< highNode->keyArray[0]<<std::endl;
		//unpin pages
		bufMgr->unPinPage(file, pid, true);
		bufMgr->unPinPage(file, highId, true);
		std::cout<<"rightSibPageNo return: "<<node->rightSibPageNo<<std::endl;
		return pair;		
	}
}

PageKeyPair<int> BTreeIndex::insertInternal(const void *key, const RecordId rid, PageId pid) 
{
	Page* currPage;
	bufMgr->readPage(file, pid, currPage);

	struct NonLeafNodeInt* node = (struct NonLeafNodeInt*)(currPage);
	PageKeyPair<int> pair;
	
	for (size_t i = 0; i < INTARRAYNONLEAFSIZE; i++)
	{
		if (i == INTARRAYNONLEAFSIZE || node->pageNoArray[i+1] == Page::INVALID_NUMBER || node->keyArray[i] >= *((int*)key))
		{
			//std::cout<<node->keyArray[i]<< " "<< *((int*)key)<<std::endl;
			if (node->level == 1)
			{			
				pair = insertLeaf(key, rid, node->pageNoArray[i]);
				break;	
			}
			else
			{
				if (node->pageNoArray[i] != 0)
					std::cout<<"Search internal page: "<<node->pageNoArray[i]<<std::endl;
				pair = insertInternal(key, rid,node->pageNoArray[i]);
				break;
				
			}	
			
		}
		
	}
	//check if split occured
	if /*no split*/(pair.pageNo == Page::INVALID_NUMBER)
	{
		bufMgr->unPinPage(file, pid, false);
		return pair;
	}
	else/*split occured*/
	{
		//attempt to insert to non leaf node
		if /*not full*/(node->pageNoArray[INTARRAYNONLEAFSIZE] == Page::INVALID_NUMBER)
		{
			int tempKey = pair.key;
			PageId tempPid = pair.pageNo;
			std::cout<<"inserting pageNo: "<<tempPid<<" key: "<<tempKey<<std::endl;
			for (size_t i = 0; i < INTARRAYNONLEAFSIZE; i++)
			{
				// empty slot, insert and return
				if (node->keyArray[i] == 0)
				{
					node->keyArray[i] = tempKey;
					node->pageNoArray[i+1] = tempPid;
					pair.set(Page::INVALID_NUMBER, -1);
					std::cout<<"search page: "<< pid<<std::endl;
					for (size_t i = 0; i < 16; i++)
						{
							std::cout<<"pageNo: "<<node->pageNoArray[i]<<" key: "<<node->keyArray[i]<<std::endl;
						}
	
					bufMgr->unPinPage(file, pid, true);
					return pair;
				}
				//non empty slot, keep searching
				else if (node->keyArray[i] < tempKey)
					continue;
				
				//non empty slot, insert and shift
				else if (node->keyArray[i] > tempKey)
				{
					int tempKey2 = node->keyArray[i];
					PageId tempPid2 = node->pageNoArray[i];
					node->keyArray[i]= tempKey;
					node->pageNoArray[i]=tempPid;
					tempKey = tempKey2;
					tempPid = tempPid2;
				}				
				
			}
			
		}

		else/*full, need to split internal node*/
		{
			Page* high;
			PageId highId;

			bufMgr->allocPage(file, highId, high);
			struct NonLeafNodeInt* highNode =(struct NonLeafNodeInt*)(high); 
		}
		
		
	}	
}

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{	
	PageId id;
	Page* currPage;

	if/*No root*/ (rootPageNum == Page::INVALID_NUMBER)
	{
		bufMgr->allocPage(file, id, currPage);
		std::cout << "Alloc Page New Root: " << id <<std::endl;
		rootPageNum = id;
		struct LeafNodeInt* node =(struct LeafNodeInt*)(currPage);
		for (size_t i = 0; i < INTARRAYLEAFSIZE; i++)
		{	
			node->ridArray[i].page_number = Page::INVALID_NUMBER;
			node->ridArray[i].slot_number = Page::INVALID_SLOT;
		}

	}
	PageKeyPair<int> pair;
	if (rootPageNum == 2)
	{
		pair = insertLeaf(key, rid, rootPageNum);	
	}
	else 
	{
		pair = insertInternal(key, rid, rootPageNum);
	}
	

	if/*root splits*/ (pair.pageNo != Page::INVALID_NUMBER)
	{
		PageId newRootId;
		Page* newRoot;
		//alloc and initialize new non-leaf node
		bufMgr->allocPage(file, newRootId, newRoot);
		struct NonLeafNodeInt* newRootNode = (struct NonLeafNodeInt*)(newRoot);		
		if/*root was originally a leaf*/(rootPageNum == 2)
		{
			
		}
		else/*root was an internal node*/ 
		{

		}
		for (size_t i = 0; i < INTARRAYNONLEAFSIZE+1; i++)
		{
			newRootNode->pageNoArray[i] = Page::INVALID_NUMBER;
		}
		newRootNode->keyArray[0] = pair.key;
		newRootNode->pageNoArray[0] = rootPageNum;
		newRootNode->pageNoArray[1] = pair.pageNo;
		if(rootPageNum == 2)
		{
			newRootNode->level = 1;
		}
		else
			newRootNode->level = 0;
		
		rootPageNum = newRootId;

		//std::cout<<"low: "<<rootNode->pageNoArray[0]<<" key: "<<rootNode->keyArray[0]<<" high: "<< rootNode->pageNoArray[1]<<std::endl;
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
