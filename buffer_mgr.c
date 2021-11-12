#include<stdio.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>



// A buffer pool (memory) represents one page frame.
typedef struct Page
{
	SM_PageHandle data; // original data
	PageNumber pageNum; // Each page has an identification number
	int dirtyBit; // Indicates if the contents of the page have been changed by the client
	int fixCount; // Counts the number of clients visiting a specific page in a given instance
	int hitNum;   // Obtain the least used page using LRU algorithm
	int refNum;   // LFU algorithm uses this to locate the least frequented page
} PageFrame;

// "bufferSize" shows the size of the buffer pool.
int buffer_Size = 0;

// FIFO function uses the rearIndex to calculate the frontIndex, which in turn is used to calculate the byte count of the first page read from disk.
int rear_Index = 0;

// This counter counts how many I/Os were written to the disk, e.g. how many pages were written.
int write_Count = 0;

// "LRU uses the "hit" count to determine the least recently added frame to the buffer pool. It is incremented by each frame added to the buffer pool.
int hit = 0;

// "CLOCK method uses "clockPointer" to point to the most recently inserted page in the buffer pool..
int clock_Pointer = 0;

// "lf Pointer" is used by LFU algorithm to store the least frequently used page frame's position. It speeds up operation  from 2nd replacement onwards.
int lfuPointer = 0;

// creating FIFO function

// created by Nidhi------

extern void FIFO(BM_BufferPool *const bm, PageFrame *page)
{
    
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;

	int i, front_Index;
	front_Index = rear_Index % buffer_Size;

	// Iterating all page frames in the buffer pool
	for(i = 0; i < buffer_Size; i++)
	{
		if(pageFrame[front_Index].fixCount == 0)
		{
			// If page in memory has been modified (dirtyBit = 1), then write page to disk
			if(pageFrame[front_Index].dirtyBit == 1)
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				writeBlock(pageFrame[front_Index].pageNum, &fh, pageFrame[front_Index].data);

				// Increase the writeCount which records the number of writes done by the buffer manager.
				write_Count++;
			}

			// Setting page frame's content to new page's content
			pageFrame[front_Index].data = page->data;
			pageFrame[front_Index].pageNum = page->pageNum;
			pageFrame[front_Index].dirtyBit = page->dirtyBit;
			pageFrame[front_Index].fixCount = page->fixCount;
			break;
		}
		else
		{
			// If the current page frame is being used by some client, we move on to the next location
			front_Index++;
			 if (front_Index % buffer_Size == 0) {
			front_Index= 0; }
			else{
			front_Index=front_Index;
			}
		}
	}
}
// creating LFU function

// created by Nidhi------

extern void LFU(BM_BufferPool *const bm, PageFrame *page)
{
	//printf("LFU Started");
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;

	int a, b, leastFreq_Index, leastFreq_Ref;
	leastFreq_Index = 0;

	// creating a loop through all the page frames in the buffer pool
	for(a = 0; a < buffer_Size; a++)
	{
		while(pageFrame[leastFreq_Index].fixCount == 0)
		{
			leastFreq_Ref = pageFrame[((leastFreq_Index + a) % buffer_Size)].refNum;
			break;
		}
	}

	a = (leastFreq_Index + 1) % buffer_Size;

	// Find the page frame having minimal refNum (i.e. it is used the least frequent)
	for(b = 0; b < buffer_Size; b++)
	{
		while(pageFrame[a].refNum < leastFreq_Ref)
		{
			leastFreq_Index = a;
			leastFreq_Ref = pageFrame[a].refNum;
			break;
		}
		a = (a + 1) % buffer_Size;
	}

	// If page in memory has been modified (dirtyBit = 1), then write page to disk
	if(pageFrame[leastFreq_Index].dirtyBit == 1)
	{
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		writeBlock(pageFrame[leastFreq_Index].pageNum, &fh, pageFrame[leastFreq_Index].data);

		// Increase the writeCount which records the number of writes done by the buffer manager.
		write_Count++;
	}

	// Setting page frame's content to new page's content
	pageFrame[leastFreq_Index].data = page->data;
	pageFrame[leastFreq_Index].pageNum = page->pageNum;
	pageFrame[leastFreq_Index].dirtyBit = page->dirtyBit;
	pageFrame[leastFreq_Index].fixCount = page->fixCount;
	lfuPointer = leastFreq_Index + 1;
}
//created by PC

// creating LRU  function

/*extern void LRU(BM_BufferPool *const bm, PageFrame *page)
{
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	int leastHitIndex, leastHitNum;
	int a = 0;
	// Iterating through all the page frames in the buffer pool.
	
	while(a<buffer_Size)
	{
		if(pageFrame[a].fixCount == 0)
		{
			leastHitIndex = a;
			leastHitNum = pageFrame[a].hitNum;
		}
		a = a+1;
	}

	// Finding the page frame having minimum hitNum (i.e. it is the least recently used) page frame
	a = 0;
	while(a<buffer_Size)
	{
		if(pageFrame[a].hitNum < leastHitNum)
		{
			leastHitIndex = a;
			leastHitNum = pageFrame[a].hitNum;
		}
	}
	

	// If page in memory has been modified (dirtyBit = 1), then write page to disk
	if(pageFrame[leastHitIndex].dirtyBit == 1)
	{
		SM_FileHandle fh;
		if(openPageFile(bm->pageFile, &fh)==RC_OK)
		{
			if(writeBlock(pageFrame[leastHitIndex].pageNum, &fh, pageFrame[leastHitIndex].data)==RC_OK)
			{
				write_Count++;
			}
		}
	}
	pageFrame[leastHitIndex].data = page->data;
	pageFrame[leastHitIndex].pageNum = page->pageNum;
	pageFrame[leastHitIndex].dirtyBit = page->dirtyBit;
	pageFrame[leastHitIndex].fixCount = page->fixCount;
	pageFrame[leastHitIndex].hitNum = page->hitNum;
}*/
extern void LRU(BM_BufferPool *const bm, PageFrame *page)
{
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	int a, leastHit_Index, leastHit_Num;

	
	//All the frames will be iterated in buffer pool
	for(a = 0; a < buffer_Size; a++)
	{
		// Finds the page frame whose fixCount is 0.
		if(pageFrame[a].fixCount == 0)
		{
			leastHit_Index = a;
			leastHit_Num = pageFrame[a].hitNum;
			break;
		}
	}

	// Finds the the page frame having least hitNum
	for(a = leastHit_Index + 1; a < buffer_Size; a++)
	{
		if(pageFrame[a].hitNum < leastHit_Num)
		{
			leastHit_Index = a;
			leastHit_Num = pageFrame[a].hitNum;
		}
	}

	
	//writing page to a disk if page in memory is modified
	if(pageFrame[leastHit_Index].dirtyBit == 1)
	{
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		writeBlock(pageFrame[leastHit_Index].pageNum, &fh, pageFrame[leastHit_Index].data);

		// rising  the writeCount which records the number of writes .
		write_Count++;
	}

	//  page frame's content  is stored into a new page content
	pageFrame[leastHit_Index].data = page->data;
	pageFrame[leastHit_Index].pageNum = page->pageNum;
	pageFrame[leastHit_Index].dirtyBit = page->dirtyBit;
	pageFrame[leastHit_Index].fixCount = page->fixCount;
	pageFrame[leastHit_Index].hitNum = page->hitNum;
}

//created by PC
// Defining CLOCK function
extern void CLOCK(BM_BufferPool *const bm, PageFrame *page)
{
	//printf("CLOCK Started");
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	while(true)
	{
		clock_Pointer = (clock_Pointer % buffer_Size == 0) ? 0 : clock_Pointer;

		if(pageFrame[clock_Pointer].hitNum == 0)
		{
			// If page in memory has been updated dirtyBit is 1, then write page to disk
			if(pageFrame[clock_Pointer].dirtyBit == 1)
			{
				SM_FileHandle fh;
				if(openPageFile(bm->pageFile, &fh)==RC_OK)
				{
					if(writeBlock(pageFrame[clock_Pointer].pageNum, &fh, pageFrame[clock_Pointer].data)==RC_OK)
					{
						write_Count++;
					}
				}
				
			}

			// Setting page frame's content to new page's content
			pageFrame[clock_Pointer].data = page->data;
			pageFrame[clock_Pointer].pageNum = page->pageNum;
			pageFrame[clock_Pointer].dirtyBit = page->dirtyBit;
			pageFrame[clock_Pointer].fixCount = page->fixCount;
			pageFrame[clock_Pointer].hitNum = page->hitNum;
			clock_Pointer++;
			break;
		}
		else
		{
			pageFrame[clock_Pointer++].hitNum = 0;
		}
	}
}


/*
    creating and initializing a buffer pool with numPages page frames.
   
*/
//written  by jainam
extern RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
		  const int numPages, ReplacementStrategy strategy,
		  void *stratData)
{
	bm->pageFile = (char *)pageFileName;
	bm->numPages = numPages;
	bm->strategy = strategy;

	// occupied memory space 
	PageFrame *page = malloc(sizeof(PageFrame) * numPages);

	buffer_Size = numPages;
	int a=0;

	// ALl pages  Intilalized  in buffer pool.
	for(; a < buffer_Size; a++)
	{
		page[a].data = NULL;
		page[a].pageNum = -1;
		page[a].dirtyBit = 0;
		page[a].fixCount = 0;
		page[a].hitNum = 0;
		page[a].refNum = 0;
	}

	bm->mgmtData = page;
	write_Count = 0;
	clock_Pointer =0;
 	lfuPointer = 0;
	return RC_OK;

}


// Shuting down buffer pool
//written by jainam
extern RC shutdownBufferPool(BM_BufferPool *const bm)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	
	//writing dirty pages into the disk
	forceFlushPool(bm);

	int b=0;
	for(; b < buffer_Size; b++)
	{
		// if the  contents of the page was updated and has not been written back to disk it means fixcount!=0
		while(pageFrame[b].fixCount != 0)
		{
			return RC_PINNED_PAGES_IN_BUFFER;

			//break;
		}
	}

	// Removing space
	free(pageFrame);
	
	bm->mgmtData = NULL;
	return RC_OK; //returning the value 
}

//jainam

//writing all the dirty pages to the disk
extern RC forceFlushPool(BM_BufferPool *const bm)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;

	int a=0;
	int b=buffer_Size;
	
	//storing all the dirty pages into the disk
	for(; a < b; a++)
	{
		if(pageFrame[a].fixCount == 0 && pageFrame[a].dirtyBit == 1)
		{
			SM_FileHandle fh;
			// Open the page file 
			openPageFile(bm->pageFile, &fh);
			// Writing block of data 
			writeBlock(pageFrame[a].pageNum, &fh, pageFrame[a].data);
			//  the page not dirty.
			pageFrame[a].dirtyBit = 0;
			// rising the write Count.
			write_Count++;
		}
	}
	return RC_OK;// returning the value
}


//------------------------------------------ ***** PAGE MANAGEMENT FUNCTIONS *****----------------------------- //

//Created by PC
// marks the page as dirty indicating that the data of the page has been updated.
extern RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PageFrame *bufferFrame = (PageFrame *)bm->mgmtData;
	
	int i = 0;
	// Iterating through all the pages in the buffer pool
	while(i<buffer_Size)
	{
		if(bufferFrame[i].pageNum == page->pageNum){
			bufferFrame[i].dirtyBit = 1;
			return RC_OK;
		}
		i = i+1;

	}
	return RC_ERROR;
}


//created by PC
// This function unpins a page from the memory i.e. removes a page from the memory

extern RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;

	int b;
	// Iterating through all the pages in the buffer pool
	for(b = 0; b < buffer_Size; b++)
	{
// If the current page is the page to be unpinned, then decrease fixCount and exit loop
		if(pageFrame[b].pageNum == page->pageNum)
		{
			pageFrame[b].fixCount--;
			return RC_OK;		
		}
	}
	return RC_READ_NON_EXISTING_PAGE;
}

//created BY PC
// This function writes the contents of the modified pages back to the page file on disk

extern RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;

	int a=0;
	// Iterating  all the pages
	for(; a < buffer_Size; a++)
	{
		// If the current page is page to be written to disk, then write the page to disk.
		if(pageFrame[a].pageNum == page->pageNum)
		{
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			writeBlock(pageFrame[a].pageNum, &fh, pageFrame[a].data);

			// Mark page as undirty 
			pageFrame[a].dirtyBit = 0;

			// rise the write Count.
			write_Count++;
		}
	}
	return RC_OK;
}


// This function pins a page with page number pageNum i.e. adds the page with page number pageNum to the buffer pool.
// If the buffer pool is full, then it uses appropriate page replacement strategy to replace a page in memory with the new page being pinned.

extern RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
   const PageNumber pageNum)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;

	// Checking if buffer pool is empty and this is the first page to be pinned
	if(pageFrame[0].pageNum != -1)
	{
		int i;
		bool isBufferFull = true;

		for(i = 0; i < buffer_Size; i++)
		{
		if(pageFrame[i].pageNum != -1)
		{
		//conditon to check if page is in memory or not
		if(pageFrame[i].pageNum == pageNum)
		{
		// Increasing fixCount so that one more client access the same page
		pageFrame[i].fixCount++;
		isBufferFull = false;
		hit++; // Incrementing hit

	if(bm->strategy == RS_LRU)
	// LRU algorithm uses the value of hit to determine the least recently used page
	pageFrame[i].hitNum = hit;
	else if(bm->strategy == RS_CLOCK)
	// hitNum = 1 for indicate that this was the last page frame examined
	pageFrame[i].hitNum = 1;
	else if(bm->strategy == RS_LFU)

	//Incrementting refNum to one more to the number of times the page is utilized
	pageFrame[i].refNum++;

	page->pageNum = pageNum;
	page->data = pageFrame[i].data;

	clock_Pointer++;
	break;
	}
	} else {
	SM_FileHandle fh;
	openPageFile(bm->pageFile, &fh);
	pageFrame[i].data = (SM_PageHandle) malloc(PAGE_SIZE);
	readBlock(pageNum, &fh, pageFrame[i].data);
	pageFrame[i].pageNum = pageNum;
	pageFrame[i].fixCount = 1;
	pageFrame[i].refNum = 0;
	rear_Index++;
	hit++; // Incrementing hit variable

	if(bm->strategy == RS_LRU)
	// LRU algorithm uses the value of hit to determine the least recently used page
	pageFrame[i].hitNum = hit;
	else if(bm->strategy == RS_CLOCK)
	//hitNum = 1 to denote this was the last page frame which was checked

	pageFrame[i].hitNum = 1;

	page->pageNum = pageNum;
	page->data = pageFrame[i].data;

	isBufferFull = false;
	break;
	}
	}


	//If isBufferFull is true then it denotes that the buffer is full and we have to replage exist page using page replacement method
	if(isBufferFull == true)
	{

	//Create new page to read and store data
	PageFrame *newPage = (PageFrame *) malloc(sizeof(PageFrame));

	// read page from disk and start page frame's data in the buffer pool
	SM_FileHandle fh;
	openPageFile(bm->pageFile, &fh);
	newPage->data = (SM_PageHandle) malloc(PAGE_SIZE);
	readBlock(pageNum, &fh, newPage->data);
	newPage->pageNum = pageNum;
	newPage->dirtyBit = 0;
	newPage->fixCount = 1;
	newPage->refNum = 0;
	rear_Index++;
	hit++;

	if(bm->strategy == RS_LRU)

	// LRU uses the value of hit to get the least recently used page
	newPage->hitNum = hit;
	else if(bm->strategy == RS_CLOCK)

	newPage->hitNum = 1;

	page->pageNum = pageNum;
	page->data = newPage->data;


	//this is for calling of appropriate algorithm which is depending on which method is selected
	switch(bm->strategy)
	{
		case RS_FIFO: // FIFO
		FIFO(bm, newPage);
		break;

		case RS_LRU: // LRU
		LRU(bm, newPage);
		break;

		case RS_CLOCK: // CLOCK
		CLOCK(bm, newPage);
		break;

		case RS_LFU: // LFU
		LFU(bm, newPage);
		break;

		case RS_LRU_K:
		printf("\n LRU-k algorithm not implemented");
		break;

		default:
		printf("\nAlgorithm Not Implemented\n");
		break;
	}

	}
	return RC_OK;


	}
	else
	{


	SM_FileHandle fh;
	openPageFile(bm->pageFile, &fh);
	pageFrame[0].data = (SM_PageHandle) malloc(PAGE_SIZE);
	ensureCapacity(pageNum,&fh);
	readBlock(pageNum, &fh, pageFrame[0].data);
	pageFrame[0].pageNum = pageNum;
	pageFrame[0].fixCount++;
	rear_Index = hit = 0;
	pageFrame[0].hitNum = hit;
	pageFrame[0].refNum = 0;
	page->pageNum = pageNum;
	page->data = pageFrame[0].data;

	return RC_OK;
	}
}


//---------------------- ***** STATISTICS FUNCTIONS ***** ----------------------------


//created by Nidhi-----
// This function returns  page numbers of an array .
extern PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	PageNumber *frame_Contents = malloc(sizeof(PageNumber) * buffer_Size);
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;

	

	for(int i=0;i< buffer_Size;i++){
		frame_Contents[i] = (pageFrame[i].pageNum != -1) ? pageFrame[i].pageNum : NO_PAGE;
		}
		
	return frame_Contents;
}
//created by Nidhi-----
// This functions every element define the dirtyBit of the respective page.
extern bool *getDirtyFlags (BM_BufferPool *const bm)
{
	bool *dirtyFlags = malloc(sizeof(bool) * buffer_Size);
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;

	int i;
	// Looping over all of the pages in the buffer pool and modifying dirtyFlags to TRUE if the page is dirty and FALSE if it isn't.
	for(i = 0; i < buffer_Size; i++)
	{
	    // chnages ternary operaaotr and convert in if else----------------------------
		if (pageFrame[i].dirtyBit == 1) {
                dirtyFlags[i]=true ;
        	} else {
             	dirtyFlags[i]=false ;
        }
	}
	return dirtyFlags;
}
//created by Nidhi-----
// The ith member of the array of ints (size numPages) returned by this method is the fix count of the page stored in the ith page frame.
extern int *getFixCounts (BM_BufferPool *const bm)
{
	int *fixCounts = malloc(sizeof(int) * buffer_Size);
	PageFrame *pageFrame= (PageFrame *)bm->mgmtData;

	int i = 0;
	// Iterating  the pages in the buffer pool and set the  fixCounts value to page's fixCount
	while(i < buffer_Size)
	{
		if(pageFrame[i].fixCount != -1){
              		fixCounts[i]=pageFrame[i].fixCount;
		}  else{
		     fixCounts[i]=0;
		}
		i++;
	}
	return fixCounts;
}

// This work returns the number of pages that have been studied from disk since a buffer pool has been initialized.
//created by Nidhi-----
extern int getNumReadIO (BM_BufferPool *const bm)
{
	// Adding extra 1 because with start rearIndex=0.
	return (rear_Index + 1);
}

// This work returns the number of pages composed to the page record since the buffer pool has been initialized.
//created by Nidhi-----
extern int getNumWriteIO (BM_BufferPool *const bm)
{
	return write_Count;
}
