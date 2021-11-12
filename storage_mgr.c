#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<unistd.h>
#include<string.h>
#include<math.h>

#include "storage_mgr.h"

FILE *page_file;
/* Written by Nidhi */
extern void initStorageManager(void){
	page_file = NULL;
	printf("Storage manager Initialized!!!");
}
/* Written by Nidhi */
extern RC createPageFile(char *file_name){
    // opening the file stream in write and read mode
    // 'w+' creates an empty file
    page_file = fopen(file_name, "w+");
    if(page_file != NULL){
        //creating empty page in memory
        SM_PageHandle emptyPage = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
        //writing empty page into file
        if(fwrite(emptyPage,sizeof(char),PAGE_SIZE,page_file)<PAGE_SIZE){
			printf("Writing into file failed....!!!\n");
        }
        else{
			printf("Writing into file succeeded....!!!\n");
        }
        // freeing the allocated memory
        free(emptyPage);
		return RC_OK;
    }
    else{
		return RC_FILE_NOT_FOUND;
    }
}
/* Written by Nidhi */
extern RC openPageFile (char *file_name, SM_FileHandle *file_handle) {
	// Opening file stream in read mode. 'r' mode creates an empty file for reading only.
	page_file = fopen(file_name, "r");

	// Checking if file was successfully opened.
	if(page_file == NULL) {
		return RC_FILE_NOT_FOUND;
	} else {
		// Updating file handle's file_name and set the current position to the start of the page.
		file_handle->fileName = file_name;
		file_handle->curPagePos = 0;

		

		struct stat fileInfo;
		if(fstat(fileno(page_file), &fileInfo) < 0)
			return RC_ERROR;
		file_handle->totalNumPages = fileInfo.st_size/ PAGE_SIZE;

		// Closing file stream so that all the buffers are flushed.
		fclose(page_file);
		return RC_OK;
	}
}
/* Written by Nidhi */
extern RC closePageFile (SM_FileHandle *fHandle) {
	// Checking if file pointer or the storage manager is intialised. If initialised, then close.
	/*if(page_file != NULL)
		page_file = NULL;
	return RC_OK;*/
	printf("test check\n");
	if(fopen(fHandle->fileName, "r") == NULL)
    	{
		printf("\nFile not found!");
		return RC_FILE_NOT_FOUND;
	}
	else{
		//Update this section 
		//Previous code fclose(fHandle->mgmtInfo);
		//below is new code
        fclose(page_file);
        printf("\nFile Closed!");
		return RC_OK;
	}


}

/* Written by Nidhi */
extern RC destroyPageFile (char *fileName) {
	// In read mode, open the file stream.
	page_file = fopen(fileName, "r");

	if(page_file == NULL)
		return RC_FILE_NOT_FOUND;
	else{
	// removing the given file so that it is no longer available
	remove(fileName);
	printf("\nFile Destroyed!");
	return RC_OK;
	}
}
/* Written By Prashansa */
extern RC readBlock (int pageNum, SM_FileHandle *file_handle, SM_PageHandle memPage) {
	// Checking if the pageNumber parameter is less than Total number of pages and less than 0, then return respective error code
	if (pageNum > file_handle->totalNumPages || pageNum < 0)
        	return RC_READ_NON_EXISTING_PAGE;

	// Opening file stream in read mode. 'r' mode opens file for reading only.
	page_file = fopen(file_handle->fileName, "r");

	// Checking if file was successfully opened.
	if(page_file == NULL)
		return RC_FILE_NOT_FOUND;

	// Setting the cursor(pointer) position of the file stream. Position is calculated by Page Number x Page Size
	// And the seek is success if fseek() return 0
	int isSeekSuccess = fseek(page_file, (pageNum * PAGE_SIZE), SEEK_SET);
	if(isSeekSuccess == 0) {
		// We're reading the content and storing it in the location pointed out by memPage.
		if(fread(memPage, sizeof(char), PAGE_SIZE, page_file) < PAGE_SIZE)
			return RC_ERROR;
	} else {
		return RC_READ_NON_EXISTING_PAGE;
	}

	// Setting the current page position to the cursor(pointer) position of the file stream
	file_handle->curPagePos = ftell(page_file);

	// Closing file stream so that all the buffers are flushed.
	fclose(page_file);

    	return RC_OK;
	

	/*if(fopen(fHandle->fileName,"r") != NULL){
		//Making sure the Page Number is > 0 and Less than Total Number of pages or else throwing an Non existing page error.
		if (pageNum > fHandle->totalNumPages || pageNum < 0)
        	return RC_READ_NON_EXISTING_PAGE;
		//Seeking to start of the page file to read
		fseek(page_file, (pageNum * PAGE_SIZE), SEEK_SET);
		//Reading from start of the page to end of the same page
		fread(memPage, sizeof(char), PAGE_SIZE, page_file);
		//updating the Current Page Position to the pagenum as the program keeps on reading the pagefile.
		fHandle->curPagePos = pageNum;
    	return RC_OK;
	}
	//if File not found.
	else{
		return RC_FILE_NOT_FOUND;
	}*/
}
/* Jainam */
extern int getBlockPos (SM_FileHandle *fHandle) {
	// Returning the current page position retrieved from the file handle
	return fHandle->curPagePos;
}

/* Jainam */

extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	// Re-directing (passing) to readBlock(...) function with pageNumber = 0 i.e. first block
	return readBlock(0, fHandle, memPage);
}

/* Written by Jainam */
extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	//Reading previous block by passing current postion-1 value to readblock.
	return readBlock(getBlockPos(fHandle) - 1, fHandle, memPage);
}


extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {

	return readBlock(getBlockPos(fHandle), fHandle, memPage);
}

/* Written by Jainam */
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	//
	return readBlock(getBlockPos(fHandle) + 1, fHandle, memPage);
}

/* Written by Jainam */
extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	//
	return readBlock(fHandle->totalNumPages, fHandle, memPage);
}
/*written by prashanasa*/
extern RC writeBlock (int pageNum, SM_FileHandle *file_handle, SM_PageHandle memPage) {
	// Checking if the pageNumber parameter is less than Total number of pages and less than 0, then return respective error code
	if (pageNum > file_handle->totalNumPages || pageNum < 0)
        	return RC_WRITE_FAILED;

	// Opening file stream in read & write mode. 'r+' mode opens the file for both reading and writing.
	page_file = fopen(file_handle->fileName, "r+");

	// Checking if file was successfully opened.
	if(page_file == NULL)
		return RC_FILE_NOT_FOUND;

	int startPosition = pageNum * PAGE_SIZE;

	if(pageNum == 0) {
		//Writing data to non-first page
		fseek(page_file, startPosition, SEEK_SET);
		int i;
		for(i = 0; i < PAGE_SIZE; i++)
		{
			// Checking if it is end of file. If yes then append an enpty block.
			if(feof(page_file)) // check file is ending in between writing
				 appendEmptyBlock(file_handle);
			// Writing a character from memPage to page file
			fputc(memPage[i], page_file);
		}

		// Setting the current page position to the cursor(pointer) position of the file stream
		file_handle->curPagePos = ftell(page_file);

		// Closing file stream so that all the buffers are flushed.
		fclose(page_file);
	} else {
		// Writing data to the first page.
		file_handle->curPagePos = startPosition;
		fclose(page_file);
		writeCurrentBlock(file_handle, memPage);
	}
	return RC_OK;
	
	/*if(fopen(fHandle->fileName,"r") != NULL){

		//Ensuring that pagenum is less than total number of pages and pagenum is greater than 0.
		if (fHandle->totalNumPages >  pageNum || pageNum > 0){

			//opeining the file in write mode to write the content to the file
			page_file = fopen(fHandle->fileName, "w");
			//seeking the start of the file
			fseek(page_file, (pageNum * PAGE_SIZE), SEEK_SET);
			//Writing content to the file
			fwrite(memPage,PAGE_SIZE,1,page_file);
			//Updating the Current Page number after writing content to the page file.
			fHandle->curPagePos = pageNum;

			//Updating the Total Number of pages after compeleting writing operation
			fHandle->totalNumPages = ftell(page_file)/PAGE_SIZE;
			return RC_OK;
		}

		//If page Num is out of Bound then throwing write Fail error.
		else{
			return RC_WRITE_FAILED;
		}
	}
	//If page_file not found.
	else{
		return RC_FILE_NOT_FOUND;
	}*/

}
/* Written By Prashansa */
extern RC writeCurrentBlock (SM_FileHandle *file_handle, SM_PageHandle memPage) {
	// Opening file stream in read & write mode. 'r+' mode opens the file for both reading and writing.
	page_file = fopen(file_handle->fileName, "r+");

	// Checking if file was successfully opened.
	if(page_file == NULL)
		return RC_FILE_NOT_FOUND;

	// Appending an empty block to make some space for the new content.
	appendEmptyBlock(file_handle);

	// Initiliazing file pointer
	fseek(page_file, file_handle->curPagePos, SEEK_SET);

	// Writing memPage contents to the file.
	fwrite(memPage, sizeof(char), strlen(memPage), page_file);

	// Setting the current page position to the cursor(pointer) position of the file stream
	file_handle->curPagePos = ftell(page_file);

	// Closing file stream so that all the buffers are flushed.
	fclose(page_file);
	return RC_OK;

	//return writeBlock(getBlockPos(file_Handle), file_Handle, memPage);
}

/* Written By Prashansa */
extern RC appendEmptyBlock (SM_FileHandle *fhandle) {
	// Creating an empty page of size PAGE_SIZE bytes
	SM_PageHandle emptyBlock = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));

	// Moving the cursor (pointer) position to the begining of the file stream.
	// And the seek is success if fseek() return 0
	int isSeekSuccess = fseek(page_file, 0, SEEK_END);

	if( isSeekSuccess == 0 ) {
		// Writing an empty page to the file
		fwrite(emptyBlock, sizeof(char), PAGE_SIZE, page_file);
	} else {
		free(emptyBlock);
		return RC_WRITE_FAILED;
	}

	// De-allocating the memory previously allocated to 'emptyPage'.
	// This is optional but always better to do for proper memory management.
	free(emptyBlock);

	// Incrementing the total number of pages since we added an empty black.
	fhandle->totalNumPages++;
	return RC_OK;

	/*if(fopen(fHandle->fileName,"w")!= NULL){
		//Writing a block of size Page Size.
		int i;
		char block[PAGE_SIZE];
		while (i<PAGE_SIZE)
		{
			block[i]=0;
			i++;
		}
		//Seeking the start of the file
		fseek(page_file, 0, SEEK_END);
		//Appending the empty block at the end of the file.
		fwrite(block, sizeof(char), PAGE_SIZE, page_file);
		//Updating the total number of pages in the page file after appending the empty block at the end of the file.
		fHandle->totalNumPages++;
		return RC_OK;
	}
	//if file not found Throwing error that file not found
	else{
		return RC_FILE_NOT_FOUND;
	}*/
	
}

/* Written By Prashansa */

extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {

	//checking of file is not empty
	if(fopen(fHandle->fileName, "r")!= NULL){
		//initializing variables
		int numofPagetoAppend,n;
		//storing the value of number pages to be aappended to end of page file to ensure capacity
		numofPagetoAppend = numberOfPages - fHandle->totalNumPages;
		n=1;
		//looping through to append empty block at the end of the file.
		while(n == numofPagetoAppend){
			appendEmptyBlock(fHandle);
			n++;
		}
	}
	else{
		return RC_FILE_NOT_FOUND;
	}
}






