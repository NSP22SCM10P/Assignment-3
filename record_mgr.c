// Assigenment -3 Record manger//
// Created by :Nidhi , Prashansa , jainam


//header file
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"


// the data structure that is used as Recod manager 
typedef struct RecordManager
{
	//its use for Buffer Manager to access files of Page 
	BM_PageHandle pageHandle;	 
	//buferpool for temporary storage
	BM_BufferPool bufferPool;	
	RID recordID;
	// To scan the records of table
	Expr *condition;
	// The overall number of tuples in table
	int tuplesCount;
	// position of first free page.
	int freePage;
	//to count the scanned records 
	int scanCount;
} RecordManager;

const int MAX_NUMBER_OF_PAGES = 100;
const int ATTRIBUTE_SIZE = 15; 

RecordManager *recordManager;

//-----------------------------------------------CUSTOM FUNCTIONS ----------------------------------------- //


//create by Prashansa
// function returns the free pages
int findFreeSlot(char *data, int recordSize)
{
	int a, total_Slots = PAGE_SIZE / recordSize; 
	a=0;

	while (a < total_Slots)
	{
		if (data[a * recordSize] != '+')
		{
			return a;
		}a++;
	}
	return -1;
}


// -------------------------------------- TABLE AND RECORD MANAGER FUNCTIONS -------------------------------------------- //


// Created by Prashansa
// initializing  the Record Manager
extern RC initRecordManager (void *mgmt_Data)
{
	// Calling method intstorageManager for initializing Storage Manager
	initStorageManager();
	// return the value
	return RC_OK; 
}


// Created by Prashansa
// shuting down the manager of records
extern RC shutdownRecordManager ()
{
	recordManager = NULL;
	free(recordManager);
	printf("shutdown recordmanager");
	return RC_OK;
}


// created by Prashansa
void pageOperation(char *name,SM_FileHandle fileHandle,char *data)
{
    createPageFile(name);
    openPageFile(name, &fileHandle);
    writeBlock(0, &fileHandle, data);
    closePageFile(&fileHandle);
}

//Created by Prashansa
// Creating a new table 
extern RC createTable (char *name, Schema *schema)
{
    
    recordManager = (RecordManager*) malloc(sizeof(RecordManager));
    initBufferPool(&recordManager->bufferPool, name, MAX_NUMBER_OF_PAGES, RS_LRU, NULL);
    SM_FileHandle fileHandle;
    char data1[PAGE_SIZE];
    char *page_Handle = data1;
     
    int result, a=0;
    
    while(a<2){
        *(int*)page_Handle = a; 
        page_Handle = page_Handle + sizeof(int);
	a++;
    }

    *(int*)page_Handle = schema->numAttr;
    page_Handle = page_Handle + sizeof(int);

    *(int*)page_Handle = schema->keySize;
    page_Handle = page_Handle + sizeof(int);
    int schemaNumberAttr = schema->numAttr;

    for(; a < schema->numAttr; a++){
            strncpy(page_Handle, schema->attrNames[a], ATTRIBUTE_SIZE);
            page_Handle = page_Handle + ATTRIBUTE_SIZE;
			
            *(int*)page_Handle = (int)schema->dataTypes[a];
            page_Handle = page_Handle + sizeof(int);
            *(int*)page_Handle = (int) schema->typeLength[a];
            page_Handle = page_Handle + sizeof(int);
    }

    
    pageOperation(name,fileHandle,data1);
    return RC_OK;
}


SM_PageHandle setTableSchema(RM_TableData *rel)
 {
     SM_PageHandle page_Handle;
    int i, attr_count;

	//set the 1st location of the page.
    page_Handle = (char*) recordManager->pageHandle.data;

    //find out total record entries.
    recordManager->tuplesCount= *(int*)page_Handle;
	//increase the pointer by the size of the data type. (max space the field can occupy)
    page_Handle = page_Handle + sizeof(int);

    //retrieve the free page.
	recordManager->freePage= *(int*)page_Handle;

	//increment the pointer.
    page_Handle = page_Handle + sizeof(int); 

	//number of attributes retrieved
    attr_count = *(int*)page_Handle;
    page_Handle = page_Handle + sizeof(int);
    
    Schema *schema; //declaring schema
    schema = (Schema*) malloc(sizeof(Schema));
    
    // Set schema parameters
    schema->numAttr = attr_count;
    schema->dataTypes = (DataType*) malloc(sizeof(DataType) *attr_count);
    schema->attrNames = (char**) malloc(sizeof(char*) *attr_count);
    schema->typeLength = (int*) malloc(sizeof(int) *attr_count);


    for(i = 0; i < attr_count; i++)
        schema->attrNames[i]= (char*) malloc(ATTRIBUTE_SIZE);
      
    for(i = 0; i< schema->numAttr; i++)
        {
        strncpy(schema->attrNames[i], page_Handle, ATTRIBUTE_SIZE);
        page_Handle = page_Handle + ATTRIBUTE_SIZE;
       
        schema->dataTypes[i]= *(int*) page_Handle;
        page_Handle = page_Handle + sizeof(int);

        schema->typeLength[i]= *(int*)page_Handle;
        page_Handle = page_Handle+ sizeof(int);
    }
    
    rel->schema = schema; //set table schema with the newly created schema.

    return page_Handle;

 }

// created by prashansa
// Function opens the table.
extern RC openTable (RM_TableData *rel, char *name)
{
		if (!name)
    {
        return RC_ERROR;
    }
    
    if(!rel)
    {
        return RC_ERROR;
    }
	//set the data to record manager
    rel->mgmtData = recordManager; 
    rel->name = name; //to set new table name.

    pinPage(&recordManager->bufferPool, &recordManager->pageHandle, 0); //pin the page in memory.

    SM_PageHandle bufMgrpageHandle;
    bufMgrpageHandle= setTableSchema(rel); //set the schema of the table.

    unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);

    forcePage(&recordManager->bufferPool, &recordManager->pageHandle); //write the page back to the disk.

    return RC_OK;


}
  
  
// created by Prashansa
//   for closeing  the table
extern RC closeTable (RM_TableData *rel)
{
	
	
	//checking wether the condition is satisfed or not
	
	if(!rel)
    {
        return RC_ERROR;
    }
	
	//calling method 
 	shutdownBufferPool(&recordManager->bufferPool); 
    rel->mgmtData = NULL;
    free(rel->schema); //to free up the space for the schema

    return RC_OK;
}

// created by Prashansa
// Function deletes the table
extern RC deleteTable (char *name)
{
	// deleting the old records from memory
	
	if(name == ((char*)0))
    {
        return RC_ERROR;
    }

    destroyPageFile(name);
    return RC_OK;

}

// created by Prashansa
// Returns the number of tuples
extern int getNumTuples (RM_TableData *rel)
{
	// Accessing tuple Count and returning it
	

	return((RecordManager * )rel->mgmtData)->tuplesCount;
}


// --------------------------------------RECORD FUNCTIONS-------------------------------------- 


char * scan_slot(RecordManager *record_Manager, Record *record,int recordSize)
{
 	char * data;
 	RID *record_ID = &record->id; 
	
	// to set the 1st page to the newly creates page
	record_ID->page = record_Manager->freePage;

	
	//to call the pinpage method to pin the pages with bufferppool
	pinPage(&record_Manager->bufferPool, &record_Manager->pageHandle, record_ID->page);
	
	//set the initial postion of the meta data
	data = record_Manager->pageHandle.data;
	 	
	// to get an empty slot
	record_ID->slot = findFreeSlot(data, recordSize); 	
	while(record_ID->slot == -1)
	{
		// calling unpinpage if the pn page is not there to detect  any page
		unpinPage(&record_Manager->bufferPool, &record_Manager->pageHandle);	
	
		record_ID->page++;
	
		pinPage(&record_Manager->bufferPool, &record_Manager->pageHandle, record_ID->page);
		
		//to set a first position		
		data = record_Manager->pageHandle.data;
		
		record_ID->slot = findFreeSlot(data, recordSize);
	}
	return data;
	
}
//Jainam
extern RC insertRecord (RM_TableData *rel, Record *record)
{
	
	//fetching meta data from data saved in database table
	RecordManager *record_Manager = rel->mgmtData;	
	
	
	//assigning the record id for specific record
	RID *record_ID = &record->id; 
	
	char  *slot_Pointer;
	
	//fatching record size in bytes to save it in schema
	int record_Size = getRecordSize(rel->schema);


       slot_Pointer = scan_slot(record_Manager,record,record_Size);
	

	
	//marking dirty page so that we can know this page was modifies
	markDirty(&record_Manager->bufferPool, &record_Manager->pageHandle);
	
	
	//here slot starting position will be decide
	slot_Pointer = slot_Pointer + (record_ID->slot * record_Size);

	
	//'+ is a indicator of no more space is available so new new should remove
	*slot_Pointer = '+';

	
	//function calling for  copy the record data to memory location
	memcpy(++slot_Pointer, record->data + 1, record_Size - 1);

	
	//this function call is for deleting page from buffer pool
	unpinPage(&record_Manager->bufferPool, &record_Manager->pageHandle);
	
	
	//adding count of tuples
	record_Manager->tuplesCount++;
        
	// pinPage function calling 	
	pinPage(&record_Manager->bufferPool, &record_Manager->pageHandle, 0);

	return RC_OK;
}
//  deleting the records

//Jainam
extern RC deleteRecord (RM_TableData *r, RID id1)
{
	
	//fetching meta data from data saved in table
	RecordManager *record_Manager = r->mgmtData;
	
	
	//pin the page for whcih the record want to update
	pinPage(&record_Manager->bufferPool, &record_Manager->pageHandle, id1.page);

	
	record_Manager->freePage = id1.page;
	
	char *data_1 = record_Manager->pageHandle.data;

	// finding record size
	int record_Size = getRecordSize(r->schema);

	
	//set data_1 pointer to the particular slot of record
	data_1 = data_1 + (id1.slot * record_Size);
	
	// '-' is used to check slot already used or not
	*data_1 = '-';
		

	//marking the page as dirty
	markDirty(&record_Manager->bufferPool, &record_Manager->pageHandle);
	//calling methodinto buffer pool
	unpinPage(&record_Manager->bufferPool, &record_Manager->pageHandle);

	return RC_OK;
}


//Jainam
extern RC updateRecord (RM_TableData *rel1, Record *record1)
{	
	// fetching metadata from saved table
	RecordManager *record_Manager = rel1->mgmtData;
	
	
	//pin page function call for the page which having record want to to update
	pinPage(&record_Manager->bufferPool, &record_Manager->pageHandle, record1->id.page);

	char *data_2;

	// deciding recordsize
	int record_Size = getRecordSize(rel1->schema);

	
	//assigning record id
	RID id = record1->id;

	
	//fetch record data memory location and calculate start position
	data_2 = record_Manager->pageHandle.data;
	data_2 = data_2 + (id.slot * record_Size);
	
	//'+' denotes that the record is not empty
	*data_2 = '+';
	
	
	//new record data will be copy to existing record
	memcpy(++data_2, record1->data + 1, record_Size - 1 );
	
	
	//setting specific page dirty as it is already used and modifies
	markDirty(&record_Manager->bufferPool, &record_Manager->pageHandle);

	
	//calling unpinpage function for unpin page after the recored retrived
	unpinPage(&record_Manager->bufferPool, &record_Manager->pageHandle);
	
	return RC_OK;	
}

// This function retrieves a record having Record ID "id" in the table referenced by "rel".
// The result record is stored in the location referenced by "record"

//Jainam
extern RC getRecord (RM_TableData *rel, RID id, Record *record)
{
	
	//fetching meta data from saved table
	RecordManager *record_Manager = rel->mgmtData;
	
	
	//pinpage function calling for record which we want to retreive
	pinPage(&record_Manager->bufferPool, &record_Manager->pageHandle, id.page);

	
	//calculation of the record size
	int record_Size = getRecordSize(rel->schema);
	char *data_Pointer = record_Manager->pageHandle.data;
	data_Pointer = data_Pointer + (id.slot * record_Size);
	
	if(*data_Pointer == '+')
	{
	        
	        //set id of record
		record->id = id;

		
		//assign a pointer 
		char *data_3 = record->data;

		
		memcpy(++data_3, data_Pointer + 1, record_Size - 1);
	}
	
		
	else
	{
		// returning an error 
		return RC_RM_NO_TUPLE_WITH_GIVEN_RID;
	}
		

	// calling method for buffer pool
	unpinPage(&record_Manager->bufferPool, &record_Manager->pageHandle);

	return RC_OK;
}


// ******** SCAN FUNCTIONS ******** //

//Jainam
void scanTable(RM_TableData *rel1, RM_ScanHandle *scan1, Expr *cond1)
{
    RecordManager *scan_Manager;
	RecordManager *table_Manager;

	
	//memory allocation 
    	scan_Manager = (RecordManager*) malloc(sizeof(RecordManager));
    	
	
	//meta data set to scan meta data
    	scan1->mgmtData = scan_Manager;
    	
	
	// 1 for start scanning from 1st page
    	scan_Manager->recordID.page = 1;
    	
	
	// 0 for scanning from first slot	
	scan_Manager->recordID.slot = 0;
	
	     	
	scan_Manager->scanCount = 0;

	
	//scan condition
    	scan_Manager->condition = cond1;
    	
	
	// meta data to table meta data
    	table_Manager = rel1->mgmtData;

	
	//tuplecount set
    	table_Manager->tuplesCount = ATTRIBUTE_SIZE;

	    	scan1->rel= rel1;    
}


//Jainam
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
	
	//condition for checking scan present
	if (cond == 0)
	{
		return RC_SCAN_CONDITION_NOT_FOUND;
	}

	// calling opentable
	openTable(rel, "ScanTable");

    scanTable(rel, scan, cond);

	return RC_OK;
}


// Nidhi
//next function is used for scanning next records of table and vlaue will be stored as final result
extern RC next (RM_ScanHandle *scan, Record *record)
{
	
	RecordManager *scan_Manager = scan->mgmtData;
	RecordManager *table_Manager = scan->rel->mgmtData;
    	Schema *schema = scan->rel->schema;
	

	// whether the condition is satisfed or not 
	if (scan_Manager->condition ==NULL)
	{
		return RC_SCAN_CONDITION_NOT_FOUND;
	}

	Value *final_result = (Value *) malloc(sizeof(Value));
   
	char *data1;
   	
	// to fet the size of schema 
	int record_Size = getRecordSize(schema);

	// to calculate the total slots that are defined in buffer manager
	int total_Slots = PAGE_SIZE / record_Size;

	int scan_Count = scan_Manager->scanCount;

	int tuples_Count = table_Manager->tuplesCount;

	
	//weather this tuple count condition is statisfed or not if not then return prespective message.
	while (tuples_Count == false)
		return RC_RM_NO_MORE_TUPLES;

	// looping till tuples_count reaches less than or equal too it
	if(scan_Count <= tuples_Count)
	{  
		//wheater this condition is okay or not
		if (scan_Count <= 0)
		{
			//setting the recordID.page as 1 and slot number to zero.
			scan_Manager->recordID.page = true;
			scan_Manager->recordID.slot = false;
		}
		else
		{
			// incrementing the size of the slot 
			scan_Manager->recordID.slot++;

		
			while(scan_Manager->recordID.slot >= total_Slots)
			{
				scan_Manager->recordID.slot = false;
				scan_Manager->recordID.page++;
			}
		}

		// calling pinpage method for buffer pool 
		pinPage(&table_Manager->bufferPool, &scan_Manager->pageHandle, scan_Manager->recordID.page);
			
		// for collecting the data of scan manager			
		data1 = scan_Manager->pageHandle.data;

		// collected data are added into the location of the slot size and record size
		data1 = data1 + (scan_Manager->recordID.slot * record_Size);
		
		record->id.page = scan_Manager->recordID.page;
		record->id.slot = scan_Manager->recordID.slot;

		// defining the first postion of the record that are pointed
		char *data_Pointer = record->data;


		*data_Pointer = '-';
		
		memcpy(++data_Pointer, data1 + 1, record_Size - 1);

		
		//value if the sacncount will be increased beacuse we already scanned one record
		scan_Manager->scanCount++;
		scan_Count++;

		evalExpr(record, schema, scan_Manager->condition, &final_result); 

		// whether this codidition is okay or not 
		while(final_result->v.boolV == 1)
		{
			// for removving the pin page that are assigned to buffer pool
			unpinPage(&table_Manager->bufferPool, &scan_Manager->pageHandle);
			// Return value			
			return RC_OK;
		}
	}
	
	
	unpinPage(&table_Manager->bufferPool, &scan_Manager->pageHandle);
	
	// update the assign value 
	scan_Manager->recordID.page = true;
	scan_Manager->recordID.slot = false;
	scan_Manager->scanCount = false;
	
	return RC_RM_NO_MORE_TUPLES;
}


//Nidhi
// for closing scan operation.
extern RC closeScan (RM_ScanHandle *scan)
{
	RecordManager *scan_Manager = scan->mgmtData;
	RecordManager *record_Manager = scan->rel->mgmtData;

	// Check whether the scan is complete it or not
	while(scan_Manager->scanCount > 0)
	{
		// calling unpinpage method to remove the scan.
		unpinPage(&record_Manager->bufferPool, &scan_Manager->pageHandle);
		
		// here, we are resetting the default values
		scan_Manager->scanCount = 0;
		// record id default  page value is mark as 1 in pages.
		scan_Manager->recordID.page = 1;
		//record id default slotvlaue  is mark as 0 in pages.
		scan_Manager->recordID.slot = 0;
	}
	
	
	// removing the memory space that are  assigned to the scan official data
    	scan->mgmtData = NULL;
    	free(scan->mgmtData);  // releasing the sapce with the help of free function
	
	return RC_OK; // return the value
}


// ------------------------- SCHEMA FUNCTIONS ------------------------ //

//Nidhi
//always return the value of schema
extern int getRecordSize (Schema *schema)
{
	int size_schema = 0, a=0;
	
	// for loop to iterate the the vlaue
	while( a < schema->numAttr)
	{
	
		int length=schema->dataTypes[a];
		if(length== DT_STRING)
		{
			// If datatype is STRING 
			size_schema = size_schema + schema->typeLength[a];
		}
		else if(length== DT_INT){
			//If the data type  is INTEGER, add  INT value
			size_schema = size_schema + sizeof(int);
		}
		else if(length== DT_FLOAT)
		{   	// If the datatype is FLOAT, the sixe of  FLOAT will be added to this fucntion
			size_schema = size_schema + sizeof(float); }
		else if(length== DT_BOOL){
			// datatype is BOOLEAN,  BOOLEAN values will be added to schema
			size_schema = size_schema+ sizeof(bool);
		}a++;

		
	}
	return ++size_schema;
}
//Nidhi
//  creating new schema
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
	// schema can allocate the memeory of space 
	Schema *new_schema = (Schema *) malloc(sizeof(Schema));

	// numAtter is for number of attribute 	
	new_schema->numAttr = numAttr;

	// atttrNames isfor number of names allocated to nee schema
	new_schema->attrNames = attrNames;

	// allocate new dataypes in to newly craeted schema
	new_schema->dataTypes = dataTypes;

	// Type Length for new schema
	new_schema->typeLength = typeLength;


	//  Key Size  in the newly created schema
	new_schema->keySize = keySize;


	//  settle Key Attributes  in the new schema
	new_schema->keyAttrs = keys;

	return new_schema; 
}

//Nidhi
//free schema function is mainly used for de allocting the memory space in given schema.
extern RC freeSchema (Schema *schema)
{
	//relasing  memory space that is occupied by schema
	free(schema);
	return RC_OK;
}


//-------------------------DEALING WITH RECORDS AND ATTRIBUTE VALUES ----------------------------- //


//Nidhi
// createing  a new record in the schmea
extern RC createRecord (Record **record, Schema *schema)
{
	// Allocating free space to the newly created record
	Record *new_Record = (Record*) malloc(sizeof(Record));
	
	//   to find the actual record size
	int record_Size = getRecordSize(schema);

	// assigning some free space  to the new record   
	new_Record->data= (char*) malloc(record_Size);


	//the position is set-1 bacuse at starting point we dont have any idea about allocation of space 
	new_Record->id.page = new_Record->id.slot = -1;


	//strating pointer is for pointing the records frm begining.
	char *data_Pointer = new_Record->data;
	
	// '-' shows the record is empty.
	*data_Pointer = '-'; 
	*(++data_Pointer) = '\0';
	*record = new_Record;

	return RC_OK;
}



//Nidhi
// function is used for setting the value for attribute with proper offset
RC attrOffset (Schema *schema, int attrNum, int *result)
{
	int a=0;
	*result = 1;


	while(a < attrNum)
	{
	
		if(schema->dataTypes[a]== DT_STRING){
			// datatypes is string,the typelength of string  should be added
			*result = *result + schema->typeLength[a];}
		else if(schema->dataTypes[a]== DT_INT){
			// datatypes is  INTEGER,value of int should be added 
			*result = *result + sizeof(int);}
		else if(schema->dataTypes[a]== DT_FLOAT){
			// datatypes is  FLOAT,value of float should be added
			*result = *result + sizeof(float);}
		else if(schema->dataTypes[a]== DT_BOOL){
			// datatypes is  BOOLEAN ,value of bool should be added
			*result = *result + sizeof(bool);
		}a++;
			
	}
	return RC_OK;
}

//Nidhi

//realising the space that is allocated by memory
extern RC freeRecord (Record *record)
{
	//  free the memory space
	free(record);
	return RC_OK; //return values
}


//Nidhi

//get an atttribute from the records itself
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
	int Attr_offset = 0;

	//get the offset value for the given schema
	attrOffset(schema, attrNum, &Attr_offset);

	// Allocate memory space for the value with the attribute of it
	Value *attribute_value = (Value*) malloc(sizeof(Value));

	// its used to get the begining position of the data 
	char *data_Pointer = record->data;
	
	//offset will be added to first position
	data_Pointer = data_Pointer + Attr_offset;

	// If attrNum = 1, then value return itself is 1 otherwise the datatype number

	int length=schema->dataTypes[attrNum];
	
	if(attrNum == 1)
	{     length=1;
	}else
	{ 
		length=schema->dataTypes[attrNum];
	}
	
	
	if(length ==DT_STRING)
	{
			//to get a value of STRING data type
			int length = schema->typeLength[attrNum];
			//set the space for string datatype with couting the length of it
			attribute_value->v.stringV = (char *) malloc(length + 1);
			strncpy(attribute_value->v.stringV, data_Pointer, length);
			attribute_value->v.stringV[length] = '\0';
			attribute_value->dt = DT_STRING;
	}
	else if(length==DT_INT)
	{
			// to get a value of INT data type
			int value = 0;
			memcpy(&value, data_Pointer, sizeof(int));
			attribute_value->v.intV = value;
			attribute_value->dt = DT_INT;
	}
	else if(length==DT_FLOAT)
	{
			// to get a value of FLOAT data type
	  		float value;
	  		memcpy(&value, data_Pointer, sizeof(float));
	  		attribute_value->v.floatV = value;
			attribute_value->dt = DT_FLOAT;
	}
	else if(length==DT_BOOL)
	{
			// to get a value of BOOL data type
			bool value;
			memcpy(&value,data_Pointer, sizeof(bool));
			attribute_value->v.boolV = value;
			attribute_value->dt = DT_BOOL;
	}
	
	
	

	*value = attribute_value;
	return RC_OK;
}

//Nidhi


//set the value of recors in given schema
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
	int Attr_offset = 0;

	
	//get the offset value for attribute
	attrOffset(schema, attrNum, &Attr_offset);

	//get the value for begining postion of record
	char *data_Pointer = record->data;
	
	// Add the offset value at start
	data_Pointer = data_Pointer + Attr_offset;
	
	int length=schema->dataTypes[attrNum];
	if(length==DT_STRING)
	{
			// to get a value of String data type
			int length = schema->typeLength[attrNum];
			// data is copied from one string to another string with the help of value
			strncpy(data_Pointer, value->v.stringV, length);
			data_Pointer = data_Pointer + schema->typeLength[attrNum];
	}
	else if (length==DT_INT)
	{
			//to get a value of INT data type
			*(int *) data_Pointer = value->v.intV;	  
			data_Pointer = data_Pointer + sizeof(int);
	}
	else if (length==DT_FLOAT)
	{
			// to  get a value of FLOAT data type
			*(float *) data_Pointer = value->v.floatV;
			data_Pointer = data_Pointer + sizeof(float);
	}
	else if (length==DT_BOOL)
	{
			// to get a value of BOOL data type
			*(bool *) data_Pointer = value->v.boolV;
			data_Pointer = data_Pointer + sizeof(bool);
	}
			
	return RC_OK; // return the value 
}
