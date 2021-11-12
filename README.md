# Assignment-3

Assignment-1 Storage manager Created by:
Group 26 Nidhi Patel ,Prashansa Chaudhary, Jainam Shah
Description: 
The main objective is to make  simple record manager. The record manager handles tables with a fixed schema. Clients can insert records, delete records, update records, and scan through the records in a table.



-----------------------HOW TO RUN PROJECT----------------------------
I.Go To Assignment-3 folder and clone it using git clone .

II.Type “LS” to list the files and check that we are in the correct directory.

III.Type----- "make clean" to delete old compiled .o files.

IV.Type ------"make" to compile all project files including "test_assign3_1.c" file 

V.Type -------"make run" to run "test_assign3_1.c" file.

VI.Type----- "make test_expr" to compile test expression related files including "test_expr.c".

VII.Type------ "make run_expr" to run "test_expr.c" file
1. TABLE AND RECORD MANAGER FUNCTIONS

initRecordManager (...)
•This function initializes the record manager.
•We have  initStorageManager(...) function of Storage Manager to initialize the storage manager. 

shutdownRecordManager(...)
•This function shutsdown the record manager and de-allocates all the resources allocated to the record manager.

createTable(...)
•This function opens the table having name specified by the paramater 'name.
•sets the attributes (name, datatype and size) of the table.
•It then creates a page file, opens that page file, writes the block containing the table in the page file and closes the page file.

openTable(...)
•This function creates a table with name as specified in the parameter 'name' in the schema specified in the parameter 'schema'.

closeTable(...)
•This function closes the table as pointed by the parameter 'rel'.
•It does so by calling BUffer Manager's function shutdownBufferPool(...).

deleteTable(...)
•This function deletes the table with name specified by the parameter 'name'.
•It calls the Storage Manager's function destroyPageFile(...).



getNumTuples(...)
•	To get an total number of tuple we will use in table 

2. RECORD FUNCTIONS

insertRecord(...)
•This function inserts a record in the table and updates the 'record' parameter with the Record ID passed in the insertRecord() function.

deleteRecord(...)
•This function deletes a record having Record ID 'id' passed through the parameter from the table .

updateRecord(...)
•To updates a record referenced by the parameter "record" in the table .

getRecord(....)
•To fetch a record having Record ID "id" passed in the paramater in the table.


3. SCAN FUNCTIONS


startScan(...)
•To starts a scan by getting data from the RM_ScanHandle data structure .

next(...)
•To returns the next tuple which satisfies the condition (test expression).

closeScan(...) 
•To function closes the scan operation.

4. SCHEMA FUNCTIONS

getRecordSize(...)
•To returns the size of a record in the specified schema.


freeSchema(...)
•This function removes the schema specified by the parameter 'schema' from the memory.

createSchema(...)
•To create a new schema with the specified parameters in memory.


5. ATTRIBUTE FUNCTIONS

createRecord(...)
•To creates a new record in the schema passed by parameter 'schema' and passes the new record to the 'record' paramater in the createRecord() function.

attrOffset(...)
•To sets the offset (in bytes) from initial position to the specified attribute of the record into the 'result' parameter passed through the function.
 

freeRecord(...)
•To de-allocates the memory space allocated to the 'record' passed through the parameter.

getAttr(...)
•To retrieves an attribute from the given record in the specified schema.

setAttr(...)
•To sets the attribute value in the record in the specified schema. The record, schema and attribute number whose data is to be retrieved is passed through the parameter.

