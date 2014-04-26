
//////////////////////////////////////////////////////////////////////////////
//                   ALL STUDENTS COMPLETE THESE SECTIONS
// Title:            (part6)
// Semester:         CS564 Spring 2014
//
// Author:           (yihong dai	)
// Email:            (ydai6@wisc.edu)
// CS Login:         (yihong)
// Lecturer's Name:  (Jeff Ballard)
//
//////////////////// PAIR PROGRAMMERS COMPLETE THIS SECTION ////////////////////
//                   CHECK ASSIGNMENT PAGE TO see IF PAIR-PROGRAMMING IS ALLOWED
//                   If allowed, learn what PAIR-PROGRAMMING IS,
//                   choose a partner wisely, and complete this section.
//
// Pair Partner:     (yulin shen)
// Email:            (shen28@wisc.edu)
// CS Login:         (yulin)
// Pair Partner:     (haoji liu)
// Email:            (hliu69@wisc.edu)
// CS Login:         (haoji)
//////////////////////////// 80 columns wide //////////////////////////////////
#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns: status
 * 	OK on success
 * 	an error code otherwise
 */
const Status QU_Delete(const string & relation, 
		const string & attrName,
		const Operator op,
		const Datatype type,
		const char *attrValue)
{
	// part 6
	// if the relation name is empty, then we return an error
	if(relation.empty() )
	{
		return BADCATPARM;
	}
	// if type is out of the specified range, then we return an error
	if(!(type==INTEGER||type==STRING||type==FLOAT))
	{
		return BADCATPARM;
	}
	Status status ;
	// open a heapfilescan to scan the relation file
	HeapFileScan * scanFile = new HeapFileScan(relation,status);

	//return an error message if error happens
	if(status !=OK)
	{
		return status;
	}
	// filter is used to hold the search value
	const char  * filter;
	// value1 and value2 are used to temporarily hold th evalue
	int value1;
	float value2;
	// if attrname is not empty, then we have to find its type
	if(!attrName.empty())
	{
		switch(type)
		{
		case INTEGER:
		{
			// convert the char array into int
			value1 = atoi(attrValue);
			filter = (const char *)&value1;
			break;
		}
		case FLOAT:{
			// convert the char array into float
			value2 = atof(attrValue);
			filter = (const char *)(&value2);
			break;
		}

		default:
		{
			filter = attrValue;
			break;
		}
		}

		AttrDesc record ;
		// fetch the attr info about the specified relation and attribute
		status = attrCat->getInfo(relation,attrName,record);
		if(status !=OK)
		{
			return status;
		}
		// start  the scan of heapfile using the filter, if filter is not empty
		status = scanFile->startScan(record.attrOffset,record.attrLen,type,filter,op);
		if(status !=OK)
		{
			return status;
		}

	}
	else // if the attrName is empty
	{
		// delete the whole relation
		scanFile->startScan(0,0,type,NULL,op);
	}

	RID outRID;
	// recursively delete the record that match our search
	while ((status= scanFile->scanNext(outRID))==OK)
	{
		scanFile->deleteRecord();
	}
	//free the pointer
	delete scanFile;

	return OK;



}


