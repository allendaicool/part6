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
#include "stdio.h"
#include "stdlib.h"

// forward declaration
const Status ScanSelect(const string & result, 
		const int projCnt,
		const AttrDesc projNames[],
		const AttrDesc *attrDesc,
		const Operator op,
		const char *filter,
		const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		const int projCnt,
		const attrInfo projNames[],
		const attrInfo *attr,
		const Operator op,
		const char *attrValue)
{
	// Qu_Select sets up things and then calls ScanSelect to do the actual work
	cout << "Doing QU_Select " << endl;
	// attrInfo *attr  is the where clause attribute.
	// If it is null, it means you have to return all the rows from the relation table.
	//The relation name is one of the input arguments of QU_Select
	Status status ;
	// attrDescCollec is passed into getRelinfo to fetch the array of attrDesc.
	AttrDesc *attrDescCollec ;
	//projectingDesc[projCnt] is used to hold the attribute in select clause
	AttrDesc  projectingDesc[projCnt];

	// return an error if bad input
	if((attr!=NULL)&& !(attr->attrType== (int)INTEGER || attr->attrType== (int)STRING || attr->attrType==(int)FLOAT))
	{
		return BADCATPARM;
	}
	// attrcnt is used to store the attribute count
	int attrcnt;
	// convert char array into string
	string projname (projNames[0].relName);
	// the attrdescCollec  store the attribute info of the given relation
	if((status = attrCat->getRelInfo(projname,attrcnt,attrDescCollec))!=OK)
	{
		return status;
	}


	// i and j  is used to hold the loop count
	int i = 0 ;
	int j = 0 ;
	int recordLenght = 0;
	int attrValueIndex = -1 ;

	/* while loop iterate through project array for each attribute of the given relation
	 * to find the projecting attribute in the attribute array and record the record length
	 * as well, also we write the attribute that is in the where clause
	 */
	while (i<attrcnt)
	{
		while (j <projCnt )
		{
			if(strcmp(attrDescCollec[i].attrName, projNames[j].attrName)==0)
			{
				projectingDesc[j] = attrDescCollec[i];
				recordLenght += attrDescCollec[i].attrLen;
			}
			j++;
		}
		//attr is in the where clause, find index of attr in the attrDescCollec
		if(attr!=NULL &&strcmp(attrDescCollec[i].attrName,attr->attrName)==0)
		{
			attrValueIndex = i ;
		}
		j = 0;
		i++;
	}
	// if there is nothing in the where clause, then we pass in the null as filter
	if(attr == NULL)
	{
		return ScanSelect(result,projCnt,projectingDesc,NULL,op,NULL,recordLenght);
	}
	else
	{
		// if there is something in the where clause, then we pass in the attrvalue as filter
		return ScanSelect(result,projCnt,projectingDesc, &attrDescCollec[attrValueIndex],op,attrValue,  recordLenght );
	}

}

/*
 * ScanSelect create insertfilescan to write result the result
 * and open a heapfilescan to scan the relation file
 * write the matched record into the result file.
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
const Status ScanSelect(const string & result, 
		const int projCnt,
		const AttrDesc projNames[],
		const AttrDesc *attrDesc,
		const Operator op,
		const char *filter,
		const int reclen)
{
	cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
	// attrDesc points the attr in where clause
	// projName is the projection in select clause;
	Status status ;
	// create an insertFileScan to write info into the result file
	InsertFileScan* resultScan = new InsertFileScan(result, status);
	if(status!=OK)
	{
		return status;
	}
	//convert char array into string
	string str(projNames[0].relName);
	// create a heapfile to scan the given relation
	HeapFileScan* scanFile = new HeapFileScan(str,status);

	if(status!=OK)
	{
		return status;
	}

	// temp used to store the type information
	Datatype temp;

	// initilize record
	Record  rec;

	// initilize an outrid
	RID outRid;


	// pass in is used to store the filter information
	const char  * passIn ;
	// value1 and value2 are used to hold value temporarily
	int value1;
	float value2;

	/* if filter is not empty, the we have
	 * to find the datatype of the given filter
	 * and do the date conversion as well
	 */
	if(filter!=NULL)
	{

		switch(attrDesc->attrType){
		case 1: {

			value1 = atoi(filter);
			passIn = (const char *)(&value1);
			temp = INTEGER;

			break;
		}
		case 2:{

			value2 = atof(filter);
			passIn = (const char*)(&value2);
			temp = FLOAT;

			break;
		}
		case 0:{

			passIn = filter ;
			temp = STRING;

			break;
		}
		}
		/* start the scan of relation file based on the filter information
		 *
		 */
		if((status =scanFile->startScan(attrDesc->attrOffset,attrDesc->attrLen,temp,passIn,op))!=OK)
		{

			return status;
		}
	}
	else
	{
		// if the filter is empty, then we  just to need to pass in the null as information
		if((status=scanFile->startScan(0,0,temp,NULL,op))!=OK)
		{
			return status;
		}
	}

	int offSet = 0;
	Record scanRecord ;
	/* start the scan based on the filter
	 * if find matched record, then we write
	 * that record into result heapfile
	 */
	while (scanFile->scanNext(outRid) == OK)
	{
		// get the record matching filter.
		status  = scanFile->getRecord(rec);
		if(status != OK)
		{
			return status;
		}
		// used to hold the record
		char recordDetail[reclen];
		// store the record into recorddetail
		for(int i = 0 ; i<projCnt; i++)
		{
			memcpy(recordDetail+offSet,(char*)rec.data+ projNames[i].attrOffset,projNames[i].attrLen);
			offSet += projNames[i].attrLen;
		}
		scanRecord.data = (void*)recordDetail;
		scanRecord.length = reclen;
		// reset the offset
		offSet = 0 ;

		RID outRid ;
		// do the inserting work
		if((status = resultScan->insertRecord(scanRecord, outRid) )!=OK)
		{
			return status;
		}
	}
	// free the pointer
	delete resultScan ;
	delete scanFile ;
	return OK;


	/*



	 */



}
