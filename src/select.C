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
	AttrDesc *attrDescCollec;
	AttrDesc  projectingDesc[projCnt];


	if((attr!=NULL)&& !(attr->attrType== (int)INTEGER || attr->attrType== (int)STRING || attr->attrType==(int)FLOAT))
	{
		return BADCATPARM;
	}
	int attrcnt;
	string projname (projNames[0].relName);
	// the attrdescCollec  store the attribute info of the given relation
	if((status = attrCat->getRelInfo(projname,attrcnt,attrDescCollec))!=OK)
	{
		return status;
	}



	int i = 0 ;
	int j = 0 ;
	int count = 0 ;
	int recordLenght = 0;
	int attrValueIndex = -1 ;
	while (i<attrcnt)
	{
		while (j <projCnt )
		{
			if(strcmp(attrDescCollec[i].attrName, projNames[j].attrName)==0)
			{
				projectingDesc[count] = attrDescCollec[i];
				recordLenght += attrDescCollec[i].attrLen;
				count++;
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
	if(attr == NULL)
	{
		return ScanSelect(result,projCnt,projectingDesc,NULL,op,NULL,recordLenght);
	}
	else
	{
		return ScanSelect(result,projCnt,projectingDesc, &attrDescCollec[attrValueIndex],op,attrValue,  recordLenght );
	}

}


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

	InsertFileScan resultScan = new InsertFileScan(result, status);
	if(status!=OK)
	{
		return status;
	}
	string str(projNames[0].relName);
	HeapFileScan scanFile = new HeapFileScan(str,status);
	if(status!=OK)
	{
		return status;
	}
	Datatype temp;

	// initilize record
	Record & rec;


	RID& outRid;


	const char  * passIn ;

	if(filter!=NULL)
	{
		switch(attrDesc->attrType){
		case (int)INTEGER: {
			passIn =	(const char *)(&(atoi(filter)));
			temp = INTEGER;
			break;
		}
		case (int)FLOAT:{
			passIn = (const char*)(&(atof(filter)));
			temp = FLOAT;
			break;
		}
		case (int)STRING:{
			passIn = filter ;
			temp = STRING;
			break;
		}
		}
		if((status =scanFile.startScan(attrDesc->attrOffset,attrDesc->attrLen,temp,passIn,op))!=OK)
		{
			return status;
		}
	}
	else
	{
		if((status=scanFile.startScan(attrDesc->attrOffset,attrDesc->attrLen,temp,NULL,op))!=OK)
		{
			return status;
		}
	}
	int offSet = 0;
	Record scanRecord ;
	while (scanFile.scanNext(outRid) == OK)
	{
		status  = scanFile.getRecord(rec);
		if(status != OK)
		{
			return status;
		}

		char recordDetail[reclen];

		for(int i = 0 ; i<projCnt; i++)
		{
			memcpy(recordDetail+offSet,(char*)rec.data+ projNames[i].attrOffset,projNames[i].attrLen);
			offSet += projNames[i].attrLen;
		}
		scanRecord.data = (void*)recordDetail;
		scanRecord.length = reclen;
		offSet = 0 ;
		RID outRid ;
		if((status = resultScan.insertRecord(scanRecord, outRid) )!=OK)
		{
			return status;
		}
	}
	delete resultScan ;
	delete scanFile ;
	return OK;


	/*



	 */



}
