#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
// part 6

	Status status;
	RelDesc rd;
	AttrDesc *attrs;
	Record rec;

	status = relCat->getInfo(relation, rd);
	if (status != OK) return status;

	int cnt;
	status = attrCat->getRelInfo(rd.relName, cnt, attrs);
	if (status != OK) return status;

	/*
	for(int i = 0; i < attrCnt; i++)
	{
		string attName(attrList[i].attrName);
		status = attrCat->getInfo(relation,attName, attrs);
		if (status != OK) return status;
	}
	*/

	for(int i = 0; i < attrCnt; i++)
	{
		if (attrList[i].attrValue == NULL) return ATTRNOTFOUND;
	}

	int length = 0;
	for(int i = 0; i < attrCnt; i++)
	{
		length = length + attrs[i].attrLen;
	}

	int int_value;
	float float_value;
	int offset = 0;
	char outData[length];
	for(int i = 0; i < attrCnt; i++)
	{
		for(int j = 0; j < attrCnt; j++)
		{
			if (strcmp(attrs[i].attrName, attrList[j].attrName) == 0)
			{
				if (attrs[i].attrType == 0)
				{
					memcpy(&outData[offset], attrList[j].attrValue, attrs[i].attrLen);
				}
				else if (attrs[i].attrType == 1)
				{
					int_value = atoi((char*)attrList[j].attrValue);
					memcpy(&outData[offset], &int_value, attrs[i].attrLen);
				}
				else if (attrs[i].attrType == 2)
				{
					float_value = atof((char*)attrList[j].attrValue);
					memcpy(&outData[offset], &float_value, attrs[i].attrLen);
				}
				offset = offset + attrs[i].attrLen;
			}
		}
	}

	rec.length = length;
	rec.data = &outData;

	InsertFileScan * ifs = new InsertFileScan(relation, status);
	if (status != OK) return status;

	RID outRid;
	status = ifs->insertRecord(rec, outRid);
	if (status != OK) return status;

	delete ifs;

return OK;

}
