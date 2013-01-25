#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../include/minirel.h"
#include "../include/heapfile.h"
#include "../include/scan.h"
#include "../include/join.h"
#include "../include/relation.h"
#include "../include/bufmgr.h"


//---------------------------------------------------------------
// Each join method takes in at least two parameters :
// - specOfS
// - specOfR
//
// They specify which relations we are going to join, which 
// attributes we are going to join on, the offsets of the 
// attributes etc.  specOfS specifies the inner relation while
// specOfR specifies the outer one.
//
//You can use MakeNewRecord() to create the new result record.
//
// Remember to clean up before exiting by "delete"ing any pointers
// that you "new"ed.  This includes any Scan/BTreeFileScan that 
// you have opened.
//---------------------------------------------------------------



JoinSpec TupleNestedLoopJoin(JoinSpec specOfR, JoinSpec specOfS)
{
	JoinSpec result;
	Status status = OK;

	Scan* scanR = specOfR.file->OpenScan(status);
	if (OK != status)
	{
		cerr << "ERROR: cannot open scan on the relation R heap file.\n";
		return result;
	}

	Scan* scanS = specOfS.file->OpenScan(status);
	if (OK != status)
	{
		cerr << "ERROR: cannot open scan on the relation S heap file.\n";
		return result;
	}

	HeapFile* joinedFile;
	joinedFile = new HeapFile(NULL, status); // create a result HeapFile
	if (OK != status)
	{
		cerr << "ERROR: cannot create a file for the joined relation.\n";
		return result;
	}

	// Join the relations
	int recLenR = specOfR.recLen;
	int recLenS = specOfS.recLen;
	int recLenJoined = recLenR + recLenS;

	char* recR = new char[recLenR];
	char* recS = new char[recLenS];
	char* recJoined = new char[recLenJoined];

	RecordID ridR, ridS, ridJoined;

	while (OK == scanR->GetNext(ridR, recR, recLenR))
	{
		while (OK == scanS->GetNext(ridS, recS, recLenS))
		{
			// TODO: check for field equality (now crossing all) using specOfR.joinAttr, specOfR.numOfAttr, etc!

			MakeNewRecord(recJoined, recR, recS, recLenR, recLenS);
			joinedFile->InsertRecord(recJoined, recLenJoined, ridJoined);
		}
	}

	delete scanR;
	delete scanS;

	delete[] recR;
	delete[] recS;
	delete[] recJoined;

	// Fill and return the result
	result.file = joinedFile;
	result.joinAttr = -1;
	result.numOfAttr = specOfR.numOfAttr + specOfS.numOfAttr;
	result.offset = -1;
	result.recLen = recLenJoined;

	return result;
}
