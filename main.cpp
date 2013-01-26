#include <stdlib.h>
#include <ctype.h>

#include "include/minirel.h"
#include "include/bufmgr.h"
#include "include/heapfile.h"
#include "include/join.h"
#include "include/relation.h"

#include "timer.h"

int MINIBASE_RESTART_FLAG = 0;// used in minibase part

#define VERBOSE 1 // 1 - verbose (human readable) output, 0 - machine readable output

#define NUM_OF_DB_PAGES  2000 // define # of DB pages
#define NUM_OF_BUF_PAGES 50 // define Buf manager size.You will need to change this for the analysis
//#define NUM_OF_BUF_PAGES 50 // define Buf manager size.You will need to change this for the analysis


void printStats(int pinCount, int missCount, double elapsedTime)
{
#if VERBOSE
	cout << "Elapsed time: " << elapsedTime << " s.\n";
	cout << "Number of buffer pool misses: " << missCount << " (out of " << pinCount << ").\n";
#else
	cout << elapsedTime << "\n" << missCount;
#endif
}


int main()
{
	Status s;

	// Remove MINIBASE.DB if it exists
	std::remove("MINIBASE.DB");

	// Initialize Minibase's Global Variables
	minibase_globals = new SystemDefs(s, 
		"MINIBASE.DB",
		"MINIBASE.LOG",
		NUM_OF_DB_PAGES,   // Number of pages allocated for database
		500,
		NUM_OF_BUF_PAGES,  // Number of frames in buffer pool
		NULL);
	
	// Initialize random seed
	srand(1);

	// Create Random Relations R(outer relation) and S for joining. The definition is in relation.h, 
	// # of tuples: NUM_OF_REC_IN_R, NUM_OF_REC_IN_S in join.h
	CreateR();
	CreateS();

	// Initialize the specification for joins
	JoinSpec specOfS, specOfR;

	CreateSpecForR(specOfR);
	CreateSpecForS(specOfS);

	// Create a timer for time measurement
	Timer timer;

	// Join and collect statistics
	long pinCount, missCount;
	MINIBASE_BM->ResetStat();

	timer.start();
	HeapFile* joinedFile = TupleNestedLoopJoin(specOfR, specOfS);
	double elapsedTime = timer.stop();

	MINIBASE_BM->GetStat(pinCount, missCount);
	
	// Print statistics
	printStats(pinCount, missCount, elapsedTime);

#if VERBOSE

	// Print out relations
	cout << "\nRelation R [Employee (id, age, proj, salary, rating, dept)]:\n";
	PrintR(specOfR.file, specOfR.relName);

	cout << "\nRelation S [Project (id, fund, manager, status)]:\n";
	PrintS(specOfS.file, specOfS.relName);

	cout << "\nJoined relation [Employee.proj = Project.id] (E.proj, R.id, E.id):\n";
	char* resultFileName = new char[3];
	strcpy(resultFileName, "RS");
	PrintResult(joinedFile, resultFileName);

#endif

	return 1;
}
