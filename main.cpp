#include <stdlib.h>
#include <ctype.h>

#include "include/minirel.h"
#include "include/bufmgr.h"
#include "include/heapfile.h"
#include "include/join.h"
#include "include/relation.h"

#include "timer.h"

#define RUN_TESTS 1 // Test mode ON/OFF

#if RUN_TESTS
#include "include/jointest.h"
#endif

int MINIBASE_RESTART_FLAG = 0;// used in minibase part

#define NUM_OF_DB_PAGES  2000 // define # of DB pages
#define NUM_OF_BUF_PAGES 50 // define Buf manager size.You will need to change this for the analysis

int main()
{
#if RUN_TESTS
	return RunTests();
#endif

	// Remove MINIBASE.DB if it exists
	std::remove("MINIBASE.DB");

	Status s;

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
	// =================== TIMED SECTION ===================

	//TupleNestedLoopJoin(specOfR, specOfS);
	//BlockNestedLoopJoin(specOfR, specOfS, (MINIBASE_BM->GetNumOfUnpinnedBuffers() - 3 * 3) * MINIBASE_PAGESIZE);
	IndexNestedLoopJoin(specOfR, specOfS);

	// ================ END OF TIMED SECTION ===============
	double elapsedTime = timer.stop();

	// Obtain and print statistics
	MINIBASE_BM->GetStat(pinCount, missCount);
	cout << elapsedTime << "\n" << missCount << "\n";

	return 0;
}
