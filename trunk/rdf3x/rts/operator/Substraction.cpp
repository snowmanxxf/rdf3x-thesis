#include "rts/operator/Substraction.hpp"
#include "rts/operator/PlanPrinter.hpp"
#include "rts/runtime/Runtime.hpp"
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
static inline unsigned singlehash1(unsigned key,unsigned hashTableSize) {
	return key&(hashTableSize-1);
}
static inline unsigned singlehash2(unsigned key,unsigned hashTableSize) {
	return hashTableSize+((key^(key>>3))&(hashTableSize-1));
}

unsigned Substraction::hash1(std::vector<unsigned> key,unsigned hashTableSize) {
	unsigned newkey = 0;
	for (unsigned i=0;i<keySize;++i)
		newkey = (newkey<<1) + key[i];
	return singlehash1(newkey,hashTableSize);
}
unsigned Substraction::hash1(unsigned key[],unsigned hashTableSize) {
	unsigned newkey = 0;
	for (unsigned i=0;i<keySize;++i)
		newkey = (newkey<<1) + key[i];
	return singlehash1(newkey,hashTableSize);
}

unsigned Substraction::hash2(std::vector<unsigned> key,unsigned hashTableSize) {
	unsigned newkey = 1;
	for (unsigned i=0;i<keySize;++i)
		newkey = newkey * (key[i]^(key[i]>>3));
	return singlehash2(newkey,hashTableSize);
}
unsigned Substraction::hash2(unsigned key[],unsigned hashTableSize) {
	unsigned newkey = 1;
	for (unsigned i=0;i<keySize;++i)
		newkey = newkey * (key[i]^(key[i]>>3));
	return singlehash2(newkey,hashTableSize);
}
//---------------------------------------------------------------------------
void Substraction::BuildHashTable::run()
   // Build the hash table
{
   if (done) {
	   cout << "hashtable done =)" << endl;
	   return; // XXX support repeated executions under nested loop joins etc!
   }

   // Prepare relevant domain informations
   vector<Register*> domainRegs;
   for (vector<Register*>::const_iterator iter=join.rightJoinKeys.begin(),limit=join.rightJoinKeys.end();iter!=limit;++iter)
      if ((*iter)->domain)
         domainRegs.push_back(*iter);
   vector<ObservedDomainDescription> observedDomains;
   observedDomains.resize(domainRegs.size());

   // Build the hash table from the right side
   unsigned hashTableSize = 1024;
   join.keySize = join.rightJoinKeys.size();
   cout << "keySize=" << join.keySize << endl;


   join.hashTable.clear();
   join.hashTable.resize(2*hashTableSize);
   for (unsigned rightCount=join.right->first();rightCount;rightCount=join.right->next()) {
      // Check the domain first
      bool joinCandidate=true;
      for (unsigned index=0,limit=domainRegs.size();index<limit;++index) {
         if (!domainRegs[index]->domain->couldQualify(domainRegs[index]->value)) {
            joinCandidate=false;
            break;
         }
         observedDomains[index].add(domainRegs[index]->value);
      }
      if (!joinCandidate)
         continue;

      // Compute the slots
      //unsigned join.rightKey=rightValue->value;
      cout << "Getting rightKey" << endl;
      join.getKey(join.rightJoinKeys,join.rightKey);
      join.printKey(join.rightKey);
      unsigned slot1=join.hash1(join.rightKey,hashTableSize),slot2=join.hash2(join.rightKey,hashTableSize);
      cout << "Hashvalues: " << slot1 << " " << slot2 << endl;


      // Scan if the entry already exists
      Entry* e=join.hashTable[slot1];
      if ((!e)||(!join.equalKeys(e->key,join.rightKey)))
         e=join.hashTable[slot2];
      if (e&&(join.equalKeys(e->key,join.rightKey))) {
         unsigned ofs=(e==join.hashTable[slot1])?slot1:slot2;
         bool match=false;
         for (Entry* iter=e;iter;iter=iter->next)
            if (join.equalKeys(iter->key,join.rightKey)) {
               // Tuple already in the table?
               match=true;
               // Then aggregate
               if (match) {
                  iter->count+=rightCount;
                  break;
               }
            }
         if (match) {
            continue;
         }

         // Append to the current bucket
         cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!appending to bucket" << endl;
         e=join.entryPool.alloc();
         cout << "entry allocated" << endl;
         e->next=join.hashTable[ofs];
         join.hashTable[ofs]=e;
         e->count=rightCount;
         for (unsigned i=0;i<join.keySize;i++) e->key[i]=join.rightKey[i];
         cout << "key=[";for (unsigned i=0;i<join.keySize;i++) cout << e->key[i] << ":"; cout << "]" << endl;
         cout << "appended to bucket" << endl;
         continue;
      }

      // Create a new tuple
      cout << "adding to table" << endl;
      e=join.entryPool.alloc();
      cout << "entry allocated" << endl;
      e->next=0;
      e->count=rightCount;
      for (unsigned i=0;i<join.keySize;i++) e->key[i]=join.rightKey[i];
      cout << "key=[";for (unsigned i=0;i<join.keySize;i++) cout << e->key[i] << ":"; cout << "]" << endl;

      // And insert it
      cout << "inserting into table" << endl;
      join.insert(e);
      hashTableSize=join.hashTable.size()/2;
      cout << "added to table" << endl;

   }

   // Update the domains
   for (unsigned index=0,limit=domainRegs.size();index<limit;++index)
      domainRegs[index]->domain->restrictTo(observedDomains[index]);

   done=true;
}
//---------------------------------------------------------------------------
void Substraction::ProbePeek::run()
   // Produce the first tuple from the probe side
{
   if (done) return; // XXX support repeated executions under nested loop joins etc!

   count=join.left->first();
   done=true;
}
//---------------------------------------------------------------------------
Substraction::Substraction(Operator* left,std::vector<Register*> leftJoinKeys,const std::vector<Register*>& leftTail,Operator* right,std::vector<Register*> rightJoinKeys,const std::vector<Register*>& rightTail,double hashPriority,double probePriority,double expectedOutputCardinality)
   : Operator(expectedOutputCardinality),left(left),right(right),
     leftJoinKeys(leftJoinKeys),rightJoinKeys(rightJoinKeys),leftTail(leftTail),rightTail(rightTail),entryPool(leftTail.size()*sizeof(unsigned)),
     buildHashTableTask(*this),probePeekTask(*this),hashPriority(hashPriority),probePriority(probePriority)
   // Constructor
{
	keySize = rightJoinKeys.size();
	cout << "keySize=" << keySize << endl;
}
//---------------------------------------------------------------------------
Substraction::~Substraction()
   // Destructor
{
   delete left;
   delete right;
   entryPool.freeAll();
}
//---------------------------------------------------------------------------
void Substraction::insert(Entry* e)
   // Insert into the hash table
{
   unsigned hashTableSize=hashTable.size()/2;
   // Try to insert
   bool firstTable=true;
   for (unsigned index=0;index<hashTableSize;index++) {
      unsigned slot=firstTable?hash1(e->key,hashTableSize):hash2(e->key,hashTableSize);
      swap(e,hashTable[slot]);
      if (!e) {
    	  cout << "inserted at " << slot << endl;
         return;
      }
      firstTable=!firstTable;
   }

   // No place found, rehash
   cout << "no place found, rehashing" << endl;
   vector<Entry*> oldTable;
   oldTable.resize(4*hashTableSize);
   swap(hashTable,oldTable);
   unsigned count = 0;
   for (vector<Entry*>::const_iterator iter=oldTable.begin(),limit=oldTable.end();iter!=limit;++iter)
      if (*iter) {
    	 count++;
    	 cout << "rehash insertion" << endl;
         insert(*iter);
      }
   cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! " << count << " out of " << 4*hashTableSize << endl;
   insert(e);
}
//---------------------------------------------------------------------------
void Substraction::printKey(std::vector<unsigned> &key) {
   if (key.empty()) cout << "[empty key]" << endl;
	cout << "Key=[";
	for (unsigned i=0;i<key.size();++i)
	  cout << key[i] << "::";
   cout << "]" << endl;
}
void Substraction::getKey(std::vector<Register*> keyRegs, std::vector<unsigned> &key) {
	unsigned count=0;
	while (key.size()<keySize)
		key.push_back(0);
	for (std::vector<Register*>::const_iterator iter=keyRegs.begin(),limit=keyRegs.end();iter!=limit&&count<key.size();++iter,++count)
		key[count]=((*iter)->value);
	for (;count<keySize;++count)
		key[count]=0;

}
bool Substraction::equalKeys(unsigned key1[], std::vector<unsigned> &key2){
	for (unsigned index=0;index<key2.size()&&index<sizeof(key1);++index)
		if (key1[index]!=key2[index]) {
			cout << "Different keys" << endl;
			return false;
		}
	cout << "equalKeys=true!" << endl;
	return true;
}
bool Substraction::contains(std::vector<unsigned> &key){
	  unsigned hashTableSize=hashTable.size()/2;
	  unsigned slot1=hash1(key,hashTableSize),slot2=hash2(key,hashTableSize);
	  cout << "Hashvalues: " << slot1 << " " << slot2 << endl;
	  Entry* e=hashTable[slot1];
	  if ((!e)||(!equalKeys(e->key,key)))
		 e=hashTable[slot2];
	  if (e&&(equalKeys(e->key,key))) {
		 for (Entry* iter=e;iter;iter=iter->next)
			if (equalKeys(iter->key,key)) {
			   // Tuple already in the table?
			   cout << "Key was found in hashtable" << endl;
			   return true;
			}
	  }
	  cout << "Key was NOT found in hashtable" << endl;
	  return false;
}
//---------------------------------------------------------------------------
unsigned Substraction::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;
   // Build the hash table if not already done
   buildHashTableTask.run();

   // Read the first tuple from the left side
   probePeekTask.run();
   if ((leftCount=probePeekTask.count)==0)
      return false;

   cout << "rightTail.size()=" << rightTail.size() << endl;
   cout << "leftTail.size()=" << leftTail.size() << endl;

   getKey(leftJoinKeys,leftKey);
   printKey(leftKey);

   //leftKey.push_back(leftValue->value);
   //for (unsigned index=0,limit=leftTail.size();index<limit;++index)
   //	  leftKey.push_back(leftTail[index]->value);

   // Setup the lookup
   found=contains(leftKey);
   askForNext=false;

   return next();
}
//---------------------------------------------------------------------------
unsigned Substraction::next()
   // Produce the next tuple
{
   while (true) {
	   if (askForNext) {
		  // Read the next tuple from the left
		  if ((leftCount=left->next())==0)
			 return false;

		  getKey(leftJoinKeys,leftKey);
		  printKey(leftKey);

		  found=contains(leftKey);

	  }
	  else askForNext=true;


      if (!found) {
    	   for (std::vector<Register*>::const_iterator iter1=rightJoinKeys.begin(),iter2=leftJoinKeys.begin(),limit=rightJoinKeys.end();iter1!=limit;++iter1,++iter2) {
    		   (*iter1)->value = (*iter2)->value;
    	   }

		   observedOutputCardinality+=leftCount;
		   unsigned count = leftCount;

		   found=contains(leftKey);

		   cout << "count=" << count << endl;
		   return count;
      }

   }
}
//---------------------------------------------------------------------------
void Substraction::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   out.beginOperator("Substraction",expectedOutputCardinality,observedOutputCardinality);
   //out.addEqualPredicateAnnotation(leftJoinKeys.front,rightJoinKeys.front());
   out.addMaterializationAnnotation(leftTail);
   out.addMaterializationAnnotation(rightTail);
   left->print(out);
   right->print(out);
   out.endOperator();
}
//---------------------------------------------------------------------------
void Substraction::addMergeHint(Register* /*reg1*/,Register* /*reg2*/)
   // Add a merge join hint
{
   // Do not propagate as we break the pipeline
}
//---------------------------------------------------------------------------
void Substraction::getAsyncInputCandidates(Scheduler& scheduler)
   // Register parts of the tree that can be executed asynchronous
{
   unsigned p1=scheduler.getRegisteredPoints();
   left->getAsyncInputCandidates(scheduler);
   scheduler.registerAsyncPoint(buildHashTableTask,0,hashPriority,p1);

   unsigned p2=scheduler.getRegisteredPoints();
   right->getAsyncInputCandidates(scheduler);
   scheduler.registerAsyncPoint(probePeekTask,1,probePriority,p2);
}
//---------------------------------------------------------------------------
