#include "rts/operator/Matching.hpp"
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
//---------------------------------------------------------------------------
unsigned Matching::hash1(unsigned key[KEYSIZE],unsigned hashTableSize) {
	unsigned newkey = 0;
	for (unsigned i=0;i<keySize;++i)
		newkey = (newkey<<1) + key[i];
	return singlehash1(newkey,hashTableSize);
}
unsigned Matching::hash2(unsigned key[KEYSIZE],unsigned hashTableSize) {
	unsigned newkey = 1;
	for (unsigned i=0;i<keySize;++i)
		newkey = newkey * (key[i]^(key[i]>>3));
	return singlehash2(newkey,hashTableSize);
}
//---------------------------------------------------------------------------
void Matching::BuildHashTable::run()
   // Build the hash table
{
   if (done) {
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
   unsigned rightKey[KEYSIZE];

   join.hashTable.clear();
   join.hashTable.resize(2*hashTableSize);
   for (unsigned rightCount=join.right->first();rightCount;rightCount=join.right->next()) {
      // Check the domain first

      for (unsigned index=0,limit=domainRegs.size();index<limit;++index) {
         /*if (!domainRegs[index]->domain->couldQualify(domainRegs[index]->value)) {
            joinCandidate=false;
            break;
         }*/
         observedDomains[index].add(domainRegs[index]->value);
      }

      // Compute the slots
      join.getKey(join.rightJoinKeys,rightKey);
      unsigned slot1=join.hash1(rightKey,hashTableSize),slot2=join.hash2(rightKey,hashTableSize);

      // Scan if the entry already exists
      Entry* e=join.hashTable[slot1];
      if ((!e)||(!join.equalKeys(e->key,rightKey)))
         e=join.hashTable[slot2];
      if (e&&(join.equalKeys(rightKey,e->key))) {
         unsigned ofs=(e==join.hashTable[slot1])?slot1:slot2;
         bool match=false;
         for (Entry* iter=e;iter;iter=iter->next)
            if (join.equalKeys(rightKey,iter->key)) {
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
         e=join.entryPool.alloc();
         e->next=join.hashTable[ofs];
         join.hashTable[ofs]=e;
         for (unsigned i=0;i<join.keySize;i++)
        	 e->key[i]=rightKey[i];
         e->count=rightCount;
         continue;
      }

      // Create a new tuple
      e=join.entryPool.alloc();
      e->next=0;
      for (unsigned i=0;i<join.keySize;i++)
    	  e->key[i]=rightKey[i];
      e->count=rightCount;

      // And insert it
      join.insert(e);
      hashTableSize=join.hashTable.size()/2;

   }

   // Update the domains
   for (unsigned index=0,limit=domainRegs.size();index<limit;++index)
      domainRegs[index]->domain->restrictTo(observedDomains[index]);

   done=true;
}
//---------------------------------------------------------------------------
void Matching::ProbePeek::run()
   // Produce the first tuple from the probe side
{
   if (done) return; // XXX support repeated executions under nested loop joins etc!

   count=join.left->first();
   done=true;
}
//---------------------------------------------------------------------------
Matching::Matching(Register* match,Operator* left,std::vector<Register*> leftJoinKeys,const std::vector<Register*>& leftTail,Operator* right,std::vector<Register*> rightJoinKeys,double hashPriority,double probePriority,double expectedOutputCardinality)
   : Operator(expectedOutputCardinality),left(left),right(right),
     leftJoinKeys(leftJoinKeys),rightJoinKeys(rightJoinKeys),match(match),leftTail(leftTail),entryPool(leftTail.size()*sizeof(unsigned)),
     buildHashTableTask(*this),probePeekTask(*this),hashPriority(hashPriority),probePriority(probePriority)
   // Constructor
{
	keySize = rightJoinKeys.size();
}
//---------------------------------------------------------------------------
Matching::~Matching()
   // Destructor
{
   delete left;
   delete right;
}
//---------------------------------------------------------------------------
void Matching::insert(Entry* e)
   // Insert into the hash table
{
   unsigned hashTableSize=hashTable.size()/2;
   // Try to insert
   bool firstTable=true;
   for (unsigned index=0;index<hashTableSize;index++) {
      unsigned slot=firstTable?hash1(e->key,hashTableSize):hash2(e->key,hashTableSize);
      swap(e,hashTable[slot]);
      if (!e) {
         return;
      }
      firstTable=!firstTable;
   }

   // No place found, rehash
   vector<Entry*> oldTable;
   oldTable.resize(4*hashTableSize);
   swap(hashTable,oldTable);
   unsigned count = 0;
   for (vector<Entry*>::const_iterator iter=oldTable.begin(),limit=oldTable.end();iter!=limit;++iter)
      if (*iter) {
    	 count++;
         insert(*iter);
      }
   insert(e);
}
//---------------------------------------------------------------------------
void Matching::printKey(unsigned key[KEYSIZE]) {
   cout << "Key=[";
	for (unsigned index=0;index<keySize;++index)
	  cout << key[index] << "::";
   cout << "]" << endl;
}
void Matching::getKey(std::vector<Register*> keyRegs, unsigned key[KEYSIZE]) {
	unsigned count=0;
	for (std::vector<Register*>::const_iterator iter=keyRegs.begin(),limit=keyRegs.end();iter!=limit&&count<keySize;++iter,++count)
		key[count]=((*iter)->value);
	for (;count<keySize;++count)
		key[count]=0;

}
bool Matching::equalKeys(unsigned key1[KEYSIZE], unsigned key2[KEYSIZE]){
	for (unsigned index=0;index<keySize;++index)
		if (key1[index]!=key2[index]) {
			return false;
		}
	return true;
}
bool Matching::contains(unsigned key[KEYSIZE]){
	  unsigned hashTableSize=hashTable.size()/2;
	  unsigned slot1=hash1(key,hashTableSize),slot2=hash2(key,hashTableSize);
	  Entry* e=hashTable[slot1];
	  if ((!e)||(!equalKeys(e->key,key)))
		 e=hashTable[slot2];
	  if (e/*&&(equalKeys(e->key,key))*/) {
		 for (Entry* iter=e;iter;iter=iter->next)
			if (equalKeys(iter->key,key)) {
			   // Tuple already in the table?
			   return true;
			}
	  }
	  return false;
}
//---------------------------------------------------------------------------
unsigned Matching::first()
   // Produce the first tuple
{
   observedOutputCardinality=0;
   // Build the hash table if not already done
   buildHashTableTask.run();

   // Read the first tuple from the left side
   probePeekTask.run();
   if ((leftCount=probePeekTask.count)==0)
      return false;

   getKey(leftJoinKeys,leftKey);

   // Setup the lookup
   found=contains(leftKey);
   askForNext=false;

   return next();
}
//---------------------------------------------------------------------------
unsigned Matching::next()
   // Produce the next tuple
{
   while (true) {
	   if (askForNext) { // Read the next tuple from the left (except from first time)
		  if ((leftCount=left->next())==0)
			 return false;
		  getKey(leftJoinKeys,leftKey);
		  found=contains(leftKey);
	  }
	  else {
		  askForNext=true;
	  }
      if (!found) {
    	  //match->value = 0;
    	  match->value = 3153797;
		  observedOutputCardinality+=leftCount;
		  return leftCount;
      }
      else {
    	  //match->value = 1;
    	  match->value = 3153800;
		  observedOutputCardinality+=leftCount;
		  return leftCount;
      }


   }
}
//---------------------------------------------------------------------------
void Matching::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   out.beginOperator("Matching",expectedOutputCardinality,observedOutputCardinality);
   out.addEqualPredicateAnnotation(leftJoinKeys.front(),rightJoinKeys.front());
   out.addMaterializationAnnotation(leftTail);
   left->print(out);
   right->print(out);
   out.endOperator();
}
//---------------------------------------------------------------------------
void Matching::addMergeHint(Register* /*reg1*/,Register* /*reg2*/)
   // Add a merge join hint
{
   // Do not propagate as we break the pipeline
}
//---------------------------------------------------------------------------
void Matching::getAsyncInputCandidates(Scheduler& scheduler)
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
