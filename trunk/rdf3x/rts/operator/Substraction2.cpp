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
Substraction::Substraction(Register* result,Operator* left,Register* leftReg,Operator* right,Register* rightReg,double expectedOutputCardinality)
   : Operator(expectedOutputCardinality),left(left),right(right),leftReg(leftReg),rightReg(rightReg),result(result)
   // Constructor
{
}
//---------------------------------------------------------------------------
Substraction::~Substraction()
   // Destructor
{
   delete left;
   delete right;
}
//---------------------------------------------------------------------------
void Substraction::buildTable() {
	while ((leftCount=left->next())!=0) {
		leftValue=leftReg->value;
		subs.insert(std::pair<unsigned,unsigned>(leftValue,leftCount));
	}
}
bool Substraction::lookup(unsigned key){
	it=subs.find(key);
	if (it!=subs.end())
		return true;
	else
		return false;
}
//---------------------------------------------------------------------------
unsigned Substraction::first()
   // Produce the first tuple
{
	observedOutputCardinality=0;
	buildTable();

	// Read the first tuple from the right side
    if ((rightCount=right->next())==0)
	   return false;

	// Setup the lookup
    rightValue = rightReg->value;
	found=lookup(rightValue);

	return next();
}
//---------------------------------------------------------------------------
unsigned Substraction::next()
   // Produce the next tuple
{
   while (true) {
	   if (found) {
		   result->value=rightReg->value;
		   observedOutputCardinality+=rightCount;
		   return rightCount;
	   }
	   if ((rightCount=right->next())==0)
		   return false;
	   rightValue = rightReg->value;
	   found=lookup(rightValue);
   }
}
//---------------------------------------------------------------------------
void Substraction::print(PlanPrinter& out)
   // Print the operator tree. Debugging only.
{
   out.beginOperator("Substraction",expectedOutputCardinality,observedOutputCardinality);
   out.addEqualPredicateAnnotation(result,leftReg);
   out.addEqualPredicateAnnotation(result,rightReg);
   left->print(out);
   right->print(out);
   out.endOperator();
}
//---------------------------------------------------------------------------
void Substraction::addMergeHint(Register* reg1,Register* reg2)
   // Add a merge join hint
{
   left->addMergeHint(reg1,reg2);
   right->addMergeHint(reg1,reg2);
}
//---------------------------------------------------------------------------
void Substraction::getAsyncInputCandidates(Scheduler& scheduler)
   // Register parts of the tree that can be executed asynchronous
{
   left->getAsyncInputCandidates(scheduler);
   right->getAsyncInputCandidates(scheduler);
}
//---------------------------------------------------------------------------
