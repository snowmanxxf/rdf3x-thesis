#ifndef H_rts_operator_Substraction
#define H_rts_operator_Substraction
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
#include "rts/operator/Operator.hpp"
#include <map>
#include <utility>
//---------------------------------------------------------------------------
/// A union that merges two sorted streams
class Substraction : public Operator
{
   private:

   /// The input
   Operator* left,*right;
   /// The input registers
   Register* leftReg,*rightReg;
   /// The result register
   Register* result;
   /// The values
   unsigned leftValue,rightValue;
   /// The counts
   unsigned leftCount,rightCount;


   std::map<unsigned,unsigned> subs;
   std::map<unsigned,unsigned>::iterator it;
   bool found;

   void buildTable();
   bool lookup(unsigned key);


   public:
   /// Constructor
   Substraction(Register* result,Operator* left,Register* leftReg,Operator* right,Register* rightReg,double expectedOutputCardinality);
   /// Destructor
   ~Substraction();

   /// Produce the first tuple
   unsigned first();
   /// Produce the next tuple
   unsigned next();

   /// Print the operator tree. Debugging only.
   void print(PlanPrinter& out);
   /// Add a merge join hint
   void addMergeHint(Register* reg1,Register* reg2);
   /// Register parts of the tree that can be executed asynchronous
   void getAsyncInputCandidates(Scheduler& scheduler);
};
//---------------------------------------------------------------------------
#endif
