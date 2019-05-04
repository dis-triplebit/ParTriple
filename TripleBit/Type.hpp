/*
 * Type.hpp
 *
 *  Created on: 2014年11月25日
 *      Author: wonder
 */

#ifndef TYPE_HPP_
#define TYPE_HPP_

//---------------------------------------------------------------------------
// RDF-3X
// (c) 2009 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
/// Information about the type system
class Type {
   public:
   /// Different literal types
   enum ID {
	   URI, Literal, CustomLanguage, CustomType,String, Integer, Decimal, Double, Boolean,Date
   };
   /// Does the type have a sub-type?
   static inline bool hasSubType(ID t) { return (t==CustomLanguage)||(t==CustomType); }
   /// Get the type of the sub-type
   static inline ID getSubTypeType(ID t) { return (t==CustomLanguage)?Literal:URI; }
   static inline string tostring(ID t) {
	   switch (t)
	   {
	   case Type::URI:
		   return "URI";
	   case Type::Literal:
		   return "Literal";
	   case Type::CustomLanguage:
		   return "CustomLanguage";
	   case Type::CustomType:
		   return "CustomType";
	   case Type::String:
		   return "String";
	   case Type::Integer:
		   return "Integer";
	   case Type::Decimal:
		   return "Decimal";
	   case Type::Double:
		   return "Double";
	   case Type::Boolean:
		   return "Boolean";
	   case Type::Date:
		   return "Date";
	   default:
		   return "unknown";
	   }
   }
};
//---------------------------------------------------------------------------
#endif /* TYPE_HPP_ */
