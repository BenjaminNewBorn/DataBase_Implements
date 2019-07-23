package hw2;

import java.util.ArrayList;


import hw1.Field;
import hw1.Tuple;
import hw1.TupleDesc;
import hw1.Type;
import hw1.RelationalOperator;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		//your code here
		this.tuples = l;
		this.td = td;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//your code here
		
		ArrayList<Tuple> res = new ArrayList<>();
		for(Tuple tuple : tuples) {
			Field curField = tuple.getField(field);
			if(curField.compare(op, operand)) {
				res.add(tuple);
			}
		}
		return new Relation(res, td);
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		//your code here
		int n = td.numFields();
		String[] fieldNames = new String[n];
		Type[] types = new Type[n];
		for(int i=0; i<n; i++) {
			fieldNames[i] = td.getFieldName(i);
			types[i] = td.getType(i);
		}
		for(int i=0; i<fields.size(); i++) {
			fieldNames[fields.get(i)] = names.get(i);
		}
		return new Relation(tuples, new TupleDesc(types, fieldNames));
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		//your code here
		int n = fields.size();
		String[] fieldNames = new String[n];
		Type[] types = new Type[n];
		for(int i=0; i<n; i++) {
			int ind = fields.get(i);
			fieldNames[i] = td.getFieldName(ind);
			types[i] = td.getType(ind);
		}
		TupleDesc newDesc = new TupleDesc(types, fieldNames);
		ArrayList<Tuple> newTuples = new ArrayList<>();
		for(Tuple tuple : tuples) {
			ArrayList<Field> newField = new ArrayList<>();
			for(int i : fields) {
				newField.add(tuple.getField(i));
			}
			newTuples.add(createNewTuple(newDesc, newField));
		}
		return new Relation(newTuples, newDesc);
	}
	
	public Tuple createNewTuple(TupleDesc td, ArrayList<Field> fields) {
		Tuple tuple = new Tuple(td);
		for(int i=0; i<td.numFields(); i++) {
			tuple.setField(i, fields.get(i));
		}
		return tuple;
	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		//your code here
        ArrayList<Tuple> res = new ArrayList<>();
        int newLength = td.numFields() + other.getDesc().numFields();
        Type[] types = new Type[newLength];
        String[] fieldNames = new String[newLength];
        for(int i = 0; i < td.numFields(); i++) {
            types[i] = td.getType(i);
            fieldNames[i] = td.getFieldName(i);
        }
        for(int i = td.numFields(); i < newLength; i++) {
            types[i] = other.getDesc().getType(i - td.numFields());
            fieldNames[i] = other.getDesc().getFieldName(i - td.numFields());
        }
        TupleDesc newTd = new TupleDesc(types, fieldNames);
        
        
        for(Tuple tuple : tuples) {
            Field curField = tuple.getField(field1);
            for(Tuple joinTuple : other.getTuples()) {
                Field joinField = joinTuple.getField(field2); {
                    if(curField.compare(RelationalOperator.EQ, joinField)) {
                        ArrayList<Field> newField = new ArrayList<>();
                        for(int i=0; i<tuple.getDesc().numFields(); i++){
                            newField.add(tuple.getField(i));
                        }
                        for(int i=0; i<joinTuple.getDesc().numFields(); i++){
                            newField.add(joinTuple.getField(i));
                        }
                        res.add(createNewTuple(newTd, newField));
                    }
                }
            }
        }
        return new Relation(res, newTd);
	}
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here
		if(op == AggregateOperator.COUNT) {
			if(groupBy) {
				Type[] types = new Type[2];
				String[] fields = new String[2];
				types[0] = td.getType(0);
				types[1] = Type.INT;
				fields[0] = td.getFieldName(0);
				fields[1] = td.getFieldName(1);
				TupleDesc newDesc = new TupleDesc(types, fields);
				this.td = newDesc;
			}else {
				Type[] types = new Type[1];
				String[] fields = new String[1];
				types[0] = Type.INT;
				fields[0] = td.getFieldName(0);
				TupleDesc newDesc = new TupleDesc(types, fields);
				this.td = newDesc;
			}
		}
		Aggregator aggregator = new Aggregator(op, groupBy, td);
		for(Tuple t : tuples) {
			aggregator.merge(t);
		}
		return new Relation(aggregator.getResults(), td);
	}
	
	public TupleDesc getDesc() {
		//your code here
		return td;
	}
	
	public ArrayList<Tuple> getTuples() {
		//your code here
		return tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		String s = "";
		s += td.toString();
		for(Tuple tuple : tuples) {
			s += "\n" + tuple.toString();
		}
		return s;
	}
}
