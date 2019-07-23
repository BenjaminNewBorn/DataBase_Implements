package hw2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import hw1.Field;
import hw1.IntField;
import hw1.Tuple;
import hw1.TupleDesc;
import hw1.Type;
import hw1.RelationalOperator;
import hw1.StringField;

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {
	AggregateOperator o;
	boolean groupBy;
	TupleDesc td;
	ArrayList<Tuple> res;
	HashMap<Field, Field> group;
	HashMap<Field, Integer> counter;
	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		//your code here
		this.o = o;
		this.groupBy = groupBy;
		this.td = td;
		res = new ArrayList<>();
		if(groupBy) {
			group = new HashMap<>();
			if(o == AggregateOperator.AVG || o == AggregateOperator.COUNT) {
				counter = new HashMap<>();
			}
		}
		
	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		Field key = t.getField(0);
		
		if(groupBy) {
			Field value = t.getField(1);
			switch(o) {
			case MAX:
				if(group.containsKey(key)) {
					if(group.get(key).compare(RelationalOperator.LT, value)) {
						group.put(key, value);
					}
				}else {
					group.put(t.getField(0), t.getField(1));
				}
				break;
			case MIN: 
				if(group.containsKey(key)) {
					if(group.get(key).compare(RelationalOperator.GT, value)) {
						group.put(key, value);
					}
				}else {
					group.put(t.getField(0), t.getField(1));
				}
				break;
			case AVG: if(td.getType(1) == Type.INT) {
				int curValue = ((IntField)group.getOrDefault(key, new IntField(0))).getValue();
				curValue += ((IntField) value).getValue();
				group.put(key, new IntField(curValue));
				counter.put(key, counter.getOrDefault(key, 0) + 1);
			}
				break;
			
			case COUNT:
				counter.put(key, counter.getOrDefault(key, 0) + 1);
				break;
			case SUM: if(td.getType(1) == Type.INT) {
				int curValue = ((IntField)group.getOrDefault(key, new IntField(0))).getValue();
				curValue += ((IntField) value).getValue();
				group.put(key, new IntField(curValue));
			}
				break;
			}
		}else {
			switch(o) {
			case MAX: 
				if(res.size() == 0) {
					res.add(t);
				}else {
					if(res.get(0).getField(0).compare(RelationalOperator.LT, t.getField(0))) {
						res.set(0, t);
					}
				}
				break;
			case MIN:
				if(res.size() == 0) {
					res.add(t);
				}else {
					if(res.get(0).getField(0).compare(RelationalOperator.GT, t.getField(0))) {
						res.set(0, t);
					}
				}
				break;
			case AVG: if(td.getType(0) == Type.INT) {
				res.add(t);
			}
				break;
			case COUNT:
				if(res.size() == 0) {
					t.setDesc(td);
					t.setField(0, new IntField(1));
					res.add(t);
				}else {
					int i = ((IntField) res.get(0).getField(0)).getValue() + 1;
					res.get(0).setField(0, new IntField(i));
				}
				break;
			case SUM: 
				if(t.getDesc().getType(0).equals(Type.INT)) {
					if(res.size() == 0){
						res.add(t);
					}else{
						int i = ((IntField) res.get(0).getField(0)).getValue() + ((IntField) t.getField(0)).getValue();
						res.get(0).setField(0, new IntField(i));
					}
				}
				
				break;
			}
		}
		
	}
	
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		//your code here
		if(groupBy) {
			if(o == AggregateOperator.MAX || o == AggregateOperator.MIN || o == AggregateOperator.SUM) {
				for(Entry<Field, Field> entry : group.entrySet()) {
					Tuple tuple = new Tuple(td);
					tuple.setField(0, entry.getKey());
					tuple.setField(1, entry.getValue());
					res.add(tuple);
				}
			}else if(o == AggregateOperator.COUNT) {
				for(Entry<Field, Integer> entry : counter.entrySet()) {
					Tuple tuple = new Tuple(td);
					tuple.setField(0, entry.getKey());
					tuple.setField(1, new IntField(entry.getValue()));
					res.add(tuple);
				}
			}else {
				for(Field key : group.keySet()) {
					int value = ((IntField) group.get(key)).getValue() / counter.get(key);
					Tuple tuple = new Tuple(td);
					tuple.setField(0, key);
					tuple.setField(1, new IntField(value));
					res.add(tuple);
				}
			}
		}else {
			if(o == AggregateOperator.AVG) {
				int sum = 0;
				for(Tuple tuple : res) {
					sum += ((IntField) tuple.getField(0)).getValue();
				}
				sum /= res.size();
				Tuple newTuple = new Tuple(res.get(0).getDesc());
				newTuple.setField(0, new IntField(sum));
				ArrayList<Tuple> curRes = new ArrayList<>();
				curRes.add(newTuple);
				return curRes;
			}
		}
		return res;
	}

}
