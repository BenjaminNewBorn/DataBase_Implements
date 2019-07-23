package hw3;

import java.util.ArrayList;

import hw1.Field;
import hw1.RelationalOperator;

public class LeafNode implements Node {
	private int degree;
	private ArrayList<Entry> entries;
	private LeafNode nextLeafNode;
	
	public LeafNode(int degree) {
		this.degree = degree;
		entries = new ArrayList<Entry>();
	}
	
	public ArrayList<Entry> getEntries() {
		return entries;
	}

	public int getDegree() {
		return this.degree;
	}
	
	public boolean isLeafNode() {
		return true;
	}
	
	public LeafNode getNextLeafNode() {
		return nextLeafNode;
	}
	
	public void setNextLeafNode(LeafNode l){
		this.nextLeafNode = l;
	}
	
	public boolean isFull() {
		return entries.size() == degree; 
	}
	
	public void insertEntry(Entry e) {
		Field f = e.getField();
		if(isEmpty()) {
			entries.add(e);
			return;
		}
		for(int i =0; i < entries.size(); i++) {
			if(f.compare(RelationalOperator.LT, entries.get(i).getField())) {
				entries.add(i, e);
				return;
			}
		}
		entries.add(e);
	}
	
	public boolean isEmpty() {
		return entries.size() == 0;
	}
	
	public Field getKey(){
		return entries.get(entries.size() - 1).getField();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(Entry e : entries){
			sb.append(e.getField().toString() + " ");
		}
		return sb.toString();
	}
	
	

}