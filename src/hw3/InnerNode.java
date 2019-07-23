package hw3;

import java.util.ArrayList;

import hw1.Field;

public class InnerNode implements Node {
	private int degree;
	private ArrayList<Node> children;
	private ArrayList<Field> keys;
	
	public InnerNode(int degree) {
		this.degree = degree;
		this.children = new ArrayList<Node>();
		this.keys = new ArrayList<Field>();
	}
		
	
	public ArrayList<Field> getKeys() {
		return keys;
	}
	
	public ArrayList<Node> getChildren() {
		return children;
	}

	public int getDegree() {
		
		return this.degree;
	}
	
	public boolean isLeafNode() {
		return false;
	}
	
	public boolean isFull() {
		return children.size() == (degree + 1);
	}
	public Field getKey(){
		return children.get(children.size() - 1).getKey();
	}


	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(Field f : keys){
			sb.append(f.toString() + " ");
		}
		return sb.toString();
	}
	
	

}