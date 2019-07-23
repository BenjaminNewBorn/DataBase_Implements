package hw3;


import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;

import hw1.Field;
import hw1.RelationalOperator;

public class BPlusTree {
	private int degree;
	private Node root;
    
    public BPlusTree(int degree) {
    	this.degree = degree;
    	 root = new LeafNode(degree);
    }
    
    public LeafNode search(Entry e) {
    	return search(e.getField());
    }
    
    public LeafNode search(Field f) {
    	Node cur = root;
    	while (!cur.isLeafNode()) {
    		ArrayList<Field> curKeys = ((InnerNode)cur).getKeys();
    		ArrayList<Node> children = ((InnerNode)cur).getChildren();
    		int i = 0;
    		for (; i < curKeys.size(); i++) {
    			if (f.compare(RelationalOperator.LTE, curKeys.get(i))) {
    				break;
    			}
    		}
    		cur = children.get(i);
    	}
    	ArrayList<Entry> entries = ((LeafNode)cur).getEntries();
    	for (Entry e : entries) {
    		if (f.compare(RelationalOperator.EQ, e.getField())) {
    			return (LeafNode)cur;
    		}
    	}
    	return null;
    }
    
    public void insert(Entry e) {
    	Field f = e.getField();
    	if(search(f) != null) return ;
    	Node cur = root;
    	Node newNode = insert(e, root, 0, null);
    	root = newNode == null ? root : newNode;
    }
    
    private InnerNode insert(Entry e, Node node, int index, InnerNode pre){
    	Field f = e.getField();
    	if(node.isLeafNode() && !((LeafNode)node).isFull()) {
    		((LeafNode)node).insertEntry(e);
    		return pre;
    	}else if(!node.isLeafNode()) {
    		InnerNode cur = (InnerNode)node; 
    		ArrayList<Field> curKeys = cur.getKeys();
    		ArrayList<Node> children = cur.getChildren();
    		int i = 0;
    		for (; i < curKeys.size(); i++) {
    			if (f.compare(RelationalOperator.LTE, curKeys.get(i))) {
    				break;
    			}
    		}
    		cur = insert(e, children.get(i), i, cur);
    		if(cur.getChildren().size() > (degree + 1)){
    			InnerNode newInner = new InnerNode(this.degree);
    			ArrayList<Node> newChildList = newInner.getChildren();
    			ArrayList<Field> newFieldList = newInner.getKeys();
    			
    			ArrayList<Node> curChildList = cur.getChildren();
    			ArrayList<Field> curFieldList = cur.getKeys();
    			
    			int left = (int)Math.ceil(curChildList.size() / 2.0f);
  			
    			newChildList.addAll(curChildList.subList(left, curChildList.size()));
    			newFieldList.addAll(curFieldList.subList(left, curFieldList.size()));
    			
    			while(left < curChildList.size()) {
    				curChildList.remove(curChildList.size() - 1);
    				curFieldList.remove(curFieldList.size() - 1);
    			}  
    			//insert a new inner node after split
    			if(null == pre) {
        			pre = new InnerNode(degree);
        			root = pre;
        			ArrayList<Node> preChildList = pre.getChildren();
        			preChildList.add(cur);
        			preChildList.add(newInner);
        			ArrayList<Field> preKeyList = pre.getKeys();
        			preKeyList.add(cur.getKey());
        		}else{
        			ArrayList<Node> preChildList = pre.getChildren();
        			ArrayList<Field> preKeyList = pre.getKeys();
        			preChildList.add(index + 1, newInner);
        			preKeyList.set(index, cur.getKey());
        			if(index < preKeyList.size() ){
        				preKeyList.add(index + 1, newInner.getKey());
        			}
        		}
    		}else{
    			if(null != pre && (index < pre.getKeys().size())){
    				pre.getKeys().set(index, cur.getKey());
    			}
    		}
    		return pre;
    	} else{
    		LeafNode newLeaf = new LeafNode(this.degree);
    		LeafNode cur = (LeafNode)node;
    		cur.insertEntry(e);
    		ArrayList<Entry> entryList = cur.getEntries();
    		int left = entryList.size() - entryList.size() / 2;
    		while (left < entryList.size()) {
    			newLeaf.insertEntry(entryList.remove(left));
    		}
    		//set next Leaf Node
    		newLeaf.setNextLeafNode(cur.getNextLeafNode());
    		cur.setNextLeafNode(newLeaf);
    		
    		//insert a new leaf node to parent after split
    		if(null == pre) {
    			pre = new InnerNode(degree);
    			root = pre;
    			ArrayList<Node> children = pre.getChildren();
    			children.add(cur);
    			children.add(newLeaf);
    			ArrayList<Field> keys = pre.getKeys();
    			keys.add(cur.getKey());
    		}else{
    			ArrayList<Node> children = pre.getChildren();
        		children.add(index + 1, newLeaf);
        		ArrayList<Field> keys = pre.getKeys();

        	    keys.set(index, cur.getKey());
        	    if(index < keys.size()){
        	    	keys.add(index + 1, newLeaf.getKey());
        	    }
    		}
    		return pre;
    	}
    }
    
    
    
    
    public void delete(Entry e) {
    	delete(e, root, 0, null);
    	if(!root.isLeafNode()){
    		InnerNode rootInnerNode = (InnerNode) root;
    		if(rootInnerNode.getKeys().size() == 0){
    			root = (Node)rootInnerNode.getChildren().get(0);
    		}
    	}
    }
    
    public void delete(Entry e, Node cur, int index, InnerNode pre) {
    	Field f = e.getField();
    	if(cur.isLeafNode()) {
    		LeafNode toDeleteNode = (LeafNode)cur;
    		if(!toDeleteNode.getEntries().remove(e)){
    			return;
    		}
    		merge(cur, index, pre);
    	}else {
    		ArrayList<Field> curKeys = ((InnerNode)cur).getKeys();
    		ArrayList<Node> children = ((InnerNode)cur).getChildren();
    		int i = 0;
    		for (; i < curKeys.size(); i++) {
    			if (f.compare(RelationalOperator.LTE, curKeys.get(i))) {
    				break;
    			}
    		}
    		delete(e, children.get(i), i, (InnerNode)cur);
    		merge(cur, index, pre);
    	}
    }
    
    private void merge(Node cur, int index, InnerNode pre){
		if(checkBelow(cur)){
			if(borrow(pre, index)){
				return;
			}else{
				merge(pre, index);
			}
		}
		setKey(pre);
    }
    
    private boolean merge(InnerNode node, int ind){
    	return merge(node, ind, ind - 1) || merge(node, ind, ind + 1);
    }
    
    private boolean merge(InnerNode node, int ind, int toMerge){
    	if(node == null){
    		return true;
    	}
    	if(toMerge < 0 || toMerge >= node.getChildren().size()){
    		return false;
    	}
    	Node toMergeNode = node.getChildren().get(toMerge);
    	if(toMergeNode.isLeafNode()){
    		LeafNode toMergeLeafNode = (LeafNode) toMergeNode;
    		LeafNode cur = (LeafNode) node.getChildren().get(ind);
    		if(toMerge > ind){
    			cur.getEntries().addAll(toMergeLeafNode.getEntries());
    			node.getChildren().remove(toMerge);
    		} else {
    			toMergeLeafNode.getEntries().addAll(cur.getEntries());
        		node.getChildren().remove(ind);
    		}
    		setKey(node);
    	} else {
    		InnerNode toMergeInnerNode = (InnerNode) toMergeNode;
    		InnerNode cur = (InnerNode) node.getChildren().get(ind);
    		if(toMerge > ind){
    			cur.getChildren().addAll(toMergeInnerNode.getChildren());
    			node.getChildren().remove(toMerge);
    			setKey(cur);
    		} else {
    			toMergeInnerNode.getChildren().addAll(cur.getChildren());
        		node.getChildren().remove(ind);
        		setKey(toMergeInnerNode);
    		}
    		setKey(node);
    	}
    	return true;
    }
    
    private boolean borrow(InnerNode node, int ind){
    	if(node == null) {
    		return true;
    	}
    	return borrow(node, ind, ind - 1) || borrow(node, ind, ind + 1);
    }
    
    private boolean borrow(InnerNode node, int ind, int toBorrow){
    	if(toBorrow < 0 || toBorrow >= node.getChildren().size()){
    		return false;
    	}
    	Node toBorrowNode = node.getChildren().get(toBorrow);
    	if(toBorrowNode.isLeafNode()){
    		LeafNode toBorrowLeafNode = (LeafNode) toBorrowNode;
    		ArrayList<Entry> entries = toBorrowLeafNode.getEntries();
    		if(entries.size() >= Math.ceil(degree / 2.0d) + 1){
    			LeafNode cur = (LeafNode) node.getChildren().get(ind);
    			if(toBorrow > ind){
    				Entry e = entries.remove(0);
    				cur.getEntries().add(e);
    			}else{
    				Entry e = entries.remove(entries.size() - 1);
        			cur.getEntries().add(0, e);
    			}
    			setKey(node);
    			return true;
    		}
    	}else{
    		InnerNode toBorrowInnerNode = (InnerNode) toBorrowNode;
    		ArrayList<Node> children = toBorrowInnerNode.getChildren();
    		ArrayList<Field> keys = toBorrowInnerNode.getKeys();
    		if(children.size() >= Math.ceil((degree + 1) / 2.0d) + 1){
    			InnerNode cur = (InnerNode) node.getChildren().get(ind);
    			if(toBorrow > ind){
    				Node n = children.remove(0);
    				keys.remove(0);
    				cur.getChildren().add(n);
    			}else{
    				Node n = children.remove(children.size() - 1);
    				keys.remove(keys.size() - 1);
    				cur.getChildren().add(0, n);
    			}
    			setKey(node);
    			return true;
    		}
    	}
    	return false;
    }
    
    private void setKey(InnerNode node){
    	if(node == null) {
    		return;
    	}
    	ArrayList<Node> children = node.getChildren();
		ArrayList<Field> keys = node.getKeys();
		if(keys.size() >= children.size() - 1) {
			for(int i=0; i<children.size() - 1; i++){
				keys.set(i, children.get(i).getKey());
			}
			while(keys.size() > children.size() - 1){
				keys.remove(keys.size() - 1);
			}
		} else {
			int i = 0;
			for(; i < keys.size(); i++) {
				keys.set(i, children.get(i).getKey());
			}
			for(; i < children.size() - 1; i++) {
				keys.add(children.get(i).getKey());
			}
		}
		
    }
    
    private boolean checkBelow (Node node) {
    	
    	if (node.isLeafNode()) {
    		if(node == root){
        		return false;
        	}
    		return Math.ceil(node.getDegree() / 2.0d) > ((LeafNode)node).getEntries().size();
    	}else {
    		InnerNode cur = (InnerNode) node;
    		int childSize = cur.getChildren().size();
    		return Math.ceil((node.getDegree() + 1) / 2.0d) > childSize;
    	}
    }
    
    public Node getRoot() {
    	
    	return root;
    }

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Deque<Node> queue = new ArrayDeque<>();
		queue.offerFirst(root);
		while(!queue.isEmpty()){
			int size = queue.size();
			for(int i=0; i<size; i++){
				Node node = queue.pollLast();
				if(node.isLeafNode()){
					sb.append(((LeafNode) node).toString() + ";");
				}else{
					InnerNode innerNode = (InnerNode) node;
					sb.append(innerNode.toString() + ";");
					for(Node child : innerNode.getChildren()){
						queue.offerFirst(child);
					}
				}
			}
			sb.append("\n");
		}
		return sb.toString();
	}
    
    

	
}
