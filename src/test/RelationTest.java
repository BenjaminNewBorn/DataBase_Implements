package test;

import static org.junit.Assert.*;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import hw2.AggregateOperator;
import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
import hw1.IntField;
import hw1.RelationalOperator;
import hw1.StringField;
import hw1.Tuple;
import hw2.Relation;
//import hw2.RelationalOperator;
import hw1.TupleDesc;
import hw1.Type;

public class RelationTest {

	private HeapFile testhf;
	private TupleDesc testtd;
	private HeapFile ahf;
	private TupleDesc atd;
	private Catalog c;
	private TupleDesc td1;
	private ArrayList<Tuple> testTuples;

	@Before
	public void setup() {
		
		try {
			Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
			Files.copy(new File("testfiles/A.dat.bak").toPath(), new File("testfiles/A.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			System.out.println("unable to copy files");
			e.printStackTrace();
		}
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/test.txt");
		
		int tableId = c.getTableId("test");
		testtd = c.getTupleDesc(tableId);
		testhf = c.getDbFile(tableId);
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/A.txt");
		
		tableId = c.getTableId("A");
		atd = c.getTupleDesc(tableId);
		ahf = c.getDbFile(tableId);
		
		//create a relation [String]a1, [String]a2
		Type[] types = new Type[] {Type.STRING, Type.STRING};
		String[] filednames = {"s1", "s2"};
	    td1 = new TupleDesc(types, filednames);
		testTuples = new ArrayList<>();
		String[] s1= {"a", "a", "c","d", "c","c"};
		String[] s2= {"a", "bc", "hm","brd", "zhu", "wh"};
		for(int i =0; i < 6; i++) {
			Tuple tuple = new Tuple(td1);
			tuple.setField(0, new StringField(s1[i]));
			tuple.setField(1, new StringField(s2[i]));
			testTuples.add(tuple);
		}
		
		
		
	}
	
	@Test
	public void testSelect() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ar = ar.select(0, RelationalOperator.EQ, new IntField(530));
		assert(ar.getTuples().size() == 5);
		assert(ar.getDesc().equals(atd));
	}
	
	@Test
	public void testProject() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);
		assert(ar.getDesc().getSize() == 4);
		assert(ar.getTuples().size() == 8);
		assert(ar.getDesc().getFieldName(0).equals("a2"));
	}
	
	@Test
	public void testJoin() {
		Relation tr = new Relation(testhf.getAllTuples(), testtd);
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		tr = tr.join(ar, 0, 0);
		
		assert(tr.getTuples().size() == 5);
		assert(tr.getDesc().getSize() == 141);
	}
	
	@Test
	public void testRename() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		
		ArrayList<Integer> f = new ArrayList<Integer>();
		ArrayList<String> n = new ArrayList<String>();
		
		f.add(0);
		n.add("b1");
		
		ar = ar.rename(f, n);
		
		assertTrue(ar.getTuples().size() == 8);
		assertTrue(ar.getDesc().getFieldName(0).equals("b1"));
		assertTrue(ar.getDesc().getFieldName(1).equals("a2"));
		assertTrue(ar.getDesc().getSize() == 8);
		
	}
	
	@Test
	public void testAggregate() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);

		Relation sr = new Relation(testTuples, td1);
		
		
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);

		
		//testSumForINT
		ar = ar.aggregate(AggregateOperator.SUM, false);
		assertTrue(ar.getTuples().size() == 1);
		IntField agg = (IntField) ar.getTuples().get(0).getField(0);
		assertTrue(agg.getValue() == 36);

        //testAveForInt 
		ar = new Relation(ahf.getAllTuples(), atd);
		ar = ar.project(c);
		ar = ar.aggregate(AggregateOperator.AVG, false);
		assertTrue(ar.getTuples().size() == 1);
		agg = (IntField) ar.getTuples().get(0).getField(0);
		assertTrue(agg.getValue() == 36 / 8);
		
		//testMaxForINT
		ar = new Relation(ahf.getAllTuples(), atd);
		ar = ar.project(c);
		ar = ar.aggregate(AggregateOperator.MAX, false);
		assertTrue(ar.getTuples().size() == 1);
		agg = (IntField) ar.getTuples().get(0).getField(0);
		assertTrue(agg.getValue() == 8);
		
		//testMinForINT
		ar = new Relation(ahf.getAllTuples(), atd);
		ar = ar.project(c);
		ar = ar.aggregate(AggregateOperator.MIN, false);
		assertTrue(ar.getTuples().size() == 1);
		agg = (IntField) ar.getTuples().get(0).getField(0);
		assertTrue(agg.getValue() == 1);
		
		//testCountForINT
		ar = new Relation(ahf.getAllTuples(), atd);
		ar = ar.project(c);
		ar = ar.aggregate(AggregateOperator.COUNT, false);
		assertTrue(ar.getTuples().size() == 1);
		agg = (IntField) ar.getTuples().get(0).getField(0);
		assertTrue(agg.getValue() == 8);
		
		//test String MAX
//		sr = sr.project(c);
//		sr = sr.aggregate(AggregateOperator.SUM, false);
		
		//test count 
		sr = new Relation(testTuples, td1);
		sr = sr.project(c);
		sr = sr.aggregate(AggregateOperator.COUNT, false);
		assertTrue(sr.getTuples().size() == 1);
		agg = (IntField) sr.getTuples().get(0).getField(0);
		assertTrue(agg.getValue() == 6);
		
		//test MIN 
		sr = new Relation(testTuples, td1);
		sr = sr.project(c);
		sr = sr.aggregate(AggregateOperator.MIN, false);
		assertTrue(sr.getTuples().size() == 1);
		StringField sgg = (StringField) sr.getTuples().get(0).getField(0);
		assertTrue(sgg.getValue().equals("a"));
		System.out.println("test Min String without group successfully");
		
		sr = new Relation(testTuples, td1);
		sr = sr.project(c);
		sr = sr.aggregate(AggregateOperator.MAX, false);
		assertTrue(sr.getTuples().size() == 1);
		sgg = (StringField) sr.getTuples().get(0).getField(0);
		assertTrue(sgg.getValue().equals("zhu"));
		
	}
	
	@Test
	public void testGroupBy() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ar = ar.aggregate(AggregateOperator.SUM, true);
		
		assertTrue(ar.getTuples().size() == 4);
	}

}
