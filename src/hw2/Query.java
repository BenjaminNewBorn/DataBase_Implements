package hw2;

import java.util.ArrayList;
import java.util.List;

import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
import hw1.TupleDesc;
import hw1.WhereExpressionVisitor;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.util.TablesNamesFinder;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

public class Query {

	private String q;
	
	public Query(String q) {
		this.q = q;
	}
	
	public Relation execute()  {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();
		//your code here
		
		
		
//		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
//		List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
		//from
		FromItem fromItem = sb.getFromItem();
		Relation fromRelation = getRelationByName(fromItem.toString());
		TupleDesc desc = fromRelation.getDesc();
		//Join
		
		List<Join> joins =  sb.getJoins();
        if(joins != null) {
            for(Join join : joins) {
                
                //get join table and condition
                Expression joinExpression = join.getOnExpression();
                FromItem joinItem = join.getRightItem();
                Relation joinRelation = getRelationByName(joinItem.toString());
                WhereExpressionVisitor wev = new WhereExpressionVisitor();
                joinExpression.accept(wev);
                
                //join relation
                String[] rightExpression = wev.getRight().toString().split("\\.");
                if (2 == rightExpression.length) {

                    if(joinItem.toString().toLowerCase().equals(rightExpression[0].toLowerCase())) {
                        fromRelation = fromRelation.join(joinRelation, fromRelation.getDesc().nameToId(wev.getLeft()), joinRelation.getDesc().nameToId(rightExpression[1]));
                    }else {
                        fromRelation = fromRelation.join(joinRelation, fromRelation.getDesc().nameToId(rightExpression[1]), joinRelation.getDesc().nameToId(wev.getLeft()));
                    }
                }
                desc = fromRelation.getDesc();
            }
        }
		//where
		Expression expression = sb.getWhere();
		if(expression != null) {
			WhereExpressionVisitor wv = new WhereExpressionVisitor();
			expression.accept(wv);
			fromRelation = fromRelation.select(desc.nameToId(wv.getLeft()), wv.getOp(), wv.getRight());
		}
		
		//select and groupby
		List<SelectItem> nodes = sb.getSelectItems();
		ArrayList<Integer> projects = new ArrayList<>();
		boolean isGroupby = false, isAggregate = false;
		ArrayList<Integer> aliasFields = new ArrayList<>();
		ArrayList<String> aliasNames = new ArrayList<>();
		AggregateOperator o = null;
		for(SelectItem node : nodes) {
			ColumnVisitor cv = new ColumnVisitor();
			node.accept(cv);
			String alias = cv.getAlias();
			if(alias != null) {
				aliasNames.add(alias);
				aliasFields.add(desc.nameToId(cv.getColumn()));
			}
			if(!isAggregate) {
				isAggregate = cv.isAggregate();
			}
			if(isAggregate) {
				isGroupby = nodes.size() == 2;
				o = cv.getOp();
			}
			if(cv.getColumn().equals("*")) {
				for(int i=0; i<desc.numFields(); i++) {
					projects.add(i);
				}
				break;
			}
			projects.add(desc.nameToId(cv.getColumn()));
			
		}
		if(aliasNames.size() > 0) fromRelation = fromRelation.rename(aliasFields, aliasNames);
		fromRelation = fromRelation.project(projects);
		if(isAggregate) {
			fromRelation = fromRelation.aggregate(o, isGroupby);
		}
		return fromRelation;
		
	}
	
	
	
	public Relation getRelationByName(String tableName){
		String fileName = "testfiles/" + tableName + ".txt";
		Catalog c = Database.getCatalog();
		c.loadSchema(fileName);
		int tableId = c.getTableId(tableName);
		HeapFile hf = c.getDbFile(tableId);		
		Relation relation = new Relation(hf.getAllTuples(), hf.getTupleDesc());
		return relation;
	}
}
