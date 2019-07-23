package hw2;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;

/**
 * Processes SELECT clauses using the visitor pattern
 * @author Doug Shook
 *
 */
public class ColumnVisitor extends SelectItemVisitorAdapter {

	private AggregateExpressionVisitor aev;
	private String column;
	private String alias;
	
	public ColumnVisitor() {
		aev = new AggregateExpressionVisitor();
	}
	
	@Override
	public void visit(AllColumns columns) {
		this.column = "*";
	}
	
	@Override
	public void visit(SelectExpressionItem item) {
		item.accept(aev);
		this.column = aev.getColumn();
		if(item.getAlias() != null) this.alias = item.getAlias().getName();
	}
	
	public boolean isAggregate() {
		return aev.isAggregate();
	}
	
	public String getColumn() {
		return this.column;
	}
	
	public AggregateOperator getOp() {
		return this.aev.getOp();
	}
	
	public String getAlias() {
		return this.alias;
	}
	
}
