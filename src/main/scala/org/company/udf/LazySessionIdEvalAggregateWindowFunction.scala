package org.company.udf


import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{AggregateWindowFunction, AttributeReference, Expression, If, IsNull, Literal}
import org.apache.spark.sql.types.{DataType, StringType}

case class LazySessionIdEvalAggregateWindowFunction(evaluateNewSessionIdExpr: Expression) extends AggregateWindowFunction {

  protected val currentSessionId: AttributeReference =
    AttributeReference("currentSessionId", StringType, nullable = true)()

  override def children: Seq[Expression] = Seq(evaluateNewSessionIdExpr)

  override def aggBufferAttributes: Seq[AttributeReference] = currentSessionId :: Nil

  override val initialValues: Seq[Expression] = Literal(null: String) :: Nil
  override val updateExpressions: Seq[Expression] = {
    If(IsNull(currentSessionId),
      evaluateNewSessionIdExpr,
      currentSessionId) :: Nil
  }

  override val evaluateExpression: Expression = aggBufferAttributes.head

  override def dataType: DataType = StringType

  override def nullable: Boolean = false
}

object LazySessionIdEvalAggregateWindowFunction {

  def calculateSession(evaluateSessionId: Column): Column = withExpr {
    LazySessionIdEvalAggregateWindowFunction(evaluateSessionId.expr)
  }

  private def withExpr(expr: Expression): Column = new Column(expr)
}