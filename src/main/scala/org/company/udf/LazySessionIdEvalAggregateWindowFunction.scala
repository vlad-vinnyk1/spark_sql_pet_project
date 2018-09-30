package org.company.udf


import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{AggregateWindowFunction, AttributeReference, Expression, If, IsNotNull, Literal}
import org.apache.spark.sql.types.{DataType, StringType}

case class LazySessionIdEvalAggregateWindowFunction(evaluateSessionIdExpr: Expression) extends AggregateWindowFunction {

  protected val currentSessionId: AttributeReference =
    AttributeReference("currentSessionId", StringType, nullable = true)()

  override def children: Seq[Expression] = Seq(evaluateSessionIdExpr)

  //State
  override def aggBufferAttributes: Seq[AttributeReference] = currentSessionId :: Nil

  override val initialValues: Seq[Expression] = Literal(null: String) :: Nil
  override val updateExpressions: Seq[Expression] = {
    If(IsNotNull(currentSessionId),
      currentSessionId,
      evaluateSessionIdExpr) :: Nil
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