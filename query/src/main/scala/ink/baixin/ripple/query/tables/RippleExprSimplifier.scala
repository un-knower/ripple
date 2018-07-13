package ink.baixin.ripple.query.tables

import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexSimplify
import org.apache.calcite.rex.RexUtil.ExprSimplifier
import com.google.common.collect.ImmutableList

class RippleExprSimplifier(
                          simplify: RexSimplify, matchNullability: Boolean
                        ) extends ExprSimplifier(simplify, matchNullability) {

  private def implicitCastType(reduced: RexCall) = {
    val operands = reduced.getOperands

    val left = operands.get(0)
    val right = operands.get(1)

    val leftTypeName = left.getType.getSqlTypeName
    val rightTypeName = right.getType.getSqlTypeName

    if (leftTypeName == SqlTypeName.TIMESTAMP && rightTypeName != SqlTypeName.TIMESTAMP) {
      reduced.clone(
        reduced.getType,
        ImmutableList.of(
          left,
          super.apply(simplify.rexBuilder.makeCast(left.getType, right, matchNullability))
        )
      )
    } else if (leftTypeName == SqlTypeName.DATE && rightTypeName != SqlTypeName.DATE) {
      reduced.clone(
        reduced.getType,
        ImmutableList.of(
          left,
          super.apply(simplify.rexBuilder.makeCast(left.getType, right, matchNullability))
        )
      )
    } else if (leftTypeName != SqlTypeName.TIMESTAMP && rightTypeName == SqlTypeName.TIMESTAMP) {
      reduced.clone(
        reduced.getType,
        ImmutableList.of(
          super.apply(simplify.rexBuilder.makeCast(right.getType, left, matchNullability)),
          right,
        )
      )
    } else if (leftTypeName != SqlTypeName.DATE && rightTypeName == SqlTypeName.DATE) {
      reduced.clone(
        reduced.getType,
        ImmutableList.of(
          super.apply(simplify.rexBuilder.makeCast(right.getType, left, matchNullability)),
          right
        )
      )
    } else {
      reduced
    }
  }

  override def visitCall(call: RexCall) = {
    super.visitCall(call) match {
      case reduced if reduced.isAlwaysTrue || reduced.isAlwaysFalse => reduced
      case reduced: RexCall if reduced.isA(SqlKind.EQUALS) => implicitCastType(reduced)
      case reduced => reduced
    }
  }
}