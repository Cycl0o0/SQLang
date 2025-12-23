package com.sqlang.parser.ast


/**
 * All expression types in SQLang.
 * Expressions are constructs that evaluate to a value.
 */

// Re-export from Node.kt for backwards compatibility
// The actual implementations are in Node.kt to avoid circular dependencies

typealias Expr = Expression

/**
 * Visitor pattern interface for expressions
 */
interface ExpressionVisitor<R> {
    fun visitLiteralExpr(expr: LiteralExpr): R
    fun visitIdentifierExpr(expr: IdentifierExpr): R
    fun visitBinaryExpr(expr: BinaryExpr): R
    fun visitUnaryExpr(expr: UnaryExpr): R
    fun visitGroupingExpr(expr: GroupingExpr): R
    fun visitCallExpr(expr: CallExpr): R
    fun visitIndexExpr(expr: IndexExpr): R
    fun visitMemberExpr(expr: MemberExpr): R
    fun visitArrayExpr(expr: ArrayExpr): R
    fun visitMapExpr(expr: MapExpr): R
    fun visitRangeExpr(expr: RangeExpr): R
    fun visitTernaryExpr(expr: TernaryExpr): R
    fun visitSelectExpr(expr: SelectExpr): R
}

/**
 * Extension function to accept a visitor
 */
fun <R> Expression.accept(visitor: ExpressionVisitor<R>): R {
    return when (this) {
        is LiteralExpr -> visitor.visitLiteralExpr(this)
        is IdentifierExpr -> visitor.visitIdentifierExpr(this)
        is BinaryExpr -> visitor.visitBinaryExpr(this)
        is UnaryExpr -> visitor.visitUnaryExpr(this)
        is GroupingExpr -> visitor.visitGroupingExpr(this)
        is CallExpr -> visitor.visitCallExpr(this)
        is IndexExpr -> visitor.visitIndexExpr(this)
        is MemberExpr -> visitor.visitMemberExpr(this)
        is ArrayExpr -> visitor.visitArrayExpr(this)
        is MapExpr -> visitor.visitMapExpr(this)
        is RangeExpr -> visitor.visitRangeExpr(this)
        is TernaryExpr -> visitor.visitTernaryExpr(this)
        is SelectExpr -> visitor.visitSelectExpr(this)
    }
}

/**
 * Utility functions for expressions
 */
object ExpressionUtils {

    /**
     * Check if an expression is a constant (compile-time evaluable)
     */
    fun isConstant(expr: Expression): Boolean {
        return when (expr) {
            is LiteralExpr -> true
            is UnaryExpr -> isConstant(expr.operand)
            is BinaryExpr -> isConstant(expr.left) && isConstant(expr.right)
            is GroupingExpr -> isConstant(expr.expression)
            is ArrayExpr -> expr.elements.all { isConstant(it) }
            is MapExpr -> expr.entries.all { isConstant(it.first) && isConstant(it.second) }
            is RangeExpr -> isConstant(expr.start) && isConstant(expr.end)
            else -> false
        }
    }

    /**
     * Get all identifiers referenced in an expression
     */
    fun getReferencedIdentifiers(expr: Expression): Set<String> {
        val identifiers = mutableSetOf<String>()
        collectIdentifiers(expr, identifiers)
        return identifiers
    }

    private fun collectIdentifiers(expr: Expression, identifiers: MutableSet<String>) {
        when (expr) {
            is IdentifierExpr -> identifiers.add(expr.name)
            is BinaryExpr -> {
                collectIdentifiers(expr.left, identifiers)
                collectIdentifiers(expr.right, identifiers)
            }
            is UnaryExpr -> collectIdentifiers(expr.operand, identifiers)
            is GroupingExpr -> collectIdentifiers(expr.expression, identifiers)
            is CallExpr -> {
                identifiers.add(expr.callee)
                expr.arguments.forEach { collectIdentifiers(it, identifiers) }
            }
            is IndexExpr -> {
                collectIdentifiers(expr.obj, identifiers)
                collectIdentifiers(expr.index, identifiers)
            }
            is MemberExpr -> collectIdentifiers(expr.obj, identifiers)
            is ArrayExpr -> expr.elements.forEach { collectIdentifiers(it, identifiers) }
            is MapExpr -> expr.entries.forEach { (k, v) ->
                collectIdentifiers(k, identifiers)
                collectIdentifiers(v, identifiers)
            }
            is RangeExpr -> {
                collectIdentifiers(expr.start, identifiers)
                collectIdentifiers(expr.end, identifiers)
            }
            is TernaryExpr -> {
                collectIdentifiers(expr.condition, identifiers)
                collectIdentifiers(expr.thenBranch, identifiers)
                collectIdentifiers(expr.elseBranch, identifiers)
            }
            is SelectExpr -> {
                expr.columns.forEach { collectIdentifiers(it.expression, identifiers) }
                expr.where?.let { collectIdentifiers(it, identifiers) }
            }
            is LiteralExpr -> { /* no identifiers */ }
        }
    }

    /**
     * Pretty print an expression for debugging
     */
    fun prettyPrint(expr: Expression): String {
        return when (expr) {
            is LiteralExpr -> when (val v = expr.value) {
                null -> "NULL"
                is String -> "\"$v\""
                is Char -> "'$v'"
                else -> v.toString()
            }
            is IdentifierExpr -> expr.name
            is BinaryExpr -> "(${prettyPrint(expr.left)} ${expr.operator.lexeme} ${prettyPrint(expr.right)})"
            is UnaryExpr -> "(${expr.operator.lexeme}${prettyPrint(expr.operand)})"
            is GroupingExpr -> "(${prettyPrint(expr.expression)})"
            is CallExpr -> "${expr.callee}(${expr.arguments.joinToString(", ") { prettyPrint(it) }})"
            is IndexExpr -> "${prettyPrint(expr.obj)}[${prettyPrint(expr.index)}]"
            is MemberExpr -> "${prettyPrint(expr.obj)}.${expr.member}"
            is ArrayExpr -> "[${expr.elements.joinToString(", ") { prettyPrint(it) }}]"
            is MapExpr -> "{${expr.entries.joinToString(", ") { "${prettyPrint(it.first)}: ${prettyPrint(it.second)}" }}}"
            is RangeExpr -> "${prettyPrint(expr.start)}..${prettyPrint(expr.end)}"
            is TernaryExpr -> "(${prettyPrint(expr.condition)} ? ${prettyPrint(expr.thenBranch)} : ${prettyPrint(expr.elseBranch)})"
            is SelectExpr -> "SELECT ..."
        }
    }
}