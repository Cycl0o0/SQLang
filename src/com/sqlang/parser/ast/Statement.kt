package com.sqlang.parser.ast

/**
 * All statement types in SQLang.
 * Statements are constructs that perform actions but don't produce values.
 */

// Re-export from Node.kt for backwards compatibility
typealias Stmt = Statement

/**
 * Visitor pattern interface for statements
 */
interface StatementVisitor<R> {
    fun visitProgramStmt(stmt: ProgramStmt): R
    fun visitDeclareStmt(stmt: DeclareStmt): R
    fun visitSetStmt(stmt: SetStmt): R
    fun visitPrintStmt(stmt: PrintStmt): R
    fun visitInputStmt(stmt: InputStmt): R
    fun visitIfStmt(stmt: IfStmt): R
    fun visitCaseStmt(stmt: CaseStmt): R
    fun visitWhileStmt(stmt: WhileStmt): R
    fun visitForStmt(stmt: ForStmt): R
    fun visitForEachStmt(stmt: ForEachStmt): R
    fun visitBreakStmt(stmt: BreakStmt): R
    fun visitContinueStmt(stmt: ContinueStmt): R
    fun visitReturnStmt(stmt: ReturnStmt): R
    fun visitFunctionStmt(stmt: FunctionStmt): R
    fun visitProcedureStmt(stmt: ProcedureStmt): R
    fun visitExecStmt(stmt: ExecStmt): R
    fun visitExpressionStmt(stmt: ExpressionStmt): R
    fun visitCreateTableStmt(stmt: CreateTableStmt): R
    fun visitInsertStmt(stmt: InsertStmt): R
    fun visitUpdateTableStmt(stmt: UpdateTableStmt): R
    fun visitDeleteStmt(stmt: DeleteStmt): R
    fun visitTryStmt(stmt: TryStmt): R
    fun visitThrowStmt(stmt: ThrowStmt): R
    fun visitBlockStmt(stmt: BlockStmt): R
}

/**
 * Extension function to accept a visitor
 */
fun <R> Statement.accept(visitor: StatementVisitor<R>): R {
    return when (this) {
        is ProgramStmt -> visitor.visitProgramStmt(this)
        is DeclareStmt -> visitor.visitDeclareStmt(this)
        is SetStmt -> visitor.visitSetStmt(this)
        is PrintStmt -> visitor.visitPrintStmt(this)
        is InputStmt -> visitor.visitInputStmt(this)
        is IfStmt -> visitor.visitIfStmt(this)
        is CaseStmt -> visitor.visitCaseStmt(this)
        is WhileStmt -> visitor.visitWhileStmt(this)
        is ForStmt -> visitor.visitForStmt(this)
        is ForEachStmt -> visitor.visitForEachStmt(this)
        is BreakStmt -> visitor.visitBreakStmt(this)
        is ContinueStmt -> visitor.visitContinueStmt(this)
        is ReturnStmt -> visitor.visitReturnStmt(this)
        is FunctionStmt -> visitor.visitFunctionStmt(this)
        is ProcedureStmt -> visitor.visitProcedureStmt(this)
        is ExecStmt -> visitor.visitExecStmt(this)
        is ExpressionStmt -> visitor.visitExpressionStmt(this)
        is CreateTableStmt -> visitor.visitCreateTableStmt(this)
        is InsertStmt -> visitor.visitInsertStmt(this)
        is UpdateTableStmt -> visitor.visitUpdateTableStmt(this)
        is DeleteStmt -> visitor.visitDeleteStmt(this)
        is TryStmt -> visitor.visitTryStmt(this)
        is ThrowStmt -> visitor.visitThrowStmt(this)
        is BlockStmt -> visitor.visitBlockStmt(this)
    }
}

/**
 * Utility functions for statements
 */
object StatementUtils {

    /**
     * Check if a statement contains a return statement
     */
    fun containsReturn(stmt: Statement): Boolean {
        return when (stmt) {
            is ReturnStmt -> true
            is IfStmt -> {
                stmt.thenBranch.any { containsReturn(it) } ||
                        stmt.elseIfBranches.any { (_, body) -> body.any { containsReturn(it) } } ||
                        stmt.elseBranch?.any { containsReturn(it) } == true
            }
            is WhileStmt -> stmt.body.any { containsReturn(it) }
            is ForStmt -> stmt.body.any { containsReturn(it) }
            is ForEachStmt -> stmt.body.any { containsReturn(it) }
            is TryStmt -> {
                stmt.tryBlock.any { containsReturn(it) } ||
                        stmt.catchBlock?.any { containsReturn(it) } == true ||
                        stmt.finallyBlock?.any { containsReturn(it) } == true
            }
            is BlockStmt -> stmt.statements.any { containsReturn(it) }
            is ProgramStmt -> stmt.statements.any { containsReturn(it) }
            else -> false
        }
    }

    /**
     * Check if a statement can break out of a loop
     */
    fun containsBreak(stmt: Statement): Boolean {
        return when (stmt) {
            is BreakStmt -> true
            is IfStmt -> {
                stmt.thenBranch.any { containsBreak(it) } ||
                        stmt.elseIfBranches.any { (_, body) -> body.any { containsBreak(it) } } ||
                        stmt.elseBranch?.any { containsBreak(it) } == true
            }
            is TryStmt -> {
                stmt.tryBlock.any { containsBreak(it) } ||
                        stmt.catchBlock?.any { containsBreak(it) } == true
            }
            is BlockStmt -> stmt.statements.any { containsBreak(it) }
            // Note: nested loops have their own break scope
            is WhileStmt, is ForStmt, is ForEachStmt -> false
            else -> false
        }
    }

    /**
     * Get all declared variables in a statement
     */
    fun getDeclaredVariables(stmt: Statement): Set<String> {
        val variables = mutableSetOf<String>()
        collectDeclaredVariables(stmt, variables)
        return variables
    }

    private fun collectDeclaredVariables(stmt: Statement, variables: MutableSet<String>) {
        when (stmt) {
            is DeclareStmt -> variables.add(stmt.name)
            is ForStmt -> {
                variables.add(stmt.variable)
                stmt.body.forEach { collectDeclaredVariables(it, variables) }
            }
            is ForEachStmt -> {
                variables.add(stmt.variable)
                stmt.body.forEach { collectDeclaredVariables(it, variables) }
            }
            is IfStmt -> {
                stmt.thenBranch.forEach { collectDeclaredVariables(it, variables) }
                stmt.elseIfBranches.forEach { (_, body) ->
                    body.forEach { collectDeclaredVariables(it, variables) }
                }
                stmt.elseBranch?.forEach { collectDeclaredVariables(it, variables) }
            }
            is WhileStmt -> stmt.body.forEach { collectDeclaredVariables(it, variables) }
            is TryStmt -> {
                stmt.tryBlock.forEach { collectDeclaredVariables(it, variables) }
                stmt.catchParam?.let { variables.add(it) }
                stmt.catchBlock?.forEach { collectDeclaredVariables(it, variables) }
                stmt.finallyBlock?.forEach { collectDeclaredVariables(it, variables) }
            }
            is BlockStmt -> stmt.statements.forEach { collectDeclaredVariables(it, variables) }
            is ProgramStmt -> stmt.statements.forEach { collectDeclaredVariables(it, variables) }
            is FunctionStmt -> {
                stmt.parameters.forEach { variables.add(it.name) }
                stmt.body.forEach { collectDeclaredVariables(it, variables) }
            }
            is ProcedureStmt -> {
                stmt.parameters.forEach { variables.add(it.name) }
                stmt.body.forEach { collectDeclaredVariables(it, variables) }
            }
            else -> { /* no variables declared */ }
        }
    }

    /**
     * Pretty print a statement for debugging
     */
    fun prettyPrint(stmt: Statement, indent: Int = 0): String {
        val pad = "  ".repeat(indent)
        return when (stmt) {
            is ProgramStmt -> "${pad}PROGRAM ${stmt.name}\n${stmt.statements.joinToString("\n") { prettyPrint(it, indent + 1) }}\n${pad}END PROGRAM"
            is DeclareStmt -> "${pad}DECLARE ${stmt.name} = ${stmt.initializer?.let { ExpressionUtils.prettyPrint(it) } ?: "NULL"}"
            is SetStmt -> "${pad}SET ${ExpressionUtils.prettyPrint(stmt.target)} = ${ExpressionUtils.prettyPrint(stmt.value)}"
            is PrintStmt -> "${pad}PRINT ${ExpressionUtils.prettyPrint(stmt.expression)}"
            is IfStmt -> "${pad}IF ${ExpressionUtils.prettyPrint(stmt.condition)} THEN ..."
            is WhileStmt -> "${pad}WHILE ${ExpressionUtils.prettyPrint(stmt.condition)} DO ..."
            is ForStmt -> "${pad}FOR ${stmt.variable} IN ${ExpressionUtils.prettyPrint(stmt.range)} DO ..."
            is ForEachStmt -> "${pad}FOR EACH ${stmt.variable} IN ${ExpressionUtils.prettyPrint(stmt.iterable)} DO ..."
            is ReturnStmt -> "${pad}RETURN ${stmt.value?.let { ExpressionUtils.prettyPrint(it) } ?: ""}"
            is FunctionStmt -> "${pad}FUNCTION ${stmt.name}(${stmt.parameters.joinToString(", ") { it.name }}) ..."
            is ProcedureStmt -> "${pad}PROCEDURE ${stmt.name}(${stmt.parameters.joinToString(", ") { it.name }}) ..."
            else -> "${pad}${stmt::class.simpleName}"
        }
    }
}