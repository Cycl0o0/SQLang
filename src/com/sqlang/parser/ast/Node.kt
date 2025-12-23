package com.sqlang.parser.ast

import com.sqlang.lexer.Token

// Base interface for all AST nodes
sealed interface Node {
    val line: Int
    val column: Int
}

// ============================================
// TYPES
// ============================================

sealed interface SQLangType : Node {
    data class IntType(override val line: Int, override val column: Int) : SQLangType
    data class DecimalType(override val line: Int, override val column: Int, val precision: Int? = null, val scale: Int? = null) : SQLangType
    data class TextType(override val line: Int, override val column: Int) : SQLangType
    data class BoolType(override val line: Int, override val column: Int) : SQLangType
    data class CharType(override val line: Int, override val column: Int) : SQLangType
    data class VoidType(override val line: Int, override val column: Int) : SQLangType
    data class ArrayType(val elementType: SQLangType, override val line: Int, override val column: Int) : SQLangType
    data class MapType(val keyType: SQLangType, val valueType: SQLangType, override val line: Int, override val column: Int) : SQLangType
    data class TableType(val columns: List<ColumnDef>?, override val line: Int, override val column: Int) : SQLangType
    data class NullableType(val innerType: SQLangType, override val line: Int, override val column: Int) : SQLangType
    data class CustomType(val name: String, override val line: Int, override val column: Int) : SQLangType
}

data class ColumnDef(
    val name: String,
    val type: SQLangType,
    val isPrimaryKey: Boolean = false,
    val isNullable: Boolean = true,
    override val line: Int,
    override val column: Int
) : Node

// ============================================
// EXPRESSIONS
// ============================================

sealed interface Expression : Node

data class LiteralExpr(
    val value: Any?,
    override val line: Int,
    override val column: Int
) : Expression

data class IdentifierExpr(
    val name: String,
    override val line: Int,
    override val column: Int
) : Expression

data class BinaryExpr(
    val left: Expression,
    val operator: Token,
    val right: Expression,
    override val line: Int,
    override val column: Int
) : Expression

data class UnaryExpr(
    val operator: Token,
    val operand: Expression,
    override val line: Int,
    override val column: Int
) : Expression

data class GroupingExpr(
    val expression: Expression,
    override val line: Int,
    override val column: Int
) : Expression

data class CallExpr(
    val callee: String,
    val arguments: List<Expression>,
    override val line: Int,
    override val column: Int
) : Expression

data class IndexExpr(
    val obj: Expression,
    val index: Expression,
    override val line: Int,
    override val column: Int
) : Expression

data class MemberExpr(
    val obj: Expression,
    val member: String,
    override val line: Int,
    override val column: Int
) : Expression

data class ArrayExpr(
    val elements: List<Expression>,
    override val line: Int,
    override val column: Int
) : Expression

data class MapExpr(
    val entries: List<Pair<Expression, Expression>>,
    override val line: Int,
    override val column: Int
) : Expression

data class RangeExpr(
    val start: Expression,
    val end: Expression,
    override val line: Int,
    override val column: Int
) : Expression

data class TernaryExpr(
    val condition: Expression,
    val thenBranch: Expression,
    val elseBranch: Expression,
    override val line: Int,
    override val column: Int
) : Expression

// SQL Expressions
data class SelectExpr(
    val columns: List<SelectColumn>,
    val from: String?,
    val where: Expression?,
    val orderBy: List<OrderByClause>?,
    val limit: Expression?,
    val offset: Expression?,
    val distinct: Boolean = false,
    override val line: Int,
    override val column: Int
) : Expression

data class SelectColumn(
    val expression: Expression,
    val alias: String?,
    override val line: Int,
    override val column: Int
) : Node

data class OrderByClause(
    val expression: Expression,
    val ascending: Boolean,
    override val line: Int,
    override val column: Int
) : Node

// ============================================
// STATEMENTS
// ============================================

sealed interface Statement : Node

data class ProgramStmt(
    val name: String,
    val statements: List<Statement>,
    override val line: Int,
    override val column: Int
) : Statement

data class DeclareStmt(
    val name: String,
    val type: SQLangType?,
    val initializer: Expression?,
    val isConst: Boolean,
    val isAuto: Boolean,
    override val line: Int,
    override val column: Int
) : Statement

data class SetStmt(
    val target: Expression,
    val value: Expression,
    override val line: Int,
    override val column: Int
) : Statement

data class PrintStmt(
    val expression: Expression,
    override val line: Int,
    override val column: Int
) : Statement

data class InputStmt(
    val target: String,
    val prompt: Expression?,
    override val line: Int,
    override val column: Int
) : Statement

data class IfStmt(
    val condition: Expression,
    val thenBranch: List<Statement>,
    val elseIfBranches: List<Pair<Expression, List<Statement>>>,
    val elseBranch: List<Statement>?,
    override val line: Int,
    override val column: Int
) : Statement

data class CaseStmt(
    val subject: Expression,
    val whenBranches: List<Pair<Expression, List<Statement>>>,
    val defaultBranch: List<Statement>?,
    override val line: Int,
    override val column: Int
) : Statement

data class WhileStmt(
    val condition: Expression,
    val body: List<Statement>,
    override val line: Int,
    override val column: Int
) : Statement

data class ForStmt(
    val variable: String,
    val range: Expression,
    val body: List<Statement>,
    override val line: Int,
    override val column: Int
) : Statement

data class ForEachStmt(
    val variable: String,
    val iterable: Expression,
    val body: List<Statement>,
    override val line: Int,
    override val column: Int
) : Statement

data class BreakStmt(
    override val line: Int,
    override val column: Int
) : Statement

data class ContinueStmt(
    override val line: Int,
    override val column: Int
) : Statement

data class ReturnStmt(
    val value: Expression?,
    override val line: Int,
    override val column: Int
) : Statement

data class FunctionStmt(
    val name: String,
    val parameters: List<Parameter>,
    val returnType: SQLangType,
    val body: List<Statement>,
    override val line: Int,
    override val column: Int
) : Statement

data class ProcedureStmt(
    val name: String,
    val parameters: List<Parameter>,
    val body: List<Statement>,
    override val line: Int,
    override val column: Int
) : Statement

data class Parameter(
    val name: String,
    val type: SQLangType,
    val defaultValue: Expression? = null,
    override val line: Int,
    override val column: Int
) : Node

data class ExecStmt(
    val callee: String,
    val arguments: List<Expression>,
    override val line: Int,
    override val column: Int
) : Statement

data class ExpressionStmt(
    val expression: Expression,
    override val line: Int,
    override val column: Int
) : Statement

// SQL Statements
data class CreateTableStmt(
    val name: String,
    val columns: List<ColumnDef>,
    override val line: Int,
    override val column: Int
) : Statement

data class InsertStmt(
    val table: String,
    val columns: List<String>?,
    val values: List<Expression>,
    override val line: Int,
    override val column: Int
) : Statement

data class UpdateTableStmt(
    val table: String,
    val assignments: List<Pair<String, Expression>>,
    val where: Expression?,
    override val line: Int,
    override val column: Int
) : Statement

data class DeleteStmt(
    val table: String,
    val where: Expression?,
    override val line: Int,
    override val column: Int
) : Statement

// Exception handling
data class TryStmt(
    val tryBlock: List<Statement>,
    val catchParam: String?,
    val catchBlock: List<Statement>?,
    val finallyBlock: List<Statement>?,
    override val line: Int,
    override val column: Int
) : Statement

data class ThrowStmt(
    val expression: Expression,
    override val line: Int,
    override val column: Int
) : Statement

// Block statement
data class BlockStmt(
    val statements: List<Statement>,
    override val line: Int,
    override val column: Int
) : Statement