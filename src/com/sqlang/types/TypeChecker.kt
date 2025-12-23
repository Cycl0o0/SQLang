package com.sqlang.types

import com.sqlang.lexer.TokenType
import com.sqlang.parser.ast.*

/**
 * Static type checker for SQLang.
 * Performs type checking before interpretation to catch errors early.
 */
class TypeChecker {

    private val errors = mutableListOf<TypeError>()
    private val scopes = mutableListOf<MutableMap<String, TypeInfo>>()
    private val functions = mutableMapOf<String, FunctionType>()
    private val procedures = mutableMapOf<String, ProcedureType>()
    private val tables = mutableMapOf<String, TableType>()

    // Current function context for return type checking
    private var currentFunctionReturnType: SQLangType? = null

    data class TypeError(
        val message: String,
        val line: Int,
        val column: Int
    )

    data class TypeInfo(
        val type: SQLangType,
        val isConst: Boolean,
        val isNullable: Boolean
    )

    data class FunctionType(
        val name: String,
        val parameters: List<Pair<String, SQLangType>>,
        val returnType: SQLangType
    )

    data class ProcedureType(
        val name: String,
        val parameters: List<Pair<String, SQLangType>>
    )

    data class TableType(
        val name: String,
        val columns: List<Pair<String, SQLangType>>
    )

    /**
     * Check a list of statements and return any type errors found.
     */
    fun check(statements: List<Statement>): List<TypeError> {
        errors.clear()
        scopes.clear()
        functions.clear()
        procedures.clear()
        tables.clear()

        // Global scope
        pushScope()
        registerBuiltins()

        // First pass: register all functions, procedures, and tables
        for (stmt in statements) {
            registerDeclarations(stmt)
        }

        // Second pass: type check all statements
        for (stmt in statements) {
            checkStatement(stmt)
        }

        popScope()
        return errors
    }

    private fun pushScope() {
        scopes.add(mutableMapOf())
    }

    private fun popScope() {
        if (scopes.isNotEmpty()) {
            scopes.removeLast()
        }
    }

    private fun currentScope(): MutableMap<String, TypeInfo> {
        return scopes.last()
    }

    private fun define(name: String, type: SQLangType, isConst: Boolean = false, isNullable: Boolean = false) {
        currentScope()[name] = TypeInfo(type, isConst, isNullable)
    }

    private fun lookup(name: String): TypeInfo? {
        for (scope in scopes.reversed()) {
            scope[name]?.let { return it }
        }
        return null
    }

    private fun error(message: String, line: Int, column: Int) {
        errors.add(TypeError(message, line, column))
    }

    private fun registerBuiltins() {
        // Built-in functions are handled dynamically
        // We could register their signatures here for better type checking
    }

    private fun registerDeclarations(stmt: Statement) {
        when (stmt) {
            is FunctionStmt -> {
                functions[stmt.name] = FunctionType(
                    stmt.name,
                    stmt.parameters.map { it.name to it.type },
                    stmt.returnType
                )
            }
            is ProcedureStmt -> {
                procedures[stmt.name] = ProcedureType(
                    stmt.name,
                    stmt.parameters.map { it.name to it.type }
                )
            }
            is CreateTableStmt -> {
                tables[stmt.name] = TableType(
                    stmt.name,
                    stmt.columns.map { it.name to it.type }
                )
            }
            is ProgramStmt -> {
                stmt.statements.forEach { registerDeclarations(it) }
            }
            else -> { /* no declarations to register */ }
        }
    }

    // ============================================
    // STATEMENT TYPE CHECKING
    // ============================================

    private fun checkStatement(stmt: Statement) {
        when (stmt) {
            is ProgramStmt -> checkProgramStmt(stmt)
            is DeclareStmt -> checkDeclareStmt(stmt)
            is SetStmt -> checkSetStmt(stmt)
            is PrintStmt -> checkPrintStmt(stmt)
            is InputStmt -> checkInputStmt(stmt)
            is IfStmt -> checkIfStmt(stmt)
            is CaseStmt -> checkCaseStmt(stmt)
            is WhileStmt -> checkWhileStmt(stmt)
            is ForStmt -> checkForStmt(stmt)
            is ForEachStmt -> checkForEachStmt(stmt)
            is BreakStmt -> { /* valid anywhere in a loop */ }
            is ContinueStmt -> { /* valid anywhere in a loop */ }
            is ReturnStmt -> checkReturnStmt(stmt)
            is FunctionStmt -> checkFunctionStmt(stmt)
            is ProcedureStmt -> checkProcedureStmt(stmt)
            is ExecStmt -> checkExecStmt(stmt)
            is ExpressionStmt -> checkExpression(stmt.expression)
            is CreateTableStmt -> checkCreateTableStmt(stmt)
            is InsertStmt -> checkInsertStmt(stmt)
            is UpdateTableStmt -> checkUpdateTableStmt(stmt)
            is DeleteStmt -> checkDeleteStmt(stmt)
            is TryStmt -> checkTryStmt(stmt)
            is ThrowStmt -> checkThrowStmt(stmt)
            is BlockStmt -> checkBlockStmt(stmt)
        }
    }

    private fun checkProgramStmt(stmt: ProgramStmt) {
        pushScope()
        stmt.statements.forEach { checkStatement(it) }
        popScope()
    }

    private fun checkDeclareStmt(stmt: DeclareStmt) {
        val declaredType = stmt.type

        if (stmt.initializer != null) {
            val initType = checkExpression(stmt.initializer)

            if (declaredType != null && initType != null) {
                if (!isAssignable(declaredType, initType)) {
                    error(
                        "Cannot assign ${typeToString(initType)} to ${typeToString(declaredType)}",
                        stmt.line, stmt.column
                    )
                }
            }

            val finalType = declaredType ?: initType ?: SQLangType.VoidType(stmt.line, stmt.column)
            define(stmt.name, finalType, stmt.isConst, isNullableType(finalType))
        } else {
            if (declaredType == null) {
                error("Cannot infer type without initializer", stmt.line, stmt.column)
            } else {
                define(stmt.name, declaredType, stmt.isConst, isNullableType(declaredType))
            }
        }
    }

    private fun checkSetStmt(stmt: SetStmt) {
        val targetType = checkExpression(stmt.target)
        val valueType = checkExpression(stmt.value)

        // Check if target is assignable
        if (stmt.target is IdentifierExpr) {
            val info = lookup(stmt.target.name)
            if (info?.isConst == true) {
                error("Cannot reassign constant '${stmt.target.name}'", stmt.line, stmt.column)
            }
        }

        if (targetType != null && valueType != null) {
            if (!isAssignable(targetType, valueType)) {
                error(
                    "Cannot assign ${typeToString(valueType)} to ${typeToString(targetType)}",
                    stmt.line, stmt.column
                )
            }
        }
    }

    private fun checkPrintStmt(stmt: PrintStmt) {
        checkExpression(stmt.expression)
    }

    private fun checkInputStmt(stmt: InputStmt) {
        stmt.prompt?.let { checkExpression(it) }
        // INPUT creates or updates a variable
    }

    private fun checkIfStmt(stmt: IfStmt) {
        val condType = checkExpression(stmt.condition)
        if (condType != null && condType !is SQLangType.BoolType) {
            // Warning: condition should be boolean
        }

        pushScope()
        stmt.thenBranch.forEach { checkStatement(it) }
        popScope()

        for ((condition, body) in stmt.elseIfBranches) {
            checkExpression(condition)
            pushScope()
            body.forEach { checkStatement(it) }
            popScope()
        }

        stmt.elseBranch?.let { branch ->
            pushScope()
            branch.forEach { checkStatement(it) }
            popScope()
        }
    }

    private fun checkCaseStmt(stmt: CaseStmt) {
        val subjectType = checkExpression(stmt.subject)

        for ((whenExpr, body) in stmt.whenBranches) {
            val whenType = checkExpression(whenExpr)
            if (subjectType != null && whenType != null) {
                if (!isComparable(subjectType, whenType)) {
                    error(
                        "Cannot compare ${typeToString(subjectType)} with ${typeToString(whenType)}",
                        whenExpr.line, whenExpr.column
                    )
                }
            }
            pushScope()
            body.forEach { checkStatement(it) }
            popScope()
        }

        stmt.defaultBranch?.let { branch ->
            pushScope()
            branch.forEach { checkStatement(it) }
            popScope()
        }
    }

    private fun checkWhileStmt(stmt: WhileStmt) {
        checkExpression(stmt.condition)
        pushScope()
        stmt.body.forEach { checkStatement(it) }
        popScope()
    }

    private fun checkForStmt(stmt: ForStmt) {
        val rangeType = checkExpression(stmt.range)

        pushScope()
        // Loop variable is always INT for ranges
        define(stmt.variable, SQLangType.IntType(stmt.line, stmt.column))
        stmt.body.forEach { checkStatement(it) }
        popScope()
    }

    private fun checkForEachStmt(stmt: ForEachStmt) {
        val iterableType = checkExpression(stmt.iterable)

        pushScope()
        // Determine element type from iterable
        val elementType = when (iterableType) {
            is SQLangType.ArrayType -> iterableType.elementType
            is SQLangType.TextType -> SQLangType.CharType(stmt.line, stmt.column)
            is SQLangType.TableType -> SQLangType.MapType(
                SQLangType.TextType(stmt.line, stmt.column),
                SQLangType.VoidType(stmt.line, stmt.column), // Mixed types
                stmt.line, stmt.column
            )
            else -> SQLangType.VoidType(stmt.line, stmt.column)
        }
        define(stmt.variable, elementType)
        stmt.body.forEach { checkStatement(it) }
        popScope()
    }

    private fun checkReturnStmt(stmt: ReturnStmt) {
        val returnType = stmt.value?.let { checkExpression(it) }

        if (currentFunctionReturnType != null) {
            if (stmt.value == null && currentFunctionReturnType !is SQLangType.VoidType) {
                error("Function must return a value", stmt.line, stmt.column)
            } else if (returnType != null && !isAssignable(currentFunctionReturnType!!, returnType)) {
                error(
                    "Cannot return ${typeToString(returnType)}, expected ${typeToString(currentFunctionReturnType!!)}",
                    stmt.line, stmt.column
                )
            }
        }
    }

    private fun checkFunctionStmt(stmt: FunctionStmt) {
        pushScope()

        // Define parameters
        for (param in stmt.parameters) {
            define(param.name, param.type)
        }

        // Set return type context
        val previousReturnType = currentFunctionReturnType
        currentFunctionReturnType = stmt.returnType

        // Check body
        stmt.body.forEach { checkStatement(it) }

        currentFunctionReturnType = previousReturnType
        popScope()
    }

    private fun checkProcedureStmt(stmt: ProcedureStmt) {
        pushScope()

        // Define parameters
        for (param in stmt.parameters) {
            define(param.name, param.type)
        }

        // Procedures have void return type
        val previousReturnType = currentFunctionReturnType
        currentFunctionReturnType = SQLangType.VoidType(stmt.line, stmt.column)

        // Check body
        stmt.body.forEach { checkStatement(it) }

        currentFunctionReturnType = previousReturnType
        popScope()
    }

    private fun checkExecStmt(stmt: ExecStmt) {
        val procType = procedures[stmt.callee]

        if (procType == null) {
            error("Unknown procedure: ${stmt.callee}", stmt.line, stmt.column)
            return
        }

        if (stmt.arguments.size != procType.parameters.size) {
            error(
                "Expected ${procType.parameters.size} arguments, got ${stmt.arguments.size}",
                stmt.line, stmt.column
            )
        }

        for ((i, arg) in stmt.arguments.withIndex()) {
            val argType = checkExpression(arg)
            if (i < procType.parameters.size && argType != null) {
                val paramType = procType.parameters[i].second
                if (!isAssignable(paramType, argType)) {
                    error(
                        "Argument ${i + 1}: cannot pass ${typeToString(argType)} as ${typeToString(paramType)}",
                        arg.line, arg.column
                    )
                }
            }
        }
    }

    private fun checkCreateTableStmt(stmt: CreateTableStmt) {
        // Table is already registered
    }

    private fun checkInsertStmt(stmt: InsertStmt) {
        val tableType = tables[stmt.table]
        if (tableType == null) {
            error("Unknown table: ${stmt.table}", stmt.line, stmt.column)
            return
        }

        if (stmt.values.size != tableType.columns.size) {
            error(
                "Expected ${tableType.columns.size} values, got ${stmt.values.size}",
                stmt.line, stmt.column
            )
        }

        for ((i, value) in stmt.values.withIndex()) {
            checkExpression(value)
            // Could add type checking for each column here
        }
    }

    private fun checkUpdateTableStmt(stmt: UpdateTableStmt) {
        val tableType = tables[stmt.table]
        if (tableType == null) {
            error("Unknown table: ${stmt.table}", stmt.line, stmt.column)
            return
        }

        for ((col, expr) in stmt.assignments) {
            if (tableType.columns.none { it.first == col }) {
                error("Unknown column: $col", stmt.line, stmt.column)
            }
            checkExpression(expr)
        }

        stmt.where?.let { checkExpression(it) }
    }

    private fun checkDeleteStmt(stmt: DeleteStmt) {
        if (!tables.containsKey(stmt.table)) {
            error("Unknown table: ${stmt.table}", stmt.line, stmt.column)
        }
        stmt.where?.let { checkExpression(it) }
    }

    private fun checkTryStmt(stmt: TryStmt) {
        pushScope()
        stmt.tryBlock.forEach { checkStatement(it) }
        popScope()

        stmt.catchBlock?.let { block ->
            pushScope()
            stmt.catchParam?.let { define(it, SQLangType.TextType(stmt.line, stmt.column)) }
            block.forEach { checkStatement(it) }
            popScope()
        }

        stmt.finallyBlock?.let { block ->
            pushScope()
            block.forEach { checkStatement(it) }
            popScope()
        }
    }

    private fun checkThrowStmt(stmt: ThrowStmt) {
        checkExpression(stmt.expression)
    }

    private fun checkBlockStmt(stmt: BlockStmt) {
        pushScope()
        stmt.statements.forEach { checkStatement(it) }
        popScope()
    }

    // ============================================
    // EXPRESSION TYPE CHECKING
    // ============================================

    private fun checkExpression(expr: Expression): SQLangType? {
        return when (expr) {
            is LiteralExpr -> checkLiteralExpr(expr)
            is IdentifierExpr -> checkIdentifierExpr(expr)
            is BinaryExpr -> checkBinaryExpr(expr)
            is UnaryExpr -> checkUnaryExpr(expr)
            is GroupingExpr -> checkExpression(expr.expression)
            is CallExpr -> checkCallExpr(expr)
            is IndexExpr -> checkIndexExpr(expr)
            is MemberExpr -> checkMemberExpr(expr)
            is ArrayExpr -> checkArrayExpr(expr)
            is MapExpr -> checkMapExpr(expr)
            is RangeExpr -> checkRangeExpr(expr)
            is TernaryExpr -> checkTernaryExpr(expr)
            is SelectExpr -> checkSelectExpr(expr)
        }
    }

    private fun checkLiteralExpr(expr: LiteralExpr): SQLangType {
        return when (expr.value) {
            null -> SQLangType.NullableType(SQLangType.VoidType(expr.line, expr.column), expr.line, expr.column)
            is Long -> SQLangType.IntType(expr.line, expr.column)
            is Double -> SQLangType.DecimalType(expr.line, expr.column)
            is String -> SQLangType.TextType(expr.line, expr.column)
            is Char -> SQLangType.CharType(expr.line, expr.column)
            is Boolean -> SQLangType.BoolType(expr.line, expr.column)
            else -> SQLangType.VoidType(expr.line, expr.column)
        }
    }

    private fun checkIdentifierExpr(expr: IdentifierExpr): SQLangType? {
        val info = lookup(expr.name)
        if (info == null) {
            error("Undefined variable: ${expr.name}", expr.line, expr.column)
            return null
        }
        return info.type
    }

    private fun checkBinaryExpr(expr: BinaryExpr): SQLangType? {
        val leftType = checkExpression(expr.left)
        val rightType = checkExpression(expr.right)

        if (leftType == null || rightType == null) return null

        return when (expr.operator.type) {
            TokenType.PLUS, TokenType.MINUS, TokenType.STAR, TokenType.SLASH, TokenType.PERCENT -> {
                if (isNumeric(leftType) && isNumeric(rightType)) {
                    if (leftType is SQLangType.DecimalType || rightType is SQLangType.DecimalType) {
                        SQLangType.DecimalType(expr.line, expr.column)
                    } else {
                        SQLangType.IntType(expr.line, expr.column)
                    }
                } else if (expr.operator.type == TokenType.PLUS &&
                    (leftType is SQLangType.TextType || rightType is SQLangType.TextType)) {
                    SQLangType.TextType(expr.line, expr.column)
                } else {
                    error("Invalid operands for ${expr.operator.lexeme}", expr.line, expr.column)
                    null
                }
            }

            TokenType.PIPE -> SQLangType.TextType(expr.line, expr.column)

            TokenType.EQUAL, TokenType.NOT_EQUAL, TokenType.LESS, TokenType.LESS_EQUAL,
            TokenType.GREATER, TokenType.GREATER_EQUAL, TokenType.LIKE -> {
                SQLangType.BoolType(expr.line, expr.column)
            }

            TokenType.AND, TokenType.OR, TokenType.AMPERSAND -> {
                SQLangType.BoolType(expr.line, expr.column)
            }

            else -> {
                error("Unknown operator: ${expr.operator.lexeme}", expr.line, expr.column)
                null
            }
        }
    }

    private fun checkUnaryExpr(expr: UnaryExpr): SQLangType? {
        val operandType = checkExpression(expr.operand) ?: return null

        return when (expr.operator.type) {
            TokenType.MINUS -> {
                if (isNumeric(operandType)) operandType
                else {
                    error("Cannot negate ${typeToString(operandType)}", expr.line, expr.column)
                    null
                }
            }
            TokenType.NOT, TokenType.BANG -> SQLangType.BoolType(expr.line, expr.column)
            else -> null
        }
    }

    private fun checkCallExpr(expr: CallExpr): SQLangType? {
        val funcType = functions[expr.callee]

        // Check if it's a built-in function
        if (funcType == null) {
            // Built-in functions - return appropriate types
            return when (expr.callee.uppercase()) {
                "LENGTH", "LEN", "SIZE", "COUNT", "FLOOR", "CEIL", "CEILING", "TO_INT", "INT" ->
                    SQLangType.IntType(expr.line, expr.column)
                "UPPER", "LOWER", "TRIM", "SUBSTRING", "SUBSTR", "CONCAT", "REPLACE", "JOIN", "TO_TEXT", "TEXT", "STR", "TYPE", "TYPEOF" ->
                    SQLangType.TextType(expr.line, expr.column)
                "ABS", "ROUND", "SQRT", "POW", "POWER", "RANDOM", "RAND", "TO_DECIMAL", "DECIMAL" ->
                    SQLangType.DecimalType(expr.line, expr.column)
                "CONTAINS", "IS_NULL", "IS_NUMBER", "TO_BOOL", "BOOL" ->
                    SQLangType.BoolType(expr.line, expr.column)
                "SPLIT", "REVERSE", "SORT" ->
                    SQLangType.ArrayType(SQLangType.TextType(expr.line, expr.column), expr.line, expr.column)
                "MIN", "MAX", "COALESCE", "POP" -> {
                    // Return type depends on arguments
                    expr.arguments.firstOrNull()?.let { checkExpression(it) }
                }
                else -> {
                    error("Unknown function: ${expr.callee}", expr.line, expr.column)
                    null
                }
            }
        }

        // User-defined function
        if (expr.arguments.size != funcType.parameters.size) {
            error(
                "Expected ${funcType.parameters.size} arguments, got ${expr.arguments.size}",
                expr.line, expr.column
            )
        }

        for ((i, arg) in expr.arguments.withIndex()) {
            val argType = checkExpression(arg)
            if (i < funcType.parameters.size && argType != null) {
                val paramType = funcType.parameters[i].second
                if (!isAssignable(paramType, argType)) {
                    error(
                        "Argument ${i + 1}: cannot pass ${typeToString(argType)} as ${typeToString(paramType)}",
                        arg.line, arg.column
                    )
                }
            }
        }

        return funcType.returnType
    }

    private fun checkIndexExpr(expr: IndexExpr): SQLangType? {
        val objType = checkExpression(expr.obj)
        val indexType = checkExpression(expr.index)

        return when (objType) {
            is SQLangType.ArrayType -> objType.elementType
            is SQLangType.MapType -> objType.valueType
            is SQLangType.TextType -> SQLangType.CharType(expr.line, expr.column)
            else -> {
                if (objType != null) {
                    error("Cannot index ${typeToString(objType)}", expr.line, expr.column)
                }
                null
            }
        }
    }

    private fun checkMemberExpr(expr: MemberExpr): SQLangType? {
        val objType = checkExpression(expr.obj)

        return when (objType) {
            is SQLangType.MapType -> objType.valueType
            is SQLangType.ArrayType -> when (expr.member) {
                "length", "size", "first", "last" -> SQLangType.IntType(expr.line, expr.column)
                else -> null
            }
            is SQLangType.TextType -> when (expr.member) {
                "length", "size" -> SQLangType.IntType(expr.line, expr.column)
                else -> null
            }
            else -> null
        }
    }

    private fun checkArrayExpr(expr: ArrayExpr): SQLangType {
        val elementTypes = expr.elements.mapNotNull { checkExpression(it) }

        // Use the common type of all elements
        val elementType = if (elementTypes.isEmpty()) {
            SQLangType.VoidType(expr.line, expr.column)
        } else {
            elementTypes.first() // Simplified - should find common supertype
        }

        return SQLangType.ArrayType(elementType, expr.line, expr.column)
    }

    private fun checkMapExpr(expr: MapExpr): SQLangType {
        val keyTypes = expr.entries.mapNotNull { checkExpression(it.first) }
        val valueTypes = expr.entries.mapNotNull { checkExpression(it.second) }

        val keyType = keyTypes.firstOrNull() ?: SQLangType.TextType(expr.line, expr.column)
        val valueType = valueTypes.firstOrNull() ?: SQLangType.VoidType(expr.line, expr.column)

        return SQLangType.MapType(keyType, valueType, expr.line, expr.column)
    }

    private fun checkRangeExpr(expr: RangeExpr): SQLangType {
        val startType = checkExpression(expr.start)
        val endType = checkExpression(expr.end)

        if (startType != null && !isNumeric(startType)) {
            error("Range start must be numeric", expr.start.line, expr.start.column)
        }
        if (endType != null && !isNumeric(endType)) {
            error("Range end must be numeric", expr.end.line, expr.end.column)
        }

        return SQLangType.ArrayType(SQLangType.IntType(expr.line, expr.column), expr.line, expr.column)
    }

    private fun checkTernaryExpr(expr: TernaryExpr): SQLangType? {
        checkExpression(expr.condition)
        val thenType = checkExpression(expr.thenBranch)
        val elseType = checkExpression(expr.elseBranch)

        // Return common type (simplified)
        return thenType ?: elseType
    }

    private fun checkSelectExpr(expr: SelectExpr): SQLangType {
        return SQLangType.TableType(null, expr.line, expr.column)
    }

    // ============================================
    // TYPE UTILITIES
    // ============================================

    private fun isNumeric(type: SQLangType): Boolean {
        return type is SQLangType.IntType || type is SQLangType.DecimalType
    }

    private fun isNullableType(type: SQLangType): Boolean {
        return type is SQLangType.NullableType
    }

    private fun isAssignable(target: SQLangType, source: SQLangType): Boolean {
        // Same type
        if (target::class == source::class) return true

        // Null can be assigned to nullable types
        if (source is SQLangType.NullableType && target is SQLangType.NullableType) return true

        // Int can be promoted to Decimal
        if (target is SQLangType.DecimalType && source is SQLangType.IntType) return true

        // Char can be used as Text
        if (target is SQLangType.TextType && source is SQLangType.CharType) return true

        return false
    }

    private fun isComparable(a: SQLangType, b: SQLangType): Boolean {
        return isAssignable(a, b) || isAssignable(b, a)
    }

    private fun typeToString(type: SQLangType): String {
        return when (type) {
            is SQLangType.IntType -> "INT"
            is SQLangType.DecimalType -> "DECIMAL"
            is SQLangType.TextType -> "TEXT"
            is SQLangType.BoolType -> "BOOL"
            is SQLangType.CharType -> "CHAR"
            is SQLangType.VoidType -> "VOID"
            is SQLangType.ArrayType -> "${typeToString(type.elementType)}[]"
            is SQLangType.MapType -> "MAP<${typeToString(type.keyType)}, ${typeToString(type.valueType)}>"
            is SQLangType.TableType -> "TABLE"
            is SQLangType.NullableType -> "${typeToString(type.innerType)}?"
            is SQLangType.CustomType -> type.name
        }
    }
}