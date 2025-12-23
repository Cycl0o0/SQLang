package com.sqlang.interpreter

import com.sqlang.lexer.TokenType
import com.sqlang.parser.ast.*
import java.util.Scanner

class Interpreter(
    private val input: Scanner = Scanner(System.`in`),
    private val output: (String) -> Unit = { println(it) }
) {
    private var globalEnv = Environment()

    init {
        registerBuiltins()
    }

    fun interpret(statements: List<Statement>) {
        for (statement in statements) {
            execute(statement, globalEnv)
        }
    }

    fun reset() {
        globalEnv = Environment()
        registerBuiltins()
    }

    private fun registerBuiltins() {
        // Built-in functions will be handled specially in CallExpr
    }

    // ============================================
    // STATEMENT EXECUTION
    // ============================================

    private fun execute(stmt: Statement, env: Environment) {
        when (stmt) {
            is ProgramStmt -> executeProgramStmt(stmt, env)
            is DeclareStmt -> executeDeclareStmt(stmt, env)
            is SetStmt -> executeSetStmt(stmt, env)
            is PrintStmt -> executePrintStmt(stmt, env)
            is InputStmt -> executeInputStmt(stmt, env)
            is IfStmt -> executeIfStmt(stmt, env)
            is CaseStmt -> executeCaseStmt(stmt, env)
            is WhileStmt -> executeWhileStmt(stmt, env)
            is ForStmt -> executeForStmt(stmt, env)
            is ForEachStmt -> executeForEachStmt(stmt, env)
            is BreakStmt -> throw BreakException()
            is ContinueStmt -> throw ContinueException()
            is ReturnStmt -> executeReturnStmt(stmt, env)
            is FunctionStmt -> executeFunctionStmt(stmt, env)
            is ProcedureStmt -> executeProcedureStmt(stmt, env)
            is ExecStmt -> executeExecStmt(stmt, env)
            is CreateTableStmt -> executeCreateTableStmt(stmt, env)
            is InsertStmt -> executeInsertStmt(stmt, env)
            is UpdateTableStmt -> executeUpdateTableStmt(stmt, env)
            is DeleteStmt -> executeDeleteStmt(stmt, env)
            is TryStmt -> executeTryStmt(stmt, env)
            is ThrowStmt -> executeThrowStmt(stmt, env)
            is BlockStmt -> executeBlockStmt(stmt, env)
            is ExpressionStmt -> evaluate(stmt.expression, env)
        }
    }

    private fun executeProgramStmt(stmt: ProgramStmt, env: Environment) {
        val programEnv = env.createChild()
        for (s in stmt.statements) {
            execute(s, programEnv)
        }
    }

    private fun executeDeclareStmt(stmt: DeclareStmt, env: Environment) {
        val value = if (stmt.initializer != null) {
            evaluate(stmt.initializer, env)
        } else {
            getDefaultValue(stmt.type)
        }
        env.define(stmt.name, value, stmt.isConst)
    }

    private fun executeSetStmt(stmt: SetStmt, env: Environment) {
        val value = evaluate(stmt.value, env)

        when (val target = stmt.target) {
            is IdentifierExpr -> env.set(target.name, value)
            is IndexExpr -> {
                val obj = evaluate(target.obj, env)
                val index = evaluate(target.index, env)
                when (obj) {
                    is SQLangValue.ArrayValue -> {
                        val idx = (index as? SQLangValue.IntValue)?.value?.toInt()
                            ?: throw SQLangRuntimeException("Array index must be an integer")
                        obj.set(idx, value)
                    }
                    is SQLangValue.MapValue -> obj.set(index, value)
                    else -> throw SQLangRuntimeException("Cannot index ${obj::class.simpleName}")
                }
            }
            is MemberExpr -> {
                val obj = evaluate(target.obj, env)
                when (obj) {
                    is SQLangValue.MapValue -> obj.set(SQLangValue.TextValue(target.member), value)
                    else -> throw SQLangRuntimeException("Cannot set member on ${obj::class.simpleName}")
                }
            }
            else -> throw SQLangRuntimeException("Invalid assignment target")
        }
    }

    private fun executePrintStmt(stmt: PrintStmt, env: Environment) {
        val value = evaluate(stmt.expression, env)
        output(value.toDisplayString())
    }

    private fun executeInputStmt(stmt: InputStmt, env: Environment) {
        if (stmt.prompt != null) {
            val prompt = evaluate(stmt.prompt, env)
            print(prompt.toDisplayString())
        }

        val line = input.nextLine()

        // Try to infer type from input
        val value: SQLangValue = when {
            line.equals("true", ignoreCase = true) -> SQLangValue.BoolValue(true)
            line.equals("false", ignoreCase = true) -> SQLangValue.BoolValue(false)
            line.equals("null", ignoreCase = true) -> SQLangValue.NullValue
            line.toLongOrNull() != null -> SQLangValue.IntValue(line.toLong())
            line.toDoubleOrNull() != null -> SQLangValue.DecimalValue(line.toDouble())
            else -> SQLangValue.TextValue(line)
        }

        if (env.isDefined(stmt.target)) {
            env.set(stmt.target, value)
        } else {
            env.define(stmt.target, value)
        }
    }

    private fun executeIfStmt(stmt: IfStmt, env: Environment) {
        val condition = evaluate(stmt.condition, env)

        if (condition.toBoolean()) {
            val ifEnv = env.createChild()
            for (s in stmt.thenBranch) {
                execute(s, ifEnv)
            }
            return
        }

        for ((elseIfCondition, elseIfBody) in stmt.elseIfBranches) {
            val elseIfResult = evaluate(elseIfCondition, env)
            if (elseIfResult.toBoolean()) {
                val elseIfEnv = env.createChild()
                for (s in elseIfBody) {
                    execute(s, elseIfEnv)
                }
                return
            }
        }

        if (stmt.elseBranch != null) {
            val elseEnv = env.createChild()
            for (s in stmt.elseBranch) {
                execute(s, elseEnv)
            }
        }
    }

    private fun executeCaseStmt(stmt: CaseStmt, env: Environment) {
        val subject = evaluate(stmt.subject, env)

        for ((whenExpr, whenBody) in stmt.whenBranches) {
            val whenValue = evaluate(whenExpr, env)
            if (valuesEqual(subject, whenValue)) {
                val whenEnv = env.createChild()
                for (s in whenBody) {
                    execute(s, whenEnv)
                }
                return
            }
        }

        if (stmt.defaultBranch != null) {
            val defaultEnv = env.createChild()
            for (s in stmt.defaultBranch) {
                execute(s, defaultEnv)
            }
        }
    }

    private fun executeWhileStmt(stmt: WhileStmt, env: Environment) {
        while (evaluate(stmt.condition, env).toBoolean()) {
            val loopEnv = env.createChild()
            try {
                for (s in stmt.body) {
                    execute(s, loopEnv)
                }
            } catch (_: BreakException) {
                break
            } catch (_: ContinueException) {
                continue
            }
        }
    }

    private fun executeForStmt(stmt: ForStmt, env: Environment) {
        val rangeValue = evaluate(stmt.range, env)

        val range = when (rangeValue) {
            is SQLangValue.RangeValue -> rangeValue.toList()
            is SQLangValue.ArrayValue -> rangeValue.elements.mapIndexed { i, _ -> i.toLong() }
            else -> throw SQLangRuntimeException("FOR loop requires a range (e.g., 1..10)")
        }

        for (i in range) {
            val loopEnv = env.createChild()
            loopEnv.define(stmt.variable, SQLangValue.IntValue(i))

            try {
                for (s in stmt.body) {
                    execute(s, loopEnv)
                }
            } catch (_: BreakException) {
                break
            } catch (_: ContinueException) {
                continue
            }
        }
    }

    private fun executeForEachStmt(stmt: ForEachStmt, env: Environment) {
        val iterable = evaluate(stmt.iterable, env)

        val elements: List<SQLangValue> = when (iterable) {
            is SQLangValue.ArrayValue -> iterable.elements
            is SQLangValue.RangeValue -> iterable.toList().map { SQLangValue.IntValue(it) }
            is SQLangValue.TextValue -> iterable.value.map { SQLangValue.CharValue(it) }
            is SQLangValue.TableValue -> iterable.rows.map { row ->
                SQLangValue.MapValue(
                    iterable.columns.zip(row).associate { (col, value) ->
                        SQLangValue.TextValue(col) to value
                    }.toMutableMap()
                )
            }
            else -> throw SQLangRuntimeException("Cannot iterate over ${iterable::class.simpleName}")
        }

        for (element in elements) {
            val loopEnv = env.createChild()
            loopEnv.define(stmt.variable, element)

            try {
                for (s in stmt.body) {
                    execute(s, loopEnv)
                }
            } catch (_: BreakException) {
                break
            } catch (_: ContinueException) {
                continue
            }
        }
    }

    private fun executeReturnStmt(stmt: ReturnStmt, env: Environment) {
        val value = if (stmt.value != null) evaluate(stmt.value, env) else null
        throw ReturnException(value)
    }

    private fun executeFunctionStmt(stmt: FunctionStmt, env: Environment) {
        val function = SQLangValue.FunctionValue(
            stmt.name,
            stmt.parameters.map { it.name },
            stmt.parameters.map { it.type },
            stmt.body,
            env
        )
        env.define(stmt.name, function)
    }

    private fun executeProcedureStmt(stmt: ProcedureStmt, env: Environment) {
        val procedure = SQLangValue.ProcedureValue(
            stmt.name,
            stmt.parameters.map { it.name },
            stmt.parameters.map { it.type },
            stmt.body,
            env
        )
        env.define(stmt.name, procedure)
    }

    private fun executeExecStmt(stmt: ExecStmt, env: Environment) {
        val callee = env.get(stmt.callee)

        if (callee !is SQLangValue.ProcedureValue) {
            throw SQLangRuntimeException("'${stmt.callee}' is not a procedure")
        }

        if (stmt.arguments.size != callee.parameters.size) {
            throw SQLangRuntimeException("Expected ${callee.parameters.size} arguments, got ${stmt.arguments.size}")
        }

        val procEnv = callee.closure.createChild()
        for (i in callee.parameters.indices) {
            val argValue = evaluate(stmt.arguments[i], env)
            procEnv.define(callee.parameters[i], argValue)
        }

        try {
            for (s in callee.body) {
                execute(s, procEnv)
            }
        } catch (_: ReturnException) {
            // Procedures can return early but shouldn't return a value
        }
    }

    private fun executeCreateTableStmt(stmt: CreateTableStmt, env: Environment) {
        val table = SQLangValue.TableValue(
            stmt.columns.map { it.name },
            mutableListOf()
        )
        env.defineTable(stmt.name, table)
        env.define(stmt.name, table)
    }

    private fun executeInsertStmt(stmt: InsertStmt, env: Environment) {
        val table = env.getTable(stmt.table)
        val values = stmt.values.map { evaluate(it, env) }
        table.insert(values)
    }

    private fun executeUpdateTableStmt(stmt: UpdateTableStmt, env: Environment) {
        val table = env.getTable(stmt.table)

        for (row in table.rows) {
            // Check WHERE condition
            if (stmt.where != null) {
                val rowEnv = env.createChild()
                for (i in table.columns.indices) {
                    rowEnv.define(table.columns[i], row[i])
                }
                if (!evaluate(stmt.where, rowEnv).toBoolean()) {
                    continue
                }
            }

            // Apply updates
            for ((col, expr) in stmt.assignments) {
                val colIndex = table.getColumnIndex(col)
                val rowEnv = env.createChild()
                for (i in table.columns.indices) {
                    rowEnv.define(table.columns[i], row[i])
                }
                row[colIndex] = evaluate(expr, rowEnv)
            }
        }
    }

    private fun executeDeleteStmt(stmt: DeleteStmt, env: Environment) {
        val table = env.getTable(stmt.table)

        if (stmt.where == null) {
            table.rows.clear()
        } else {
            table.rows.removeIf { row ->
                val rowEnv = env.createChild()
                for (i in table.columns.indices) {
                    rowEnv.define(table.columns[i], row[i])
                }
                evaluate(stmt.where, rowEnv).toBoolean()
            }
        }
    }

    private fun executeTryStmt(stmt: TryStmt, env: Environment) {
        try {
            val tryEnv = env.createChild()
            for (s in stmt.tryBlock) {
                execute(s, tryEnv)
            }
        } catch (e: ThrowException) {
            if (stmt.catchBlock != null) {
                val catchEnv = env.createChild()
                if (stmt.catchParam != null) {
                    catchEnv.define(stmt.catchParam, e.value)
                }
                for (s in stmt.catchBlock) {
                    execute(s, catchEnv)
                }
            }
        } finally {
            if (stmt.finallyBlock != null) {
                val finallyEnv = env.createChild()
                for (s in stmt.finallyBlock) {
                    execute(s, finallyEnv)
                }
            }
        }
    }

    private fun executeThrowStmt(stmt: ThrowStmt, env: Environment) {
        val value = evaluate(stmt.expression, env)
        throw ThrowException(value)
    }

    private fun executeBlockStmt(stmt: BlockStmt, env: Environment) {
        val blockEnv = env.createChild()
        for (s in stmt.statements) {
            execute(s, blockEnv)
        }
    }

    // ============================================
    // EXPRESSION EVALUATION
    // ============================================

    private fun evaluate(expr: Expression, env: Environment): SQLangValue {
        return when (expr) {
            is LiteralExpr -> evaluateLiteral(expr)
            is IdentifierExpr -> env.get(expr.name)
            is BinaryExpr -> evaluateBinary(expr, env)
            is UnaryExpr -> evaluateUnary(expr, env)
            is GroupingExpr -> evaluate(expr.expression, env)
            is CallExpr -> evaluateCall(expr, env)
            is IndexExpr -> evaluateIndex(expr, env)
            is MemberExpr -> evaluateMember(expr, env)
            is ArrayExpr -> evaluateArray(expr, env)
            is MapExpr -> evaluateMap(expr, env)
            is RangeExpr -> evaluateRange(expr, env)
            is TernaryExpr -> evaluateTernary(expr, env)
            is SelectExpr -> evaluateSelect(expr, env)
        }
    }

    private fun evaluateLiteral(expr: LiteralExpr): SQLangValue {
        return when (val value = expr.value) {
            null -> SQLangValue.NullValue
            is Long -> SQLangValue.IntValue(value)
            is Double -> SQLangValue.DecimalValue(value)
            is String -> SQLangValue.TextValue(value)
            is Char -> SQLangValue.CharValue(value)
            is Boolean -> SQLangValue.BoolValue(value)
            else -> throw SQLangRuntimeException("Unknown literal type: ${value::class.simpleName}")
        }
    }

    private fun evaluateBinary(expr: BinaryExpr, env: Environment): SQLangValue {
        // Short-circuit evaluation for AND/OR
        if (expr.operator.type == TokenType.AND || expr.operator.type == TokenType.AMPERSAND) {
            val left = evaluate(expr.left, env)
            if (!left.toBoolean()) return SQLangValue.BoolValue(false)
            return SQLangValue.BoolValue(evaluate(expr.right, env).toBoolean())
        }

        if (expr.operator.type == TokenType.OR) {
            val left = evaluate(expr.left, env)
            if (left.toBoolean()) return SQLangValue.BoolValue(true)
            return SQLangValue.BoolValue(evaluate(expr.right, env).toBoolean())
        }

        val left = evaluate(expr.left, env)
        val right = evaluate(expr.right, env)

        return when (expr.operator.type) {
            // Arithmetic
            TokenType.PLUS -> when {
                left is SQLangValue.IntValue && right is SQLangValue.IntValue ->
                    SQLangValue.IntValue(left.value + right.value)
                left is SQLangValue.DecimalValue || right is SQLangValue.DecimalValue ->
                    SQLangValue.DecimalValue(toDouble(left) + toDouble(right))
                left is SQLangValue.TextValue || right is SQLangValue.TextValue ->
                    SQLangValue.TextValue(left.toDisplayString() + right.toDisplayString())
                else -> throw SQLangRuntimeException("Cannot add ${left::class.simpleName} and ${right::class.simpleName}")
            }

            TokenType.MINUS -> when {
                left is SQLangValue.IntValue && right is SQLangValue.IntValue ->
                    SQLangValue.IntValue(left.value - right.value)
                else -> SQLangValue.DecimalValue(toDouble(left) - toDouble(right))
            }

            TokenType.STAR -> when {
                left is SQLangValue.IntValue && right is SQLangValue.IntValue ->
                    SQLangValue.IntValue(left.value * right.value)
                else -> SQLangValue.DecimalValue(toDouble(left) * toDouble(right))
            }

            TokenType.SLASH -> SQLangValue.DecimalValue(toDouble(left) / toDouble(right))

            TokenType.PERCENT -> when {
                left is SQLangValue.IntValue && right is SQLangValue.IntValue ->
                    SQLangValue.IntValue(left.value % right.value)
                else -> SQLangValue.DecimalValue(toDouble(left) % toDouble(right))
            }

            // String concatenation
            TokenType.PIPE -> SQLangValue.TextValue(left.toDisplayString() + right.toDisplayString())

            // Comparison
            TokenType.EQUAL -> SQLangValue.BoolValue(valuesEqual(left, right))
            TokenType.NOT_EQUAL -> SQLangValue.BoolValue(!valuesEqual(left, right))

            TokenType.LESS -> SQLangValue.BoolValue(compareValues(left, right) < 0)
            TokenType.LESS_EQUAL -> SQLangValue.BoolValue(compareValues(left, right) <= 0)
            TokenType.GREATER -> SQLangValue.BoolValue(compareValues(left, right) > 0)
            TokenType.GREATER_EQUAL -> SQLangValue.BoolValue(compareValues(left, right) >= 0)

            TokenType.LIKE -> {
                val text = (left as? SQLangValue.TextValue)?.value
                    ?: throw SQLangRuntimeException("LIKE requires TEXT on left side")
                val pattern = (right as? SQLangValue.TextValue)?.value
                    ?: throw SQLangRuntimeException("LIKE requires TEXT pattern")

                // Convert SQL LIKE pattern to regex
                val regex = pattern
                    .replace("%", ".*")
                    .replace("_", ".")
                    .let { Regex("^$it$", RegexOption.IGNORE_CASE) }

                SQLangValue.BoolValue(regex.matches(text))
            }

            else -> throw SQLangRuntimeException("Unknown operator: ${expr.operator.lexeme}")
        }
    }

    private fun evaluateUnary(expr: UnaryExpr, env: Environment): SQLangValue {
        val operand = evaluate(expr.operand, env)

        return when (expr.operator.type) {
            TokenType.MINUS -> when (operand) {
                is SQLangValue.IntValue -> SQLangValue.IntValue(-operand.value)
                is SQLangValue.DecimalValue -> SQLangValue.DecimalValue(-operand.value)
                else -> throw SQLangRuntimeException("Cannot negate ${operand::class.simpleName}")
            }
            TokenType.NOT, TokenType.BANG -> SQLangValue.BoolValue(!operand.toBoolean())
            else -> throw SQLangRuntimeException("Unknown unary operator: ${expr.operator.lexeme}")
        }
    }

    private fun evaluateCall(expr: CallExpr, env: Environment): SQLangValue {
        // Check for built-in functions first
        val builtin = evaluateBuiltin(expr.callee, expr.arguments, env)
        if (builtin != null) return builtin

        val callee = env.get(expr.callee)

        return when (callee) {
            is SQLangValue.FunctionValue -> {
                if (expr.arguments.size != callee.parameters.size) {
                    throw SQLangRuntimeException("Expected ${callee.parameters.size} arguments, got ${expr.arguments.size}")
                }

                val funcEnv = callee.closure.createChild()
                for (i in callee.parameters.indices) {
                    val argValue = evaluate(expr.arguments[i], env)
                    funcEnv.define(callee.parameters[i], argValue)
                }

                try {
                    for (s in callee.body) {
                        execute(s, funcEnv)
                    }
                    SQLangValue.NullValue
                } catch (e: ReturnException) {
                    e.value ?: SQLangValue.NullValue
                }
            }
            is SQLangValue.ProcedureValue -> {
                throw SQLangRuntimeException("Use EXEC to call procedures, not CALL")
            }
            else -> throw SQLangRuntimeException("'${expr.callee}' is not callable")
        }
    }

    private fun evaluateBuiltin(name: String, args: List<Expression>, env: Environment): SQLangValue? {
        return when (name.uppercase()) {
            // String functions
            "LENGTH", "LEN" -> {
                require(args.size == 1) { "LENGTH requires 1 argument" }
                val str = evaluate(args[0], env)
                when (str) {
                    is SQLangValue.TextValue -> SQLangValue.IntValue(str.value.length.toLong())
                    is SQLangValue.ArrayValue -> SQLangValue.IntValue(str.elements.size.toLong())
                    else -> throw SQLangRuntimeException("LENGTH requires TEXT or ARRAY")
                }
            }

            "UPPER", "UPPERCASE" -> {
                require(args.size == 1) { "UPPER requires 1 argument" }
                val str = (evaluate(args[0], env) as? SQLangValue.TextValue)?.value
                    ?: throw SQLangRuntimeException("UPPER requires TEXT")
                SQLangValue.TextValue(str.uppercase())
            }

            "LOWER", "LOWERCASE" -> {
                require(args.size == 1) { "LOWER requires 1 argument" }
                val str = (evaluate(args[0], env) as? SQLangValue.TextValue)?.value
                    ?: throw SQLangRuntimeException("LOWER requires TEXT")
                SQLangValue.TextValue(str.lowercase())
            }

            "TRIM" -> {
                require(args.size == 1) { "TRIM requires 1 argument" }
                val str = (evaluate(args[0], env) as? SQLangValue.TextValue)?.value
                    ?: throw SQLangRuntimeException("TRIM requires TEXT")
                SQLangValue.TextValue(str.trim())
            }

            "SUBSTRING", "SUBSTR" -> {
                require(args.size in 2..3) { "SUBSTRING requires 2 or 3 arguments" }
                val str = (evaluate(args[0], env) as? SQLangValue.TextValue)?.value
                    ?: throw SQLangRuntimeException("SUBSTRING requires TEXT")
                val start = (evaluate(args[1], env) as? SQLangValue.IntValue)?.value?.toInt()
                    ?: throw SQLangRuntimeException("SUBSTRING start must be INT")
                val length = if (args.size == 3) {
                    (evaluate(args[2], env) as? SQLangValue.IntValue)?.value?.toInt()
                        ?: throw SQLangRuntimeException("SUBSTRING length must be INT")
                } else null

                val endIndex = if (length != null) minOf(start + length, str.length) else str.length
                SQLangValue.TextValue(str.substring(start, endIndex))
            }

            "CONCAT" -> {
                val result = args.joinToString("") { evaluate(it, env).toDisplayString() }
                SQLangValue.TextValue(result)
            }

            "REPLACE" -> {
                require(args.size == 3) { "REPLACE requires 3 arguments" }
                val str = (evaluate(args[0], env) as? SQLangValue.TextValue)?.value
                    ?: throw SQLangRuntimeException("REPLACE requires TEXT")
                val old = (evaluate(args[1], env) as? SQLangValue.TextValue)?.value
                    ?: throw SQLangRuntimeException("REPLACE old value must be TEXT")
                val new = (evaluate(args[2], env) as? SQLangValue.TextValue)?.value
                    ?: throw SQLangRuntimeException("REPLACE new value must be TEXT")
                SQLangValue.TextValue(str.replace(old, new))
            }

            "SPLIT" -> {
                require(args.size == 2) { "SPLIT requires 2 arguments" }
                val str = (evaluate(args[0], env) as? SQLangValue.TextValue)?.value
                    ?: throw SQLangRuntimeException("SPLIT requires TEXT")
                val delimiter = (evaluate(args[1], env) as? SQLangValue.TextValue)?.value
                    ?: throw SQLangRuntimeException("SPLIT delimiter must be TEXT")
                SQLangValue.ArrayValue(str.split(delimiter).map { SQLangValue.TextValue(it) }.toMutableList())
            }

            "JOIN" -> {
                require(args.size == 2) { "JOIN requires 2 arguments" }
                val arr = (evaluate(args[0], env) as? SQLangValue.ArrayValue)?.elements
                    ?: throw SQLangRuntimeException("JOIN requires ARRAY")
                val delimiter = (evaluate(args[1], env) as? SQLangValue.TextValue)?.value
                    ?: throw SQLangRuntimeException("JOIN delimiter must be TEXT")
                SQLangValue.TextValue(arr.joinToString(delimiter) { it.toDisplayString() })
            }

            // Math functions
            "ABS" -> {
                require(args.size == 1) { "ABS requires 1 argument" }
                when (val num = evaluate(args[0], env)) {
                    is SQLangValue.IntValue -> SQLangValue.IntValue(kotlin.math.abs(num.value))
                    is SQLangValue.DecimalValue -> SQLangValue.DecimalValue(kotlin.math.abs(num.value))
                    else -> throw SQLangRuntimeException("ABS requires numeric value")
                }
            }

            "FLOOR" -> {
                require(args.size == 1) { "FLOOR requires 1 argument" }
                val num = toDouble(evaluate(args[0], env))
                SQLangValue.IntValue(kotlin.math.floor(num).toLong())
            }

            "CEIL", "CEILING" -> {
                require(args.size == 1) { "CEIL requires 1 argument" }
                val num = toDouble(evaluate(args[0], env))
                SQLangValue.IntValue(kotlin.math.ceil(num).toLong())
            }

            "ROUND" -> {
                require(args.size in 1..2) { "ROUND requires 1 or 2 arguments" }
                val num = toDouble(evaluate(args[0], env))
                val decimals = if (args.size == 2) {
                    (evaluate(args[1], env) as? SQLangValue.IntValue)?.value?.toInt() ?: 0
                } else 0

                val factor = Math.pow(10.0, decimals.toDouble())
                SQLangValue.DecimalValue(kotlin.math.round(num * factor) / factor)
            }

            "SQRT" -> {
                require(args.size == 1) { "SQRT requires 1 argument" }
                val num = toDouble(evaluate(args[0], env))
                SQLangValue.DecimalValue(kotlin.math.sqrt(num))
            }

            "POW", "POWER" -> {
                require(args.size == 2) { "POW requires 2 arguments" }
                val base = toDouble(evaluate(args[0], env))
                val exp = toDouble(evaluate(args[1], env))
                SQLangValue.DecimalValue(Math.pow(base, exp))
            }

            "MIN" -> {
                require(args.isNotEmpty()) { "MIN requires at least 1 argument" }
                val values = args.map { evaluate(it, env) }
                values.reduce { a, b -> if (compareValues(a, b) < 0) a else b }
            }

            "MAX" -> {
                require(args.isNotEmpty()) { "MAX requires at least 1 argument" }
                val values = args.map { evaluate(it, env) }
                values.reduce { a, b -> if (compareValues(a, b) > 0) a else b }
            }

            "RANDOM", "RAND" -> {
                require(args.isEmpty() || args.size == 2) { "RANDOM requires 0 or 2 arguments" }
                if (args.isEmpty()) {
                    SQLangValue.DecimalValue(Math.random())
                } else {
                    val min = (evaluate(args[0], env) as? SQLangValue.IntValue)?.value
                        ?: throw SQLangRuntimeException("RANDOM min must be INT")
                    val max = (evaluate(args[1], env) as? SQLangValue.IntValue)?.value
                        ?: throw SQLangRuntimeException("RANDOM max must be INT")
                    SQLangValue.IntValue((min..max).random())
                }
            }

            // Array functions
            "SIZE", "COUNT" -> {
                require(args.size == 1) { "SIZE requires 1 argument" }
                when (val coll = evaluate(args[0], env)) {
                    is SQLangValue.ArrayValue -> SQLangValue.IntValue(coll.elements.size.toLong())
                    is SQLangValue.MapValue -> SQLangValue.IntValue(coll.entries.size.toLong())
                    is SQLangValue.TableValue -> SQLangValue.IntValue(coll.rows.size.toLong())
                    is SQLangValue.TextValue -> SQLangValue.IntValue(coll.value.length.toLong())
                    else -> throw SQLangRuntimeException("SIZE requires collection or TEXT")
                }
            }

            "PUSH", "APPEND" -> {
                require(args.size == 2) { "PUSH requires 2 arguments" }
                val arr = (evaluate(args[0], env) as? SQLangValue.ArrayValue)
                    ?: throw SQLangRuntimeException("PUSH requires ARRAY")
                val value = evaluate(args[1], env)
                arr.elements.add(value)
                arr
            }

            "POP" -> {
                require(args.size == 1) { "POP requires 1 argument" }
                val arr = (evaluate(args[0], env) as? SQLangValue.ArrayValue)
                    ?: throw SQLangRuntimeException("POP requires ARRAY")
                if (arr.elements.isEmpty()) {
                    throw SQLangRuntimeException("Cannot POP from empty array")
                }
                arr.elements.removeLast()
            }

            "CONTAINS" -> {
                require(args.size == 2) { "CONTAINS requires 2 arguments" }
                when (val coll = evaluate(args[0], env)) {
                    is SQLangValue.ArrayValue -> {
                        val value = evaluate(args[1], env)
                        SQLangValue.BoolValue(coll.elements.any { valuesEqual(it, value) })
                    }
                    is SQLangValue.TextValue -> {
                        val substr = (evaluate(args[1], env) as? SQLangValue.TextValue)?.value
                            ?: throw SQLangRuntimeException("CONTAINS substring must be TEXT")
                        SQLangValue.BoolValue(coll.value.contains(substr))
                    }
                    is SQLangValue.MapValue -> {
                        val key = evaluate(args[1], env)
                        SQLangValue.BoolValue(coll.entries.containsKey(key))
                    }
                    else -> throw SQLangRuntimeException("CONTAINS requires ARRAY, TEXT, or MAP")
                }
            }

            "REVERSE" -> {
                require(args.size == 1) { "REVERSE requires 1 argument" }
                when (val value = evaluate(args[0], env)) {
                    is SQLangValue.ArrayValue -> SQLangValue.ArrayValue(value.elements.reversed().toMutableList())
                    is SQLangValue.TextValue -> SQLangValue.TextValue(value.value.reversed())
                    else -> throw SQLangRuntimeException("REVERSE requires ARRAY or TEXT")
                }
            }

            "SORT" -> {
                require(args.size == 1) { "SORT requires 1 argument" }
                val arr = (evaluate(args[0], env) as? SQLangValue.ArrayValue)
                    ?: throw SQLangRuntimeException("SORT requires ARRAY")
                SQLangValue.ArrayValue(arr.elements.sortedWith { a, b -> compareValues(a, b) }.toMutableList())
            }

            // Type conversion
            "TO_INT", "INT" -> {
                require(args.size == 1) { "TO_INT requires 1 argument" }
                when (val value = evaluate(args[0], env)) {
                    is SQLangValue.IntValue -> value
                    is SQLangValue.DecimalValue -> SQLangValue.IntValue(value.value.toLong())
                    is SQLangValue.TextValue -> SQLangValue.IntValue(value.value.toLongOrNull()
                        ?: throw SQLangRuntimeException("Cannot convert '${value.value}' to INT"))
                    is SQLangValue.BoolValue -> SQLangValue.IntValue(if (value.value) 1 else 0)
                    else -> throw SQLangRuntimeException("Cannot convert ${value::class.simpleName} to INT")
                }
            }

            "TO_DECIMAL", "DECIMAL" -> {
                require(args.size == 1) { "TO_DECIMAL requires 1 argument" }
                when (val value = evaluate(args[0], env)) {
                    is SQLangValue.IntValue -> SQLangValue.DecimalValue(value.value.toDouble())
                    is SQLangValue.DecimalValue -> value
                    is SQLangValue.TextValue -> SQLangValue.DecimalValue(value.value.toDoubleOrNull()
                        ?: throw SQLangRuntimeException("Cannot convert '${value.value}' to DECIMAL"))
                    else -> throw SQLangRuntimeException("Cannot convert ${value::class.simpleName} to DECIMAL")
                }
            }

            "TO_TEXT", "TEXT", "STR" -> {
                require(args.size == 1) { "TO_TEXT requires 1 argument" }
                SQLangValue.TextValue(evaluate(args[0], env).toDisplayString())
            }

            "TO_BOOL", "BOOL" -> {
                require(args.size == 1) { "TO_BOOL requires 1 argument" }
                SQLangValue.BoolValue(evaluate(args[0], env).toBoolean())
            }

            // Type checking
            "TYPE", "TYPEOF" -> {
                require(args.size == 1) { "TYPE requires 1 argument" }
                val value = evaluate(args[0], env)
                val typeName = when (value) {
                    is SQLangValue.IntValue -> "INT"
                    is SQLangValue.DecimalValue -> "DECIMAL"
                    is SQLangValue.TextValue -> "TEXT"
                    is SQLangValue.BoolValue -> "BOOL"
                    is SQLangValue.CharValue -> "CHAR"
                    is SQLangValue.NullValue -> "NULL"
                    is SQLangValue.ArrayValue -> "ARRAY"
                    is SQLangValue.MapValue -> "MAP"
                    is SQLangValue.TableValue -> "TABLE"
                    is SQLangValue.RangeValue -> "RANGE"
                    is SQLangValue.FunctionValue -> "FUNCTION"
                    is SQLangValue.ProcedureValue -> "PROCEDURE"
                }
                SQLangValue.TextValue(typeName)
            }

            "IS_NULL" -> {
                require(args.size == 1) { "IS_NULL requires 1 argument" }
                SQLangValue.BoolValue(evaluate(args[0], env) is SQLangValue.NullValue)
            }

            "IS_NUMBER" -> {
                require(args.size == 1) { "IS_NUMBER requires 1 argument" }
                val value = evaluate(args[0], env)
                SQLangValue.BoolValue(value is SQLangValue.IntValue || value is SQLangValue.DecimalValue)
            }

            // Utility
            "COALESCE" -> {
                for (arg in args) {
                    val value = evaluate(arg, env)
                    if (value !is SQLangValue.NullValue) {
                        return value
                    }
                }
                SQLangValue.NullValue
            }

            "RANGE" -> {
                require(args.size == 2) { "RANGE requires 2 arguments" }
                val start = (evaluate(args[0], env) as? SQLangValue.IntValue)?.value
                    ?: throw SQLangRuntimeException("RANGE start must be INT")
                val end = (evaluate(args[1], env) as? SQLangValue.IntValue)?.value
                    ?: throw SQLangRuntimeException("RANGE end must be INT")
                SQLangValue.RangeValue(start, end)
            }

            else -> null // Not a builtin
        }
    }

    private fun evaluateIndex(expr: IndexExpr, env: Environment): SQLangValue {
        val obj = evaluate(expr.obj, env)
        val index = evaluate(expr.index, env)

        return when (obj) {
            is SQLangValue.ArrayValue -> {
                val idx = (index as? SQLangValue.IntValue)?.value?.toInt()
                    ?: throw SQLangRuntimeException("Array index must be an integer")
                obj.get(idx)
            }
            is SQLangValue.MapValue -> obj.get(index)
            is SQLangValue.TextValue -> {
                val idx = (index as? SQLangValue.IntValue)?.value?.toInt()
                    ?: throw SQLangRuntimeException("String index must be an integer")
                if (idx < 0 || idx >= obj.value.length) {
                    throw SQLangRuntimeException("String index out of bounds: $idx")
                }
                SQLangValue.CharValue(obj.value[idx])
            }
            else -> throw SQLangRuntimeException("Cannot index ${obj::class.simpleName}")
        }
    }

    private fun evaluateMember(expr: MemberExpr, env: Environment): SQLangValue {
        val obj = evaluate(expr.obj, env)

        return when (obj) {
            is SQLangValue.MapValue -> obj.get(SQLangValue.TextValue(expr.member))
            is SQLangValue.ArrayValue -> when (expr.member) {
                "length", "size" -> SQLangValue.IntValue(obj.elements.size.toLong())
                "first" -> if (obj.elements.isNotEmpty()) obj.elements.first() else SQLangValue.NullValue
                "last" -> if (obj.elements.isNotEmpty()) obj.elements.last() else SQLangValue.NullValue
                else -> throw SQLangRuntimeException("Unknown array property: ${expr.member}")
            }
            is SQLangValue.TextValue -> when (expr.member) {
                "length", "size" -> SQLangValue.IntValue(obj.value.length.toLong())
                else -> throw SQLangRuntimeException("Unknown string property: ${expr.member}")
            }
            else -> throw SQLangRuntimeException("Cannot access member on ${obj::class.simpleName}")
        }
    }

    private fun evaluateArray(expr: ArrayExpr, env: Environment): SQLangValue {
        val elements = expr.elements.map { evaluate(it, env) }.toMutableList()
        return SQLangValue.ArrayValue(elements)
    }

    private fun evaluateMap(expr: MapExpr, env: Environment): SQLangValue {
        val entries = mutableMapOf<SQLangValue, SQLangValue>()
        for ((keyExpr, valueExpr) in expr.entries) {
            val key = evaluate(keyExpr, env)
            val value = evaluate(valueExpr, env)
            entries[key] = value
        }
        return SQLangValue.MapValue(entries)
    }

    private fun evaluateRange(expr: RangeExpr, env: Environment): SQLangValue {
        val start = (evaluate(expr.start, env) as? SQLangValue.IntValue)?.value
            ?: throw SQLangRuntimeException("Range start must be an integer")
        val end = (evaluate(expr.end, env) as? SQLangValue.IntValue)?.value
            ?: throw SQLangRuntimeException("Range end must be an integer")
        return SQLangValue.RangeValue(start, end)
    }

    private fun evaluateTernary(expr: TernaryExpr, env: Environment): SQLangValue {
        return if (evaluate(expr.condition, env).toBoolean()) {
            evaluate(expr.thenBranch, env)
        } else {
            evaluate(expr.elseBranch, env)
        }
    }

    private fun evaluateSelect(expr: SelectExpr, env: Environment): SQLangValue {
        // Get source table
        val sourceRows: List<MutableList<SQLangValue>>
        val sourceColumns: List<String>

        if (expr.from != null) {
            val table = env.getTable(expr.from)
            sourceRows = table.rows.toList()
            sourceColumns = table.columns
        } else {
            // SELECT without FROM - just evaluate expressions
            val row = mutableListOf<SQLangValue>()
            val cols = mutableListOf<String>()
            for ((i, col) in expr.columns.withIndex()) {
                row.add(evaluate(col.expression, env))
                cols.add(col.alias ?: "col$i")
            }
            return SQLangValue.TableValue(cols, mutableListOf(row))
        }

        // Filter with WHERE
        val filteredRows = if (expr.where != null) {
            sourceRows.filter { row ->
                val rowEnv = env.createChild()
                for (i in sourceColumns.indices) {
                    rowEnv.define(sourceColumns[i], row[i])
                }
                evaluate(expr.where, rowEnv).toBoolean()
            }
        } else {
            sourceRows
        }

        // Handle SELECT *
        if (expr.columns.size == 1 &&
            expr.columns[0].expression is LiteralExpr &&
            (expr.columns[0].expression as LiteralExpr).value == "*") {

            var resultRows = filteredRows.map { it.toMutableList() }.toMutableList()

            // ORDER BY
            if (expr.orderBy != null) {
                resultRows = resultRows.sortedWith { row1, row2 ->
                    for (orderClause in expr.orderBy) {
                        val env1 = env.createChild()
                        val env2 = env.createChild()
                        for (i in sourceColumns.indices) {
                            env1.define(sourceColumns[i], row1[i])
                            env2.define(sourceColumns[i], row2[i])
                        }
                        val val1 = evaluate(orderClause.expression, env1)
                        val val2 = evaluate(orderClause.expression, env2)
                        val cmp = compareValues(val1, val2)
                        if (cmp != 0) {
                            return@sortedWith if (orderClause.ascending) cmp else -cmp
                        }
                    }
                    0
                }.toMutableList()
            }

            // LIMIT and OFFSET
            val offset = expr.offset?.let {
                (evaluate(it, env) as? SQLangValue.IntValue)?.value?.toInt() ?: 0
            } ?: 0
            val limit = expr.limit?.let {
                (evaluate(it, env) as? SQLangValue.IntValue)?.value?.toInt()
            }

            if (offset > 0 || limit != null) {
                val endIndex = if (limit != null) minOf(offset + limit, resultRows.size) else resultRows.size
                resultRows = resultRows.subList(offset, endIndex).toMutableList()
            }

            return SQLangValue.TableValue(sourceColumns, resultRows)
        }

        // Project columns
        val resultColumns = expr.columns.mapIndexed { i, col ->
            col.alias ?: if (col.expression is IdentifierExpr) {
                col.expression.name
            } else {
                "col$i"
            }
        }

        var resultRows = filteredRows.map { row ->
            val rowEnv = env.createChild()
            for (i in sourceColumns.indices) {
                rowEnv.define(sourceColumns[i], row[i])
            }
            expr.columns.map { col ->
                evaluate(col.expression, rowEnv)
            }.toMutableList()
        }.toMutableList()

        // ORDER BY
        if (expr.orderBy != null) {
            resultRows = resultRows.sortedWith { row1, row2 ->
                for (orderClause in expr.orderBy) {
                    val idx = resultColumns.indexOf(
                        if (orderClause.expression is IdentifierExpr) {
                            orderClause.expression.name
                        } else ""
                    )
                    if (idx >= 0) {
                        val cmp = compareValues(row1[idx], row2[idx])
                        if (cmp != 0) {
                            return@sortedWith if (orderClause.ascending) cmp else -cmp
                        }
                    }
                }
                0
            }.toMutableList()
        }

        // LIMIT and OFFSET
        val offset = expr.offset?.let {
            (evaluate(it, env) as? SQLangValue.IntValue)?.value?.toInt() ?: 0
        } ?: 0
        val limit = expr.limit?.let {
            (evaluate(it, env) as? SQLangValue.IntValue)?.value?.toInt()
        }

        if (offset > 0 || limit != null) {
            val endIndex = if (limit != null) minOf(offset + limit, resultRows.size) else resultRows.size
            resultRows = resultRows.subList(offset, endIndex).toMutableList()
        }

        // DISTINCT
        if (expr.distinct) {
            resultRows = resultRows.distinctBy { row ->
                row.joinToString("|") { it.toDisplayString() }
            }.toMutableList()
        }

        return SQLangValue.TableValue(resultColumns, resultRows)
    }

    // ============================================
    // HELPER METHODS
    // ============================================

    private fun getDefaultValue(type: SQLangType?): SQLangValue {
        return when (type) {
            is SQLangType.IntType -> SQLangValue.IntValue(0)
            is SQLangType.DecimalType -> SQLangValue.DecimalValue(0.0)
            is SQLangType.TextType -> SQLangValue.TextValue("")
            is SQLangType.BoolType -> SQLangValue.BoolValue(false)
            is SQLangType.CharType -> SQLangValue.CharValue('\u0000')
            is SQLangType.ArrayType -> SQLangValue.ArrayValue(mutableListOf())
            is SQLangType.MapType -> SQLangValue.MapValue(mutableMapOf())
            is SQLangType.NullableType -> SQLangValue.NullValue
            else -> SQLangValue.NullValue
        }
    }

    private fun toDouble(value: SQLangValue): Double {
        return when (value) {
            is SQLangValue.IntValue -> value.value.toDouble()
            is SQLangValue.DecimalValue -> value.value
            else -> throw SQLangRuntimeException("Cannot convert ${value::class.simpleName} to number")
        }
    }

    private fun valuesEqual(a: SQLangValue, b: SQLangValue): Boolean {
        if (a is SQLangValue.NullValue && b is SQLangValue.NullValue) return true
        if (a is SQLangValue.NullValue || b is SQLangValue.NullValue) return false

        // Numeric comparison
        if ((a is SQLangValue.IntValue || a is SQLangValue.DecimalValue) &&
            (b is SQLangValue.IntValue || b is SQLangValue.DecimalValue)) {
            return toDouble(a) == toDouble(b)
        }

        return when {
            a is SQLangValue.TextValue && b is SQLangValue.TextValue -> a.value == b.value
            a is SQLangValue.BoolValue && b is SQLangValue.BoolValue -> a.value == b.value
            a is SQLangValue.CharValue && b is SQLangValue.CharValue -> a.value == b.value
            else -> false
        }
    }

    private fun compareValues(a: SQLangValue, b: SQLangValue): Int {
        if (a is SQLangValue.NullValue) return if (b is SQLangValue.NullValue) 0 else -1
        if (b is SQLangValue.NullValue) return 1

        return when {
            (a is SQLangValue.IntValue || a is SQLangValue.DecimalValue) &&
                    (b is SQLangValue.IntValue || b is SQLangValue.DecimalValue) -> {
                toDouble(a).compareTo(toDouble(b))
            }
            a is SQLangValue.TextValue && b is SQLangValue.TextValue -> a.value.compareTo(b.value)
            a is SQLangValue.BoolValue && b is SQLangValue.BoolValue -> a.value.compareTo(b.value)
            else -> throw SQLangRuntimeException("Cannot compare ${a::class.simpleName} and ${b::class.simpleName}")
        }
    }
}