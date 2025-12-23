package com.sqlang.parser

import com.sqlang.lexer.Token
import com.sqlang.lexer.TokenType
import com.sqlang.lexer.TokenType.*
import com.sqlang.parser.ast.*

class Parser(private val tokens: List<Token>) {
    private var current = 0

    fun parse(): List<Statement> {
        val statements = mutableListOf<Statement>()
        while (!isAtEnd()) {
            val stmt = parseStatement()
            if (stmt != null) {
                statements.add(stmt)
            }
        }
        return statements
    }

    // ============================================
    // STATEMENT PARSING
    // ============================================

    private fun parseStatement(): Statement? {
        return when {
            check(CREATE) -> parseCreateStatement()
            check(DECLARE) -> parseDeclareStatement()
            check(CONST) -> parseConstStatement()
            check(SET) -> parseSetStatement()
            check(PRINT) -> parsePrintStatement()
            check(INPUT) -> parseInputStatement()
            check(IF) -> parseIfStatement()
            check(CASE) -> parseCaseStatement()
            check(WHILE) -> parseWhileStatement()
            check(FOR) -> parseForStatement()
            check(BREAK) -> parseBreakStatement()
            check(CONTINUE) -> parseContinueStatement()
            check(RETURN) -> parseReturnStatement()
            check(EXEC) -> parseExecStatement()
            check(INSERT) -> parseInsertStatement()
            check(UPDATE) -> parseUpdateStatement()
            check(DELETE) -> parseDeleteStatement()
            check(TRY) -> parseTryStatement()
            check(THROW) -> parseThrowStatement()
            else -> parseExpressionStatement()
        }
    }

    private fun parseCreateStatement(): Statement {
        val token = advance() // consume CREATE

        return when {
            check(PROGRAM) -> parseProgramStatement(token)
            check(FUNCTION) -> parseFunctionStatement(token)
            check(PROCEDURE) -> parseProcedureStatement(token)
            check(TABLE) -> parseCreateTableStatement(token)
            else -> throw ParserException("Expected PROGRAM, FUNCTION, PROCEDURE, or TABLE after CREATE", peek())
        }
    }

    private fun parseProgramStatement(createToken: Token): ProgramStmt {
        advance() // consume PROGRAM
        val name = consume(IDENTIFIER, "Expected program name").lexeme
        consume(SEMICOLON, "Expected ';' after program name")

        val statements = mutableListOf<Statement>()
        while (!check(END) && !isAtEnd()) {
            val stmt = parseStatement()
            if (stmt != null) {
                statements.add(stmt)
            }
        }

        consume(END, "Expected END")
        consume(PROGRAM, "Expected PROGRAM after END")
        consume(SEMICOLON, "Expected ';' after END PROGRAM")

        return ProgramStmt(name, statements, createToken.line, createToken.column)
    }

    private fun parseFunctionStatement(createToken: Token): FunctionStmt {
        advance() // consume FUNCTION
        val name = consume(IDENTIFIER, "Expected function name").lexeme
        consume(LPAREN, "Expected '(' after function name")

        val parameters = parseParameterList()

        consume(RPAREN, "Expected ')' after parameters")
        consume(RETURNS, "Expected RETURNS")
        val returnType = parseType()
        consume(AS, "Expected AS")

        val body = parseBlock(END, FUNCTION)

        consume(END, "Expected END")
        consume(FUNCTION, "Expected FUNCTION after END")
        consume(SEMICOLON, "Expected ';' after END FUNCTION")

        return FunctionStmt(name, parameters, returnType, body, createToken.line, createToken.column)
    }

    private fun parseProcedureStatement(createToken: Token): ProcedureStmt {
        advance() // consume PROCEDURE
        val name = consume(IDENTIFIER, "Expected procedure name").lexeme
        consume(LPAREN, "Expected '(' after procedure name")

        val parameters = parseParameterList()

        consume(RPAREN, "Expected ')' after parameters")
        consume(AS, "Expected AS")

        val body = parseBlock(END, PROCEDURE)

        consume(END, "Expected END")
        consume(PROCEDURE, "Expected PROCEDURE after END")
        consume(SEMICOLON, "Expected ';' after END PROCEDURE")

        return ProcedureStmt(name, parameters, body, createToken.line, createToken.column)
    }

    private fun parseParameterList(): List<Parameter> {
        val parameters = mutableListOf<Parameter>()

        if (!check(RPAREN)) {
            do {
                val paramToken = consume(IDENTIFIER, "Expected parameter name")
                val paramName = paramToken.lexeme
                val paramType = parseType()

                val defaultValue = if (match(EQUAL)) {
                    parseExpression()
                } else null

                parameters.add(Parameter(paramName, paramType, defaultValue, paramToken.line, paramToken.column))
            } while (match(COMMA))
        }

        return parameters
    }

    private fun parseCreateTableStatement(createToken: Token): CreateTableStmt {
        advance() // consume TABLE
        val name = consume(IDENTIFIER, "Expected table name").lexeme
        consume(LPAREN, "Expected '(' after table name")

        val columns = mutableListOf<ColumnDef>()
        do {
            val colToken = consume(IDENTIFIER, "Expected column name")
            val colName = colToken.lexeme
            val colType = parseType()

            var isPrimaryKey = false
            if (check(PRIMARY)) {
                advance()
                consume(KEY, "Expected KEY after PRIMARY")
                isPrimaryKey = true
            }

            columns.add(ColumnDef(colName, colType, isPrimaryKey, !isPrimaryKey, colToken.line, colToken.column))
        } while (match(COMMA))

        consume(RPAREN, "Expected ')' after column definitions")
        consume(SEMICOLON, "Expected ';' after CREATE TABLE")

        return CreateTableStmt(name, columns, createToken.line, createToken.column)
    }

    private fun parseDeclareStatement(): DeclareStmt {
        val token = advance() // consume DECLARE

        val isAuto = match(AUTO)
        val name = consume(IDENTIFIER, "Expected variable name").lexeme

        val type = if (!isAuto && !check(EQUAL)) {
            parseType()
        } else null

        val initializer = if (match(EQUAL)) {
            parseExpression()
        } else null

        consume(SEMICOLON, "Expected ';' after variable declaration")

        return DeclareStmt(name, type, initializer, isConst = false, isAuto = isAuto, token.line, token.column)
    }

    private fun parseConstStatement(): DeclareStmt {
        val token = advance() // consume CONST
        val name = consume(IDENTIFIER, "Expected constant name").lexeme
        val type = parseType()

        consume(EQUAL, "Expected '=' in constant declaration")
        val initializer = parseExpression()
        consume(SEMICOLON, "Expected ';' after constant declaration")

        return DeclareStmt(name, type, initializer, isConst = true, isAuto = false, token.line, token.column)
    }

    private fun parseSetStatement(): SetStmt {
        val token = advance() // consume SET
        val target = parseExpression()
        consume(EQUAL, "Expected '=' in SET statement")
        val value = parseExpression()
        consume(SEMICOLON, "Expected ';' after SET statement")

        return SetStmt(target, value, token.line, token.column)
    }

    private fun parsePrintStatement(): PrintStmt {
        val token = advance() // consume PRINT
        val expression = parseExpression()
        consume(SEMICOLON, "Expected ';' after PRINT statement")

        return PrintStmt(expression, token.line, token.column)
    }

    private fun parseInputStatement(): InputStmt {
        val token = advance() // consume INPUT
        val target = consume(IDENTIFIER, "Expected variable name after INPUT").lexeme

        val prompt = if (match(COMMA)) {
            parseExpression()
        } else null

        consume(SEMICOLON, "Expected ';' after INPUT statement")

        return InputStmt(target, prompt, token.line, token.column)
    }

    private fun parseIfStatement(): IfStmt {
        val token = advance() // consume IF
        val condition = parseExpression()
        consume(THEN, "Expected THEN after IF condition")

        val thenBranch = mutableListOf<Statement>()
        while (!check(ELSE) && !check(END) && !isAtEnd()) {
            val stmt = parseStatement()
            if (stmt != null) {
                thenBranch.add(stmt)
            }
        }

        val elseIfBranches = mutableListOf<Pair<Expression, List<Statement>>>()
        var elseBranch: List<Statement>? = null

        while (match(ELSE)) {
            if (match(IF)) {
                // ELSE IF
                val elseIfCondition = parseExpression()
                consume(THEN, "Expected THEN after ELSE IF condition")

                val elseIfBody = mutableListOf<Statement>()
                while (!check(ELSE) && !check(END) && !isAtEnd()) {
                    val stmt = parseStatement()
                    if (stmt != null) {
                        elseIfBody.add(stmt)
                    }
                }
                elseIfBranches.add(Pair(elseIfCondition, elseIfBody))
            } else {
                // ELSE
                val elseBody = mutableListOf<Statement>()
                while (!check(END) && !isAtEnd()) {
                    val stmt = parseStatement()
                    if (stmt != null) {
                        elseBody.add(stmt)
                    }
                }
                elseBranch = elseBody
                break
            }
        }

        consume(END, "Expected END")
        consume(IF, "Expected IF after END")
        consume(SEMICOLON, "Expected ';' after END IF")

        return IfStmt(condition, thenBranch, elseIfBranches, elseBranch, token.line, token.column)
    }

    private fun parseCaseStatement(): CaseStmt {
        val token = advance() // consume CASE
        val subject = parseExpression()

        val whenBranches = mutableListOf<Pair<Expression, List<Statement>>>()
        var defaultBranch: List<Statement>? = null

        while (match(WHEN)) {
            val whenExpr = parseExpression()
            consume(THEN, "Expected THEN after WHEN expression")

            val whenBody = mutableListOf<Statement>()
            while (!check(WHEN) && !check(DEFAULT) && !check(END) && !isAtEnd()) {
                val stmt = parseStatement()
                if (stmt != null) {
                    whenBody.add(stmt)
                }
            }
            whenBranches.add(Pair(whenExpr, whenBody))
        }

        if (match(DEFAULT)) {
            val defaultBody = mutableListOf<Statement>()
            while (!check(END) && !isAtEnd()) {
                val stmt = parseStatement()
                if (stmt != null) {
                    defaultBody.add(stmt)
                }
            }
            defaultBranch = defaultBody
        }

        consume(END, "Expected END")
        consume(CASE, "Expected CASE after END")
        consume(SEMICOLON, "Expected ';' after END CASE")

        return CaseStmt(subject, whenBranches, defaultBranch, token.line, token.column)
    }

    private fun parseWhileStatement(): WhileStmt {
        val token = advance() // consume WHILE
        val condition = parseExpression()
        consume(DO, "Expected DO after WHILE condition")

        val body = parseBlock(END, WHILE)

        consume(END, "Expected END")
        consume(WHILE, "Expected WHILE after END")
        consume(SEMICOLON, "Expected ';' after END WHILE")

        return WhileStmt(condition, body, token.line, token.column)
    }

    private fun parseForStatement(): Statement {
        val token = advance() // consume FOR

        if (match(EACH)) {
            // FOR EACH
            val variable = consume(IDENTIFIER, "Expected variable name after FOR EACH").lexeme
            consume(IN, "Expected IN after variable")
            val iterable = parseExpression()
            consume(DO, "Expected DO after FOR EACH ... IN")

            val body = parseBlock(END, FOR)

            consume(END, "Expected END")
            consume(FOR, "Expected FOR after END")
            consume(SEMICOLON, "Expected ';' after END FOR")

            return ForEachStmt(variable, iterable, body, token.line, token.column)
        } else {
            // FOR i IN 1..10
            val variable = consume(IDENTIFIER, "Expected variable name after FOR").lexeme
            consume(IN, "Expected IN after variable")
            val range = parseExpression()
            consume(DO, "Expected DO after range")

            val body = parseBlock(END, FOR)

            consume(END, "Expected END")
            consume(FOR, "Expected FOR after END")
            consume(SEMICOLON, "Expected ';' after END FOR")

            return ForStmt(variable, range, body, token.line, token.column)
        }
    }

    private fun parseBreakStatement(): BreakStmt {
        val token = advance() // consume BREAK
        consume(SEMICOLON, "Expected ';' after BREAK")
        return BreakStmt(token.line, token.column)
    }

    private fun parseContinueStatement(): ContinueStmt {
        val token = advance() // consume CONTINUE
        consume(SEMICOLON, "Expected ';' after CONTINUE")
        return ContinueStmt(token.line, token.column)
    }

    private fun parseReturnStatement(): ReturnStmt {
        val token = advance() // consume RETURN
        val value = if (!check(SEMICOLON)) {
            parseExpression()
        } else null
        consume(SEMICOLON, "Expected ';' after RETURN")
        return ReturnStmt(value, token.line, token.column)
    }

    private fun parseExecStatement(): ExecStmt {
        val token = advance() // consume EXEC
        val callee = consume(IDENTIFIER, "Expected procedure name after EXEC").lexeme
        consume(LPAREN, "Expected '(' after procedure name")

        val arguments = mutableListOf<Expression>()
        if (!check(RPAREN)) {
            do {
                arguments.add(parseExpression())
            } while (match(COMMA))
        }

        consume(RPAREN, "Expected ')' after arguments")
        consume(SEMICOLON, "Expected ';' after EXEC statement")

        return ExecStmt(callee, arguments, token.line, token.column)
    }

    private fun parseInsertStatement(): InsertStmt {
        val token = advance() // consume INSERT
        consume(INTO, "Expected INTO after INSERT")
        val table = consume(IDENTIFIER, "Expected table name").lexeme

        val columns = if (match(LPAREN)) {
            val cols = mutableListOf<String>()
            do {
                cols.add(consume(IDENTIFIER, "Expected column name").lexeme)
            } while (match(COMMA))
            consume(RPAREN, "Expected ')' after column list")
            cols
        } else null

        consume(VALUES, "Expected VALUES")
        consume(LPAREN, "Expected '(' after VALUES")

        val values = mutableListOf<Expression>()
        do {
            values.add(parseExpression())
        } while (match(COMMA))

        consume(RPAREN, "Expected ')' after values")
        consume(SEMICOLON, "Expected ';' after INSERT statement")

        return InsertStmt(table, columns, values, token.line, token.column)
    }

    private fun parseUpdateStatement(): Statement {
        val token = advance() // consume UPDATE
        val target = consume(IDENTIFIER, "Expected target after UPDATE").lexeme
        consume(SET, "Expected SET after UPDATE target")

        // Check if this is a simple variable update or table update
        if (peek().type == IDENTIFIER && peekNext()?.type == EQUAL) {
            // Table update: UPDATE table SET col = value WHERE ...
            val assignments = mutableListOf<Pair<String, Expression>>()
            do {
                val col = consume(IDENTIFIER, "Expected column name").lexeme
                consume(EQUAL, "Expected '=' after column name")
                val value = parseExpression()
                assignments.add(Pair(col, value))
            } while (match(COMMA))

            val where = if (match(WHERE)) {
                parseExpression()
            } else null

            consume(SEMICOLON, "Expected ';' after UPDATE statement")

            return UpdateTableStmt(target, assignments, where, token.line, token.column)
        } else {
            // This shouldn't happen with current grammar, but handle gracefully
            throw ParserException("Invalid UPDATE syntax", peek())
        }
    }

    private fun parseDeleteStatement(): DeleteStmt {
        val token = advance() // consume DELETE
        consume(FROM, "Expected FROM after DELETE")
        val table = consume(IDENTIFIER, "Expected table name").lexeme

        val where = if (match(WHERE)) {
            parseExpression()
        } else null

        consume(SEMICOLON, "Expected ';' after DELETE statement")

        return DeleteStmt(table, where, token.line, token.column)
    }

    private fun parseTryStatement(): TryStmt {
        val token = advance() // consume TRY

        val tryBlock = mutableListOf<Statement>()
        while (!check(CATCH) && !check(FINALLY) && !check(END) && !isAtEnd()) {
            val stmt = parseStatement()
            if (stmt != null) {
                tryBlock.add(stmt)
            }
        }

        var catchParam: String? = null
        var catchBlock: List<Statement>? = null

        if (match(CATCH)) {
            if (match(LPAREN)) {
                catchParam = consume(IDENTIFIER, "Expected exception variable name").lexeme
                consume(RPAREN, "Expected ')' after catch parameter")
            }

            val block = mutableListOf<Statement>()
            while (!check(FINALLY) && !check(END) && !isAtEnd()) {
                val stmt = parseStatement()
                if (stmt != null) {
                    block.add(stmt)
                }
            }
            catchBlock = block
        }

        var finallyBlock: List<Statement>? = null
        if (match(FINALLY)) {
            val block = mutableListOf<Statement>()
            while (!check(END) && !isAtEnd()) {
                val stmt = parseStatement()
                if (stmt != null) {
                    block.add(stmt)
                }
            }
            finallyBlock = block
        }

        consume(END, "Expected END")
        consume(TRY, "Expected TRY after END")
        consume(SEMICOLON, "Expected ';' after END TRY")

        return TryStmt(tryBlock, catchParam, catchBlock, finallyBlock, token.line, token.column)
    }

    private fun parseThrowStatement(): ThrowStmt {
        val token = advance() // consume THROW
        val expression = parseExpression()
        consume(SEMICOLON, "Expected ';' after THROW")
        return ThrowStmt(expression, token.line, token.column)
    }

    private fun parseExpressionStatement(): ExpressionStmt? {
        val expr = parseExpression()
        consume(SEMICOLON, "Expected ';' after expression")
        return ExpressionStmt(expr, expr.line, expr.column)
    }

    private fun parseBlock(endKeyword1: TokenType, endKeyword2: TokenType): List<Statement> {
        val statements = mutableListOf<Statement>()
        while (!check(endKeyword1) && !isAtEnd()) {
            val stmt = parseStatement()
            if (stmt != null) {
                statements.add(stmt)
            }
        }
        return statements
    }

    // ============================================
    // TYPE PARSING
    // ============================================

    private fun parseType(): SQLangType {
        val token = peek()
        val baseType = when {
            match(INT) -> SQLangType.IntType(token.line, token.column)
            match(DECIMAL_TYPE) -> {
                if (match(LPAREN)) {
                    val precision = consume(INTEGER, "Expected precision").literal as Long
                    consume(COMMA, "Expected ',' after precision")
                    val scale = consume(INTEGER, "Expected scale").literal as Long
                    consume(RPAREN, "Expected ')' after scale")
                    SQLangType.DecimalType(token.line, token.column, precision.toInt(), scale.toInt())
                } else {
                    SQLangType.DecimalType(token.line, token.column)
                }
            }
            match(TEXT) -> SQLangType.TextType(token.line, token.column)
            match(BOOL) -> SQLangType.BoolType(token.line, token.column)
            match(CHAR) -> SQLangType.CharType(token.line, token.column)
            match(VOID) -> SQLangType.VoidType(token.line, token.column)
            match(TABLE) -> SQLangType.TableType(null, token.line, token.column)
            match(MAP) -> {
                consume(LESS, "Expected '<' after MAP")
                val keyType = parseType()
                consume(COMMA, "Expected ',' in MAP type")
                val valueType = parseType()
                consume(GREATER, "Expected '>' after MAP value type")
                SQLangType.MapType(keyType, valueType, token.line, token.column)
            }
            check(IDENTIFIER) -> {
                val name = advance().lexeme
                SQLangType.CustomType(name, token.line, token.column)
            }
            else -> throw ParserException("Expected type", token)
        }

        // Check for array suffix []
        return if (match(LBRACKET)) {
            consume(RBRACKET, "Expected ']' for array type")
            val arrayType = SQLangType.ArrayType(baseType, token.line, token.column)

            // Check for nullable
            if (match(QUESTION)) {
                SQLangType.NullableType(arrayType, token.line, token.column)
            } else {
                arrayType
            }
        } else if (match(QUESTION)) {
            SQLangType.NullableType(baseType, token.line, token.column)
        } else {
            baseType
        }
    }

    // ============================================
    // EXPRESSION PARSING (Pratt Parser style)
    // ============================================

    private fun parseExpression(): Expression {
        return parseOr()
    }

    private fun parseOr(): Expression {
        var expr = parseAnd()

        while (match(OR) || match(PIPE)) {
            val operator = previous()
            val right = parseAnd()
            expr = BinaryExpr(expr, operator, right, expr.line, expr.column)
        }

        return expr
    }

    private fun parseAnd(): Expression {
        var expr = parseEquality()

        while (match(AND) || match(AMPERSAND)) {
            val operator = previous()
            val right = parseEquality()
            expr = BinaryExpr(expr, operator, right, expr.line, expr.column)
        }

        return expr
    }

    private fun parseEquality(): Expression {
        var expr = parseComparison()

        while (match(EQUAL, NOT_EQUAL, IS)) {
            val operator = previous()

            if (operator.type == IS) {
                // Handle IS NULL / IS NOT NULL
                val notNull = match(NOT)
                if (match(NULL)) {
                    val nullToken = previous()
                    val nullLiteral = LiteralExpr(null, nullToken.line, nullToken.column)
                    expr = if (notNull) {
                        BinaryExpr(expr, Token(NOT_EQUAL, "IS NOT", null, operator.line, operator.column), nullLiteral, expr.line, expr.column)
                    } else {
                        BinaryExpr(expr, Token(EQUAL, "IS", null, operator.line, operator.column), nullLiteral, expr.line, expr.column)
                    }
                } else {
                    throw ParserException("Expected NULL after IS", peek())
                }
            } else {
                val right = parseComparison()
                expr = BinaryExpr(expr, operator, right, expr.line, expr.column)
            }
        }

        return expr
    }

    private fun parseComparison(): Expression {
        var expr = parseTerm()

        while (match(LESS, LESS_EQUAL, GREATER, GREATER_EQUAL, LIKE, BETWEEN)) {
            val operator = previous()

            if (operator.type == BETWEEN) {
                val low = parseTerm()
                consume(AND, "Expected AND in BETWEEN expression")
                val high = parseTerm()
                // Create a compound expression for BETWEEN
                val lowCheck = BinaryExpr(expr, Token(GREATER_EQUAL, ">=", null, operator.line, operator.column), low, expr.line, expr.column)
                val highCheck = BinaryExpr(expr, Token(LESS_EQUAL, "<=", null, operator.line, operator.column), high, expr.line, expr.column)
                expr = BinaryExpr(lowCheck, Token(AND, "AND", null, operator.line, operator.column), highCheck, expr.line, expr.column)
            } else {
                val right = parseTerm()
                expr = BinaryExpr(expr, operator, right, expr.line, expr.column)
            }
        }

        return expr
    }

    private fun parseTerm(): Expression {
        var expr = parseFactor()

        while (match(PLUS, MINUS)) {
            val operator = previous()
            val right = parseFactor()
            expr = BinaryExpr(expr, operator, right, expr.line, expr.column)
        }

        return expr
    }

    private fun parseFactor(): Expression {
        var expr = parseUnary()

        while (match(STAR, SLASH, PERCENT)) {
            val operator = previous()
            val right = parseUnary()
            expr = BinaryExpr(expr, operator, right, expr.line, expr.column)
        }

        return expr
    }

    private fun parseUnary(): Expression {
        if (match(NOT, BANG, MINUS)) {
            val operator = previous()
            val right = parseUnary()
            return UnaryExpr(operator, right, operator.line, operator.column)
        }

        return parsePostfix()
    }

    private fun parsePostfix(): Expression {
        var expr = parsePrimary()

        while (true) {
            expr = when {
                match(LBRACKET) -> {
                    val index = parseExpression()
                    consume(RBRACKET, "Expected ']' after index")
                    IndexExpr(expr, index, expr.line, expr.column)
                }
                match(DOT) -> {
                    val member = consume(IDENTIFIER, "Expected member name after '.'").lexeme
                    MemberExpr(expr, member, expr.line, expr.column)
                }
                match(DOTDOT) -> {
                    val end = parsePrimary()
                    RangeExpr(expr, end, expr.line, expr.column)
                }
                else -> break
            }
        }

        return expr
    }

    private fun parsePrimary(): Expression {
        val token = peek()

        return when {
            match(INTEGER) -> LiteralExpr(previous().literal, token.line, token.column)
            match(DECIMAL) -> LiteralExpr(previous().literal, token.line, token.column)
            match(STRING) -> LiteralExpr(previous().literal, token.line, token.column)
            match(CHAR_LITERAL) -> LiteralExpr(previous().literal, token.line, token.column)
            match(TRUE) -> LiteralExpr(true, token.line, token.column)
            match(FALSE) -> LiteralExpr(false, token.line, token.column)
            match(NULL) -> LiteralExpr(null, token.line, token.column)

            match(CALL) -> {
                val callee = consume(IDENTIFIER, "Expected function name after CALL").lexeme
                consume(LPAREN, "Expected '(' after function name")
                val arguments = mutableListOf<Expression>()
                if (!check(RPAREN)) {
                    do {
                        arguments.add(parseExpression())
                    } while (match(COMMA))
                }
                consume(RPAREN, "Expected ')' after arguments")
                CallExpr(callee, arguments, token.line, token.column)
            }

            match(SELECT) -> parseSelectExpression(token)

            match(LBRACKET) -> {
                // Array literal
                val elements = mutableListOf<Expression>()
                if (!check(RBRACKET)) {
                    do {
                        elements.add(parseExpression())
                    } while (match(COMMA))
                }
                consume(RBRACKET, "Expected ']' after array elements")
                ArrayExpr(elements, token.line, token.column)
            }

            match(LBRACE) -> {
                // Map literal
                val entries = mutableListOf<Pair<Expression, Expression>>()
                if (!check(RBRACE)) {
                    do {
                        val key = parseExpression()
                        consume(COLON, "Expected ':' in map entry")
                        val value = parseExpression()
                        entries.add(Pair(key, value))
                    } while (match(COMMA))
                }
                consume(RBRACE, "Expected '}' after map entries")
                MapExpr(entries, token.line, token.column)
            }

            match(LPAREN) -> {
                val expr = parseExpression()
                consume(RPAREN, "Expected ')' after expression")
                GroupingExpr(expr, token.line, token.column)
            }

            match(IDENTIFIER) -> {
                val name = previous().lexeme

                // Check for function call without CALL keyword (optional)
                if (check(LPAREN)) {
                    advance()
                    val arguments = mutableListOf<Expression>()
                    if (!check(RPAREN)) {
                        do {
                            arguments.add(parseExpression())
                        } while (match(COMMA))
                    }
                    consume(RPAREN, "Expected ')' after arguments")
                    CallExpr(name, arguments, token.line, token.column)
                } else {
                    IdentifierExpr(name, token.line, token.column)
                }
            }

            else -> throw ParserException("Unexpected token: ${token.lexeme}", token)
        }
    }

    private fun parseSelectExpression(token: Token): SelectExpr {
        val distinct = match(DISTINCT)

        // Parse columns
        val columns = mutableListOf<SelectColumn>()
        if (match(STAR)) {
            columns.add(SelectColumn(LiteralExpr("*", token.line, token.column), null, token.line, token.column))
        } else {
            do {
                val colExpr = parseExpression()
                val alias = if (match(AS)) {
                    consume(IDENTIFIER, "Expected alias name").lexeme
                } else null
                columns.add(SelectColumn(colExpr, alias, colExpr.line, colExpr.column))
            } while (match(COMMA))
        }

        // FROM clause (optional for computed values)
        val from = if (match(FROM)) {
            consume(IDENTIFIER, "Expected table name after FROM").lexeme
        } else null

        // WHERE clause
        val where = if (match(WHERE)) {
            parseExpression()
        } else null

        // ORDER BY clause
        val orderBy = if (match(ORDER)) {
            consume(BY, "Expected BY after ORDER")
            val clauses = mutableListOf<OrderByClause>()
            do {
                val expr = parseExpression()
                val asc = !match(DESC)
                if (!asc || match(ASC)) { /* consume ASC if present */ }
                clauses.add(OrderByClause(expr, asc, expr.line, expr.column))
            } while (match(COMMA))
            clauses
        } else null

        // LIMIT clause
        val limit = if (match(LIMIT)) {
            parseExpression()
        } else null

        // OFFSET clause
        val offset = if (match(OFFSET)) {
            parseExpression()
        } else null

        return SelectExpr(columns, from, where, orderBy, limit, offset, distinct, token.line, token.column)
    }

    // ============================================
    // HELPER METHODS
    // ============================================

    private fun match(vararg types: TokenType): Boolean {
        for (type in types) {
            if (check(type)) {
                advance()
                return true
            }
        }
        return false
    }

    private fun check(type: TokenType): Boolean {
        if (isAtEnd()) return false
        return peek().type == type
    }

    private fun advance(): Token {
        if (!isAtEnd()) current++
        return previous()
    }

    private fun isAtEnd(): Boolean = peek().type == EOF

    private fun peek(): Token = tokens[current]

    private fun peekNext(): Token? {
        return if (current + 1 < tokens.size) tokens[current + 1] else null
    }

    private fun previous(): Token = tokens[current - 1]

    private fun consume(type: TokenType, message: String): Token {
        if (check(type)) return advance()
        throw ParserException(message, peek())
    }
}

class ParserException(message: String, token: Token) :
    RuntimeException("$message at line ${token.line}, column ${token.column} (found '${token.lexeme}')")