package com.sqlang.lexer

class Lexer(private val source: String) {
    private val tokens = mutableListOf<Token>()
    private var start = 0
    private var current = 0
    private var line = 1
    private var column = 1
    private var startColumn = 1

    companion object {
        private val keywords = mapOf(
            // Program structure
            "CREATE" to TokenType.CREATE,
            "PROGRAM" to TokenType.PROGRAM,
            "END" to TokenType.END,

            // Variables
            "DECLARE" to TokenType.DECLARE,
            "CONST" to TokenType.CONST,
            "SET" to TokenType.SET,
            "AUTO" to TokenType.AUTO,

            // Types
            "INT" to TokenType.INT,
            "DECIMAL" to TokenType.DECIMAL_TYPE,
            "TEXT" to TokenType.TEXT,
            "BOOL" to TokenType.BOOL,
            "CHAR" to TokenType.CHAR,
            "VOID" to TokenType.VOID,
            "TABLE" to TokenType.TABLE,
            "MAP" to TokenType.MAP,

            // Booleans & Null
            "TRUE" to TokenType.TRUE,
            "FALSE" to TokenType.FALSE,
            "NULL" to TokenType.NULL,

            // Control flow
            "IF" to TokenType.IF,
            "THEN" to TokenType.THEN,
            "ELSE" to TokenType.ELSE,
            "CASE" to TokenType.CASE,
            "WHEN" to TokenType.WHEN,
            "DEFAULT" to TokenType.DEFAULT,
            "WHILE" to TokenType.WHILE,
            "DO" to TokenType.DO,
            "FOR" to TokenType.FOR,
            "EACH" to TokenType.EACH,
            "IN" to TokenType.IN,
            "LOOP" to TokenType.LOOP,
            "BREAK" to TokenType.BREAK,
            "CONTINUE" to TokenType.CONTINUE,

            // Functions
            "FUNCTION" to TokenType.FUNCTION,
            "PROCEDURE" to TokenType.PROCEDURE,
            "RETURNS" to TokenType.RETURNS,
            "RETURN" to TokenType.RETURN,
            "CALL" to TokenType.CALL,
            "EXEC" to TokenType.EXEC,
            "AS" to TokenType.AS,

            // SQL operations
            "SELECT" to TokenType.SELECT,
            "FROM" to TokenType.FROM,
            "WHERE" to TokenType.WHERE,
            "INSERT" to TokenType.INSERT,
            "INTO" to TokenType.INTO,
            "VALUES" to TokenType.VALUES,
            "UPDATE" to TokenType.UPDATE,
            "DELETE" to TokenType.DELETE,
            "ORDER" to TokenType.ORDER,
            "BY" to TokenType.BY,
            "ASC" to TokenType.ASC,
            "DESC" to TokenType.DESC,
            "LIMIT" to TokenType.LIMIT,
            "OFFSET" to TokenType.OFFSET,
            "JOIN" to TokenType.JOIN,
            "LEFT" to TokenType.LEFT,
            "RIGHT" to TokenType.RIGHT,
            "INNER" to TokenType.INNER,
            "OUTER" to TokenType.OUTER,
            "ON" to TokenType.ON,
            "GROUP" to TokenType.GROUP,
            "HAVING" to TokenType.HAVING,
            "DISTINCT" to TokenType.DISTINCT,
            "AND" to TokenType.AND,
            "OR" to TokenType.OR,
            "NOT" to TokenType.NOT,
            "IS" to TokenType.IS,
            "LIKE" to TokenType.LIKE,
            "BETWEEN" to TokenType.BETWEEN,
            "PRIMARY" to TokenType.PRIMARY,
            "KEY" to TokenType.KEY,

            // I/O
            "PRINT" to TokenType.PRINT,
            "INPUT" to TokenType.INPUT,

            // Exceptions
            "TRY" to TokenType.TRY,
            "CATCH" to TokenType.CATCH,
            "THROW" to TokenType.THROW,
            "FINALLY" to TokenType.FINALLY
        )
    }

    fun scanTokens(): List<Token> {
        while (!isAtEnd()) {
            start = current
            startColumn = column
            scanToken()
        }
        tokens.add(Token(TokenType.EOF, "", null, line, column))
        return tokens
    }

    private fun scanToken() {
        when (val c = advance()) {
            '(' -> addToken(TokenType.LPAREN)
            ')' -> addToken(TokenType.RPAREN)
            '[' -> addToken(TokenType.LBRACKET)
            ']' -> addToken(TokenType.RBRACKET)
            '{' -> addToken(TokenType.LBRACE)
            '}' -> addToken(TokenType.RBRACE)
            ',' -> addToken(TokenType.COMMA)
            ';' -> addToken(TokenType.SEMICOLON)
            ':' -> addToken(TokenType.COLON)
            '?' -> addToken(TokenType.QUESTION)
            '@' -> addToken(TokenType.AT)
            '+' -> addToken(TokenType.PLUS)
            '*' -> addToken(TokenType.STAR)
            '%' -> addToken(TokenType.PERCENT)

            '.' -> {
                if (match('.')) {
                    addToken(TokenType.DOTDOT)
                } else {
                    addToken(TokenType.DOT)
                }
            }

            '-' -> {
                if (match('-')) {
                    // Single line comment
                    while (peek() != '\n' && !isAtEnd()) advance()
                } else if (match('>')) {
                    addToken(TokenType.ARROW)
                } else {
                    addToken(TokenType.MINUS)
                }
            }

            '/' -> {
                if (match('*')) {
                    // Block comment
                    blockComment()
                } else {
                    addToken(TokenType.SLASH)
                }
            }

            '=' -> addToken(TokenType.EQUAL)

            '!' -> {
                if (match('=')) {
                    addToken(TokenType.NOT_EQUAL)
                } else {
                    addToken(TokenType.BANG)
                }
            }

            '<' -> {
                when {
                    match('=') -> addToken(TokenType.LESS_EQUAL)
                    match('>') -> addToken(TokenType.NOT_EQUAL)
                    else -> addToken(TokenType.LESS)
                }
            }

            '>' -> {
                if (match('=')) {
                    addToken(TokenType.GREATER_EQUAL)
                } else {
                    addToken(TokenType.GREATER)
                }
            }

            '&' -> {
                if (match('&')) {
                    addToken(TokenType.AMPERSAND)
                } else {
                    throw LexerException("Unexpected character '&' at line $line, column $startColumn. Did you mean '&&'?")
                }
            }

            '|' -> {
                if (match('|')) {
                    addToken(TokenType.PIPE)
                } else {
                    throw LexerException("Unexpected character '|' at line $line, column $startColumn. Did you mean '||'?")
                }
            }

            '\'' -> charLiteral()
            '"' -> string()

            ' ', '\r', '\t' -> { /* Ignore whitespace */ }

            '\n' -> {
                line++
                column = 1
            }

            else -> {
                when {
                    c.isDigit() -> number()
                    c.isLetter() || c == '_' -> identifier()
                    else -> throw LexerException("Unexpected character '$c' at line $line, column $startColumn")
                }
            }
        }
    }

    private fun identifier() {
        while (peek().isLetterOrDigit() || peek() == '_') advance()

        val text = source.substring(start, current)
        val type = keywords[text.uppercase()] ?: TokenType.IDENTIFIER
        addToken(type)
    }

    private fun number() {
        while (peek().isDigit()) advance()

        // Look for decimal part
        if (peek() == '.' && peekNext().isDigit()) {
            advance() // consume '.'
            while (peek().isDigit()) advance()

            val value = source.substring(start, current).toDouble()
            addToken(TokenType.DECIMAL, value)
        } else {
            val value = source.substring(start, current).toLong()
            addToken(TokenType.INTEGER, value)
        }
    }

    private fun string() {
        val builder = StringBuilder()

        while (peek() != '"' && !isAtEnd()) {
            if (peek() == '\n') {
                line++
                column = 1
            }
            if (peek() == '\\') {
                advance() // consume backslash
                when (val escaped = advance()) {
                    'n' -> builder.append('\n')
                    't' -> builder.append('\t')
                    'r' -> builder.append('\r')
                    '\\' -> builder.append('\\')
                    '"' -> builder.append('"')
                    '\'' -> builder.append('\'')
                    else -> throw LexerException("Invalid escape sequence '\\$escaped' at line $line")
                }
            } else {
                builder.append(advance())
            }
        }

        if (isAtEnd()) {
            throw LexerException("Unterminated string at line $line")
        }

        advance() // closing "
        addToken(TokenType.STRING, builder.toString())
    }

    private fun charLiteral() {
        val c = if (peek() == '\\') {
            advance() // consume backslash
            when (val escaped = advance()) {
                'n' -> '\n'
                't' -> '\t'
                'r' -> '\r'
                '\\' -> '\\'
                '\'' -> '\''
                '"' -> '"'
                else -> throw LexerException("Invalid escape sequence '\\$escaped' at line $line")
            }
        } else {
            advance()
        }

        if (peek() != '\'') {
            throw LexerException("Unterminated char literal at line $line")
        }
        advance() // closing '

        addToken(TokenType.CHAR_LITERAL, c)
    }

    private fun blockComment() {
        var depth = 1
        while (depth > 0 && !isAtEnd()) {
            when {
                peek() == '/' && peekNext() == '*' -> {
                    advance()
                    advance()
                    depth++
                }
                peek() == '*' && peekNext() == '/' -> {
                    advance()
                    advance()
                    depth--
                }
                peek() == '\n' -> {
                    line++
                    column = 1
                    advance()
                }
                else -> advance()
            }
        }

        if (depth > 0) {
            throw LexerException("Unterminated block comment")
        }
    }

    private fun match(expected: Char): Boolean {
        if (isAtEnd()) return false
        if (source[current] != expected) return false
        current++
        column++
        return true
    }

    private fun peek(): Char {
        if (isAtEnd()) return '\u0000'
        return source[current]
    }

    private fun peekNext(): Char {
        if (current + 1 >= source.length) return '\u0000'
        return source[current + 1]
    }

    private fun advance(): Char {
        val c = source[current]
        current++
        column++
        return c
    }

    private fun isAtEnd(): Boolean = current >= source.length

    private fun addToken(type: TokenType, literal: Any? = null) {
        val text = source.substring(start, current)
        tokens.add(Token(type, text, literal, line, startColumn))
    }
}

class LexerException(message: String) : RuntimeException(message)