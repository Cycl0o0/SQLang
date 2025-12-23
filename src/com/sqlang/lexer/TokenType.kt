package com.sqlang.lexer

enum class TokenType {
    // Literals
    INTEGER,
    DECIMAL,
    STRING,
    CHAR_LITERAL,
    TRUE,
    FALSE,
    NULL,

    // Identifiers
    IDENTIFIER,

    // Keywords - Program structure
    CREATE,
    PROGRAM,
    END,

    // Keywords - Variables
    DECLARE,
    CONST,
    SET,
    AUTO,

    // Keywords - Types
    INT,
    DECIMAL_TYPE,
    TEXT,
    BOOL,
    CHAR,
    VOID,
    TABLE,
    MAP,

    // Keywords - Control flow
    IF,
    THEN,
    ELSE,
    CASE,
    WHEN,
    DEFAULT,
    WHILE,
    DO,
    FOR,
    EACH,
    IN,
    LOOP,
    BREAK,
    CONTINUE,

    // Keywords - Functions
    FUNCTION,
    PROCEDURE,
    RETURNS,
    RETURN,
    CALL,
    EXEC,
    AS,

    // Keywords - SQL operations
    SELECT,
    FROM,
    WHERE,
    INSERT,
    INTO,
    VALUES,
    UPDATE,
    DELETE,
    ORDER,
    BY,
    ASC,
    DESC,
    LIMIT,
    OFFSET,
    JOIN,
    LEFT,
    RIGHT,
    INNER,
    OUTER,
    ON,
    GROUP,
    HAVING,
    DISTINCT,
    AND,
    OR,
    NOT,
    IS,
    LIKE,
    BETWEEN,
    PRIMARY,
    KEY,

    // Keywords - I/O
    PRINT,
    INPUT,

    // Keywords - Exceptions
    TRY,
    CATCH,
    THROW,
    FINALLY,

    // Operators - Arithmetic
    PLUS,           // +
    MINUS,          // -
    STAR,           // *
    SLASH,          // /
    PERCENT,        // %

    // Operators - Comparison
    EQUAL,          // =
    NOT_EQUAL,      // != or <>
    LESS,           // <
    LESS_EQUAL,     // <=
    GREATER,        // >
    GREATER_EQUAL,  // >=

    // Operators - Logical
    AMPERSAND,      // && (alternative to AND)
    PIPE,           // || (string concat or alternative to OR)
    BANG,           // !

    // Delimiters
    LPAREN,         // (
    RPAREN,         // )
    LBRACKET,       // [
    RBRACKET,       // ]
    LBRACE,         // {
    RBRACE,         // }
    COMMA,          // ,
    SEMICOLON,      // ;
    COLON,          // :
    DOT,            // .
    DOTDOT,         // ..
    QUESTION,       // ?
    AT,             // @
    ARROW,          // ->

    // Special
    EOF,
    NEWLINE,
    COMMENT
}