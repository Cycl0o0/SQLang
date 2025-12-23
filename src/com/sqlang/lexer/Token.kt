package com.sqlang.lexer

import com.sqlang.lexer.TokenType

data class Token(
    val type: TokenType,
    val lexeme: String,
    val literal: Any?,
    val line: Int,
    val column: Int
) {
    override fun toString(): String {
        return "Token($type, '$lexeme', $literal, line=$line, col=$column)"
    }
}