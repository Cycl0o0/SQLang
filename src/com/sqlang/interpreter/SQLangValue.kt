package com.sqlang.interpreter

import com.sqlang.parser.ast.SQLangType

sealed class SQLangValue {
    abstract fun toBoolean(): Boolean
    abstract fun toDisplayString(): String

    data class IntValue(val value: Long) : SQLangValue() {
        override fun toBoolean(): Boolean = value != 0L
        override fun toDisplayString(): String = value.toString()
        override fun toString(): String = value.toString()
    }

    data class DecimalValue(val value: Double) : SQLangValue() {
        override fun toBoolean(): Boolean = value != 0.0
        override fun toDisplayString(): String = value.toString()
        override fun toString(): String = value.toString()
    }

    data class TextValue(val value: String) : SQLangValue() {
        override fun toBoolean(): Boolean = value.isNotEmpty()
        override fun toDisplayString(): String = value
        override fun toString(): String = value
    }

    data class CharValue(val value: Char) : SQLangValue() {
        override fun toBoolean(): Boolean = true
        override fun toDisplayString(): String = value.toString()
        override fun toString(): String = value.toString()
    }

    data class BoolValue(val value: Boolean) : SQLangValue() {
        override fun toBoolean(): Boolean = value
        override fun toDisplayString(): String = if (value) "TRUE" else "FALSE"
        override fun toString(): String = toDisplayString()
    }

    data object NullValue : SQLangValue() {
        override fun toBoolean(): Boolean = false
        override fun toDisplayString(): String = "NULL"
        override fun toString(): String = "NULL"
    }

    data class ArrayValue(val elements: MutableList<SQLangValue>) : SQLangValue() {
        override fun toBoolean(): Boolean = elements.isNotEmpty()
        override fun toDisplayString(): String = "[" + elements.joinToString(", ") { it.toDisplayString() } + "]"
        override fun toString(): String = toDisplayString()

        fun get(index: Int): SQLangValue {
            if (index < 0 || index >= elements.size) {
                throw SQLangRuntimeException("Array index out of bounds: $index (size: ${elements.size})")
            }
            return elements[index]
        }

        fun set(index: Int, value: SQLangValue) {
            if (index < 0 || index >= elements.size) {
                throw SQLangRuntimeException("Array index out of bounds: $index (size: ${elements.size})")
            }
            elements[index] = value
        }

        fun size(): Int = elements.size
    }

    data class MapValue(val entries: MutableMap<SQLangValue, SQLangValue>) : SQLangValue() {
        override fun toBoolean(): Boolean = entries.isNotEmpty()
        override fun toDisplayString(): String {
            return "{" + entries.entries.joinToString(", ") {
                "${it.key.toDisplayString()}: ${it.value.toDisplayString()}"
            } + "}"
        }
        override fun toString(): String = toDisplayString()

        fun get(key: SQLangValue): SQLangValue {
            return entries[key] ?: NullValue
        }

        fun set(key: SQLangValue, value: SQLangValue) {
            entries[key] = value
        }

        fun size(): Int = entries.size
    }

    data class TableValue(
        val columns: List<String>,
        val rows: MutableList<MutableList<SQLangValue>>
    ) : SQLangValue() {
        override fun toBoolean(): Boolean = rows.isNotEmpty()
        override fun toDisplayString(): String {
            if (rows.isEmpty()) return "Empty Table (${columns.joinToString(", ")})"

            val sb = StringBuilder()
            sb.appendLine(columns.joinToString(" | "))
            sb.appendLine("-".repeat(columns.sumOf { it.length } + (columns.size - 1) * 3))
            for (row in rows) {
                sb.appendLine(row.joinToString(" | ") { it.toDisplayString() })
            }
            return sb.toString().trimEnd()
        }
        override fun toString(): String = toDisplayString()

        fun getColumnIndex(name: String): Int {
            return columns.indexOf(name).takeIf { it >= 0 }
                ?: throw SQLangRuntimeException("Unknown column: $name")
        }

        fun insert(values: List<SQLangValue>) {
            if (values.size != columns.size) {
                throw SQLangRuntimeException("Expected ${columns.size} values, got ${values.size}")
            }
            rows.add(values.toMutableList())
        }
    }

    data class RangeValue(val start: Long, val end: Long) : SQLangValue() {
        override fun toBoolean(): Boolean = true
        override fun toDisplayString(): String = "$start..$end"
        override fun toString(): String = toDisplayString()

        fun toList(): List<Long> = (start..end).toList()
    }

    data class FunctionValue(
        val name: String,
        val parameters: List<String>,
        val parameterTypes: List<SQLangType>,
        val body: List<com.sqlang.parser.ast.Statement>,
        val closure: Environment
    ) : SQLangValue() {
        override fun toBoolean(): Boolean = true
        override fun toDisplayString(): String = "<function $name>"
        override fun toString(): String = toDisplayString()
    }

    data class ProcedureValue(
        val name: String,
        val parameters: List<String>,
        val parameterTypes: List<SQLangType>,
        val body: List<com.sqlang.parser.ast.Statement>,
        val closure: Environment
    ) : SQLangValue() {
        override fun toBoolean(): Boolean = true
        override fun toDisplayString(): String = "<procedure $name>"
        override fun toString(): String = toDisplayString()
    }
}

class SQLangRuntimeException(message: String) : RuntimeException(message)

class ReturnException(val value: SQLangValue?) : RuntimeException()
class BreakException : RuntimeException()
class ContinueException : RuntimeException()
class ThrowException(val value: SQLangValue) : RuntimeException(value.toDisplayString())