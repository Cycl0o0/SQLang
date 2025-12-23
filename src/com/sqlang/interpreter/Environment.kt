package com.sqlang.interpreter

class Environment(private val parent: Environment? = null) {
    private val variables = mutableMapOf<String, SQLangValue>()
    private val constants = mutableSetOf<String>()
    private val tables = mutableMapOf<String, SQLangValue.TableValue>()

    fun define(name: String, value: SQLangValue, isConst: Boolean = false) {
        if (variables.containsKey(name)) {
            throw SQLangRuntimeException("Variable '$name' is already defined in this scope")
        }
        variables[name] = value
        if (isConst) {
            constants.add(name)
        }
    }

    fun get(name: String): SQLangValue {
        return variables[name]
            ?: parent?.get(name)
            ?: throw SQLangRuntimeException("Undefined variable: $name")
    }

    fun set(name: String, value: SQLangValue) {
        if (constants.contains(name)) {
            throw SQLangRuntimeException("Cannot reassign constant: $name")
        }

        when {
            variables.containsKey(name) -> variables[name] = value
            parent != null -> parent.set(name, value)
            else -> throw SQLangRuntimeException("Undefined variable: $name")
        }
    }

    fun isDefined(name: String): Boolean {
        return variables.containsKey(name) || (parent?.isDefined(name) ?: false)
    }

    fun defineTable(name: String, table: SQLangValue.TableValue) {
        tables[name] = table
    }

    fun getTable(name: String): SQLangValue.TableValue {
        return tables[name]
            ?: parent?.getTable(name)
            ?: throw SQLangRuntimeException("Undefined table: $name")
    }

    fun hasTable(name: String): Boolean {
        return tables.containsKey(name) || (parent?.hasTable(name) ?: false)
    }

    fun createChild(): Environment = Environment(this)

    fun dump(): String {
        val sb = StringBuilder()
        sb.appendLine("=== Environment ===")
        for ((name, value) in variables) {
            val constMarker = if (constants.contains(name)) " (const)" else ""
            sb.appendLine("  $name$constMarker = ${value.toDisplayString()}")
        }
        for ((name, table) in tables) {
            sb.appendLine("  TABLE $name: ${table.columns.joinToString(", ")}")
        }
        if (parent != null) {
            sb.appendLine("--- Parent ---")
            sb.append(parent.dump())
        }
        return sb.toString()
    }
}