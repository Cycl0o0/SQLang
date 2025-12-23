package com.sqlang

import com.sqlang.lexer.Lexer
import com.sqlang.lexer.LexerException
import com.sqlang.parser.Parser
import com.sqlang.parser.ParserException
import com.sqlang.interpreter.Interpreter
import com.sqlang.interpreter.SQLangRuntimeException
import java.io.File
import java.util.Scanner

fun main(args: Array<String>) {
    println("""
        ╔═══════════════════════════════════════════════════════════╗
        ║                    SQLang v0.1.0                          ║
        ║           SQL-inspired Programming Language               ║
        ║                                                           ║
        ║  Usage:                                                   ║
        ║    sqlang <file.sqlang>  - Run a SQLang program           ║
        ║    sqlang                - Start interactive REPL         ║
        ╚═══════════════════════════════════════════════════════════╝
    """.trimIndent())
    println()

    if (args.isEmpty()) {
        repl()
    } else {
        runFile(args[0])
    }
}

fun runFile(path: String) {
    val file = File(path)
    if (!file.exists()) {
        System.err.println("Error: File not found: $path")
        System.exit(1)
    }

    val source = file.readText()
    run(source)
}

fun repl() {
    val scanner = Scanner(System.`in`)
    val interpreter = Interpreter()

    println("SQLang REPL - Type 'exit' to quit, 'help' for commands")
    println()

    val buffer = StringBuilder()
    var inMultiLine = false

    while (true) {
        print(if (inMultiLine) "...> " else "sql> ")

        if (!scanner.hasNextLine()) break

        val line = scanner.nextLine()

        when {
            line.trim().equals("exit", ignoreCase = true) -> {
                println("Goodbye!")
                break
            }
            line.trim().equals("help", ignoreCase = true) -> {
                printHelp()
                continue
            }
            line.trim().equals("clear", ignoreCase = true) -> {
                interpreter.reset()
                println("Environment cleared.")
                continue
            }
            line.trim().isEmpty() && !inMultiLine -> continue
        }

        buffer.append(line).append("\n")

        // Check if we need more input (simple heuristic)
        val content = buffer.toString().trim()
        if (content.endsWith(";") ||
            (content.contains("END") && !content.contains("CREATE") && !content.contains("IF") && !content.contains("WHILE") && !content.contains("FOR") && !content.contains("CASE") && !content.contains("TRY"))) {

            inMultiLine = false
            try {
                val tokens = Lexer(buffer.toString()).scanTokens()
                val statements = Parser(tokens).parse()
                interpreter.interpret(statements)
            } catch (e: LexerException) {
                System.err.println("Lexer Error: ${e.message}")
            } catch (e: ParserException) {
                System.err.println("Parser Error: ${e.message}")
            } catch (e: SQLangRuntimeException) {
                System.err.println("Runtime Error: ${e.message}")
            } catch (e: Exception) {
                System.err.println("Error: ${e.message}")
            }
            buffer.clear()
        } else if (content.contains("CREATE") || content.contains("IF") || content.contains("WHILE") || content.contains("FOR") || content.contains("CASE") || content.contains("TRY")) {
            inMultiLine = true
        }
    }
}

fun run(source: String) {
    try {
        val tokens = Lexer(source).scanTokens()
        val statements = Parser(tokens).parse()
        val interpreter = Interpreter()
        interpreter.interpret(statements)
    } catch (e: LexerException) {
        System.err.println("Lexer Error: ${e.message}")
        System.exit(65)
    } catch (e: ParserException) {
        System.err.println("Parser Error: ${e.message}")
        System.exit(65)
    } catch (e: SQLangRuntimeException) {
        System.err.println("Runtime Error: ${e.message}")
        System.exit(70)
    }
}

fun printHelp() {
    println("""
        SQLang REPL Commands:
          help   - Show this help message
          clear  - Clear the environment (reset all variables)
          exit   - Exit the REPL
        
        Quick Examples:
          DECLARE x INT = 10;
          PRINT x * 2;
          
          DECLARE names TEXT[] = ['Alice', 'Bob'];
          FOR EACH name IN names DO
              PRINT 'Hello, ' || name;
          END FOR;
          
          CREATE FUNCTION factorial(n INT) RETURNS INT AS
              IF n <= 1 THEN
                  RETURN 1;
              END IF;
              RETURN n * CALL factorial(n - 1);
          END FUNCTION;
          PRINT CALL factorial(5);
    """.trimIndent())
}