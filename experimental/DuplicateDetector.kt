import java.io.File

fun main(args: Array<String>) {
    val verbose = args.contains("--verbose")
    val rootPath = args.firstOrNull { it != "--verbose" } ?: "."
    val root = File(rootPath).absoluteFile
    if (!root.isDirectory) {
        println("Error: '$rootPath' is not a directory.")
        return
    }
    val jsExtensions = setOf("js", "mjs", "jsx")
    val cssExtensions = setOf("css")
    val jsKeywords = setOf(
        "if", "for", "while", "catch", "switch", "function", "class", 
        "const", "let", "var", "forEach", "addEventListener", "removeEventListener",
        "async", "await"
    )
    val cssBlacklist = setOf(
        "*", "html", "body", "img", "a", "p", "div", "span", "h1", "h2", "h3", "h4", "h5", "h6"
    )
    val files = root.walkTopDown()
        .filter { it.isFile && it.extension.lowercase() in (jsExtensions + cssExtensions) }
        .toList()
    val jsFuncPatterns = listOf(
        Regex("""\b(?:async\s+)?function\s+(\w+)\s*\("""),
        Regex("""\b(?:var|let|const)\s+(\w+)\s*=\s*(?:async\s*)?function\s*\("""),
        Regex("""\b(?:var|let|const)\s+(\w+)\s*=\s*(?:async\s*)?\(.*?\)\s*=>\s*\{"""),
        Regex("""(\w+)\s*:\s*(?:async\s*)?function\s*\("""),
        Regex("""\b(?:async\s+)?(\w+)\s*\(.*?\)\s*\{(?!\s*\/)"""),   
        Regex("""\bstatic\s+(?:async\s+)?(\w+)\s*\(.*?\)\s*\{"""),
        Regex("""\b(?:get|set)\s+(\w+)\s*\(.*?\)\s*\{""")
    )
    fun stripJsCommentsAndStrings(code: String): String {
        var result = code
        result = Regex("//[^\n]*").replace(result) { m -> " ".repeat(m.value.length) }
        result = Regex("/\\*.*?\\*/", RegexOption.DOT_MATCHES_ALL).replace(result) { m -> " ".repeat(m.value.length) }
        result = Regex("""(['"])(?:\\.|.)*?\1""").replace(result) { m -> " ".repeat(m.value.length) }
        return result
    }
    val jsDuplicates = mutableMapOf<String, MutableMap<String, MutableList<Int>>>()
    files.filter { it.extension.lowercase() in jsExtensions }.forEach { file ->
        val raw = file.readText()
        val cleaned = stripJsCommentsAndStrings(raw)
        if (verbose) println("\nScanning ${file.name} (JS)")
        val foundOffsets = mutableSetOf<Int>()
        jsFuncPatterns.forEach { pattern ->
            pattern.findAll(cleaned).forEach { match ->
                val name = match.groupValues[1]
                val offset = match.range.first
                if (offset in foundOffsets || name in jsKeywords) return@forEach
                foundOffsets.add(offset)
                val lineNum = raw.substring(0, offset.coerceAtMost(raw.length)).count { it == '\n' } + 1
                jsDuplicates.getOrPut(name) { mutableMapOf() }
                            .getOrPut(file.absolutePath) { mutableListOf() }
                            .add(lineNum)
                if (verbose) println("  Found function: $name -> ${file.name}:$lineNum")
            }
        }
    }
    val cssRulePattern = Regex("""([^\{\}]+)\s*\{\s*[^\{\}]*?\s*\}""", RegexOption.MULTILINE)
    val cssDuplicates = mutableMapOf<String, MutableMap<String, MutableList<Int>>>()
    files.filter { it.extension.lowercase() in cssExtensions }.forEach { file ->
        val text = file.readText()
        val noComments = Regex("""/\*.*?\*/""", RegexOption.DOT_MATCHES_ALL).replace(text) { m -> " ".repeat(m.value.length) }
        if (verbose) println("\nScanning ${file.name} (CSS)")
        cssRulePattern.findAll(noComments).forEach { match ->
            val rawSelector = match.groupValues[1]
            val offset = match.range.first
            val lineNum = text.substring(0, offset.coerceAtMost(text.length)).count { it == '\n' } + 1
            val individualSelectors = rawSelector.split(",").map { it.trim().replace(Regex("""\s+"""), " ") }
            individualSelectors.forEach { selector ->
                if (selector.startsWith("@") || selector.isBlank() || selector in cssBlacklist) return@forEach
                cssDuplicates.getOrPut(selector) { mutableMapOf() }
                            .getOrPut(file.absolutePath) { mutableListOf() }
                            .add(lineNum)
                if (verbose) println("  Found selector: '$selector' -> ${file.name}:$lineNum")
            }
        }
    }
    println("\n" + "=".repeat(60))
    println("Duplicate Detection Report (${root.path})")
    println("=".repeat(60))
    var dupFound = false
    jsDuplicates.filter { it.value.keys.size > 1 }.toSortedMap().forEach { (name, fileMap) ->
        dupFound = true
        println("\n⚡ Duplicate JavaScript function: '$name'")
        fileMap.forEach { (filePath, lines) ->
            lines.forEach { line -> println("   $filePath:$line") }
        }
    }
    cssDuplicates.filter { it.value.keys.size > 1 }.toSortedMap().forEach { (selector, fileMap) ->
        dupFound = true
        println("\n⚡ Duplicate CSS selector: '$selector'")
        fileMap.forEach { (filePath, lines) ->
            lines.forEach { line -> println("   $filePath:$line") }
        }
    }
    if (!dupFound) {
        println("\n✅ No file-spanning naming conflicts found.")
    }
}