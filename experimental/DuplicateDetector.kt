import java.io.File
import kotlin.system.measureTimeMillis

fun main(args: Array<String>) {
    val options = parseArgs(args)
    
    if (options["help"] == true) {
        printHelp()
        return
    }
    
    val verbose = options["verbose"] == true
    val outputJson = options["json"] == true
    val showContext = options["context"] == true
    val minOccurrences = (options["min"] as? String)?.toIntOrNull() ?: 2
    val excludePatterns = options["exclude"] as? List<String> ?: emptyList()
    val rootPath = args.firstOrNull { !it.startsWith("--") } ?: "."
    
    val root = File(rootPath).absoluteFile
    if (!root.isDirectory) {
        println("Error: '$rootPath' is not a directory.")
        return
    }
    
    val jsExtensions = setOf("js", "mjs", "jsx", "ts", "tsx", "mts", "cts")
    val cssExtensions = setOf("css", "scss", "sass", "less")
    
    val jsKeywords = setOf(
        "if", "for", "while", "catch", "do", "switch", "try", "finally",
        "function", "class", "constructor", "interface", "type", "enum",
        "const", "let", "var", "async", "await", "yield", "return", "break",
        "continue", "throw", "new", "delete", "typeof", "instanceof", "void",
        "in", "of", "from", "as", "get", "set", "static", "extends", "implements",
        "public", "private", "protected", "readonly", "abstract", "override",
        "forEach", "map", "filter", "reduce", "some", "every", "find", "findIndex",
        "includes", "indexOf", "push", "pop", "shift", "unshift", "splice", "slice",
        "addEventListener", "removeEventListener", "dispatchEvent", "createEvent",
        "querySelector", "querySelectorAll", "getElementById", "getElementsByClassName",
        "setTimeout", "setInterval", "clearTimeout", "clearInterval", "requestAnimationFrame",
        "console", "document", "window", "Math", "JSON", "Array", "Object", "String",
        "Number", "Boolean", "Symbol", "Promise", "Map", "Set", "WeakMap", "WeakSet",
        "parseInt", "parseFloat", "isNaN", "isFinite", "encodeURI", "decodeURI",
        "apply", "call", "bind", "then", "catch", "finally", "resolve", "reject"
    )
    
    val cssBlacklist = setOf(
        "*", "html", "body", "head", "img", "a", "p", "br", "hr",
        "div", "span", "h1", "h2", "h3", "h4", "h5", "h6",
        "ul", "ol", "li", "dl", "dt", "dd", "table", "tr", "td", "th",
        "form", "input", "button", "select", "option", "textarea", "label",
        "header", "footer", "nav", "main", "section", "article", "aside",
        "figure", "figcaption", "canvas", "video", "audio", "source", "track",
        "script", "style", "link", "meta", "title", "base", "blockquote",
        "pre", "code", "kbd", "samp", "var", "abbr", "cite", "q", "mark",
        "time", "meter", "progress", "details", "summary", "dialog"
    )
    
    val defaultExcludes = setOf(
        "node_modules", ".git", ".svn", ".hg", "dist", "build", "target",
        ".cache", ".tmp", ".temp", "vendor", "bower_components", ".idea",
        ".vscode", ".vs", "coverage", ".nyc_output", ".parcel-cache",
        "__pycache__", ".next", ".nuxt", ".svelte-kit"
    )
    
    val allExcludes = defaultExcludes + excludePatterns.toSet()
    
    if (verbose) {
        println("DuplicateDetector v2.0")
        println("   Root: ${root.path}")
        println("   Min occurrences: $minOccurrences")
        println("   Excludes: $allExcludes")
        println()
    }
    
    val scanTime = measureTimeMillis {
        val files = collectFiles(root, jsExtensions, cssExtensions, allExcludes, verbose)
        
        if (verbose) {
            println("Found ${files.first.size} JS/TS files and ${files.second.size} CSS files")
            println()
        }
        
        val jsResults = scanJsFiles(files.first, jsKeywords, verbose, showContext)
        val cssResults = scanCssFiles(files.second, verbose, showContext)
        
        printResults(jsResults, cssResults, minOccurrences, outputJson, root.path)
    }
    
    if (verbose) {
        println("\nScan completed in ${scanTime}ms")
    }
}

fun parseArgs(args: Array<String>): Map<String, Any> {
    val options = mutableMapOf<String, Any>()
    val excludes = mutableListOf<String>()
    
    for (arg in args) {
        when {
            arg == "--verbose" -> options["verbose"] = true
            arg == "--json" -> options["json"] = true
            arg == "--context" -> options["context"] = true
            arg == "--help" -> options["help"] = true
            arg.startsWith("--min=") -> options["min"] = arg.substring(6)
            arg.startsWith("--exclude=") -> excludes.add(arg.substring(10))
        }
    }
    
    if (excludes.isNotEmpty()) {
        options["exclude"] = excludes
    }
    
    return options
}

fun printHelp() {
    println("""
        |DuplicateDetector v2.0
        |
        |Scans JavaScript/TypeScript and CSS files for duplicate function names
        |and CSS selectors across multiple files.
        |
        |Usage:
        |  kotlin DuplicateDetector.kt [options] [path]
        |
        |Options:
        |  --verbose       Show detailed scanning progress
        |  --json          Output results in JSON format
        |  --min=N         Only report items appearing in N or more files (default: 2)
        |  --context       Show code snippet context for each duplicate
        |  --exclude=PAT   Exclude paths matching pattern (can be repeated)
        |  --help          Show this help message
        |
        |Examples:
        |  kotlin DuplicateDetector.kt .
        |  kotlin DuplicateDetector.kt --verbose --json --min=3 ./src
        |  kotlin DuplicateDetector.kt --exclude=node_modules --exclude=dist .
        |
    """.trimMargin())
}

data class DuplicateEntry(
    val name: String,
    val locations: Map<String, List<Pair<Int, String?>>>
)

fun collectFiles(
    root: File,
    jsExtensions: Set<String>,
    cssExtensions: Set<String>,
    excludes: Set<String>,
    verbose: Boolean
): Pair<List<File>, List<File>> {
    val jsFiles = mutableListOf<File>()
    val cssFiles = mutableListOf<File>()
    
    root.walkTopDown().forEach { file ->
        if (file.isFile) {
            val path = file.absolutePath
            val shouldExclude = excludes.any { ex -> 
                path.contains(File.separator + ex + File.separator) || 
                path.endsWith(File.separator + ex)
            }
            
            if (!shouldExclude) {
                when (file.extension.lowercase()) {
                    in jsExtensions -> jsFiles.add(file)
                    in cssExtensions -> cssFiles.add(file)
                }
            }
        }
    }
    
    return Pair(jsFiles, cssFiles)
}

fun stripJsCommentsAndStrings(code: String): String {
    var result = code
    result = Regex("//[^\n\r]*").replace(result) { m -> " ".repeat(m.value.length) }
    result = Regex("/\\*[^*]*\\*+(?:[^/*][^*]*\\*+)*/").replace(result) { m -> " ".repeat(m.value.length) }
    result = Regex("""`(?:[^`\\]|\\.)*`""").replace(result) { m -> " ".repeat(m.value.length) }
    result = Regex("""'(?:[^'\\]|\\.)*'""").replace(result) { m -> " ".repeat(m.value.length) }
    result = Regex(""""(?:[^"\\]|\\.)*"""").replace(result) { m -> " ".repeat(m.value.length) }
    return result
}

fun getContext(code: String, lineNum: Int, contextLines: Int = 2): String {
    val lines = code.lines()
    val start = maxOf(0, lineNum - contextLines - 1)
    val end = minOf(lines.size, lineNum + contextLines)
    return lines.subList(start, end).mapIndexed { idx, line ->
        val currentLine = start + idx + 1
        val marker = if (currentLine == lineNum) ">>>" else "   "
        "$marker $line"
    }.joinToString("\n")
}

fun scanJsFiles(
    files: List<File>,
    keywords: Set<String>,
    verbose: Boolean,
    showContext: Boolean
): MutableMap<String, MutableMap<String, MutableList<Pair<Int, String?>>>> {
    val duplicates = mutableMapOf<String, MutableMap<String, MutableList<Pair<Int, String?>>>>()
    
    val jsFuncPatterns = listOf(
        Regex("""\b(?:async\s+)?function\s+(\w+)\s*\("""),
        Regex("""\b(?:var|let|const)\s+(\w+)\s*=\s*(?:async\s*)?function\s*\("""),
        Regex("""\b(?:var|let|const)\s+(\w+)\s*=\s*(?:async\s*)?\([^)]*\)\s*=>\s*[{(]"""),
        Regex("""(\w+)\s*\([^)]*\)\s*:\s*(?:async\s*)?function\s*\("""),
        Regex("""\b(?:async\s+)?(\w+)\s*\([^)]*\)\s*(?::\s*\w+)?\s*\{(?:\s*\/\/[^\n]*)?\s*(?!\s*\{)"""),
        Regex("""\bstatic\s+(?:async\s+)?(\w+)\s*\([^)]*\)"""),
        Regex("""\b(?:get|set)\s+(\w+)\s*\([^)]*\)"""),
        Regex("""(\w+)\s*\([^)]*\)\s*:\s*(?:\w+|\w+<[^>]+>|\([^)]*\)\s*=>\s*\w+)"""),
        Regex("""\bexport\s+default\s+(?:async\s+)?function\s+(\w+)"""),
        Regex("""\bexport\s+(?:async\s+)?function\s+(\w+)"""),
        Regex("""\bexport\s+const\s+(\w+)\s*=\s*(?:async\s*)?\([^)]*\)\s*=>"""),
        Regex("""\bexport\s+const\s+(\w+)\s*=\s*(?:async\s*)?function"""),
        Regex("""\b(?:private|public|protected)\s+(?:async\s+)?(\w+)\s*\([^)]*\)"""),
        Regex("""\babstract\s+(?:async\s+)?(\w+)\s*\([^)]*\)"""),
    )
    
    files.forEachIndexed { idx, file ->
        if (verbose) {
            print("\r   Scanning JS/TS: ${idx + 1}/${files.size} - ${file.name}")
        }
        
        try {
            val raw = file.readText()
            val cleaned = stripJsCommentsAndStrings(raw)
            val foundOffsets = mutableSetOf<Int>()
            
            jsFuncPatterns.forEach { pattern ->
                pattern.findAll(cleaned).forEach { match ->
                    val name = match.groupValues[1]
                    val offset = match.range.first
                    
                    if (offset in foundOffsets || name in keywords) return@forEach
                    if (name.first().isUpperCase() && name.any { it.isLowerCase() }) return@forEach
                    if (name.length < 2) return@forEach
                    
                    foundOffsets.add(offset)
                    val lineNum = raw.substring(0, offset.coerceAtMost(raw.length)).count { it == '\n' } + 1
                    val context = if (showContext) getContext(raw, lineNum) else null
                    
                    duplicates.getOrPut(name) { mutableMapOf() }
                        .getOrPut(file.absolutePath) { mutableListOf() }
                        .add(Pair(lineNum, context))
                }
            }
        } catch (e: Exception) {
            if (verbose) println("\n   Warning: Could not read ${file.name}: ${e.message}")
        }
    }
    
    if (verbose) println()
    return duplicates
}

fun scanCssFiles(
    files: List<File>,
    verbose: Boolean,
    showContext: Boolean
): MutableMap<String, MutableMap<String, MutableList<Pair<Int, String?>>>> {
    val duplicates = mutableMapOf<String, MutableMap<String, MutableList<Pair<Int, String?>>>>()
    
    val cssRulePattern = Regex("""([^{}+>~[\],]+)\s*\{""")
    
    files.forEachIndexed { idx, file ->
        if (verbose) {
            print("\r   Scanning CSS: ${idx + 1}/${files.size} - ${file.name}")
        }
        
        try {
            val text = file.readText()
            val noComments = Regex("""/\*[^*]*\*+(?:[^/*][^*]*\*+)*/""").replace(text) { m -> " ".repeat(m.value.length) }
            
            cssRulePattern.findAll(noComments).forEach { match ->
                val rawSelector = match.groupValues[1]
                val offset = match.range.first
                val lineNum = text.substring(0, offset.coerceAtMost(text.length)).count { it == '\n' } + 1
                
                val individualSelectors = rawSelector.split(",").map { s ->
                    s.trim()
                        .replace(Regex("""\s+"""), " ")
                        .replace(Regex("""\s*\+\s*"""), "+")
                        .replace(Regex("""\s*~\s*"""), "~")
                        .replace(Regex("""\s*>\s*"""), ">")
                }
                
                individualSelectors.forEach { selector ->
                    if (selector.startsWith("@") || selector.isBlank()) return@forEach
                    if (selector == "*" || selector in cssBlacklist) return@forEach
                    if (selector.startsWith(":") && !selector.contains("(")) return@forEach
                    if (selector.startsWith("[") && selector.count { it == '[' } == 1) return@forEach
                    
                    val context = if (showContext) getContext(text, lineNum) else null
                    
                    duplicates.getOrPut(selector) { mutableMapOf() }
                        .getOrPut(file.absolutePath) { mutableListOf() }
                        .add(Pair(lineNum, context))
                }
            }
        } catch (e: Exception) {
            if (verbose) println("\n   Warning: Could not read ${file.name}: ${e.message}")
        }
    }
    
    if (verbose) println()
    return duplicates
}

fun printResults(
    jsResults: Map<String, Map<String, List<Pair<Int, String?>>>>,
    cssResults: Map<String, Map<String, List<Pair<Int, String?>>>>,
    minOccurrences: Int,
    outputJson: Boolean,
    rootPath: String
) {
    val jsDuplicates = jsResults.filter { it.value.keys.size >= minOccurrences }
    val cssDuplicates = cssResults.filter { it.value.keys.size >= minOccurrences }
    
    if (outputJson) {
        printJsonOutput(jsDuplicates, cssDuplicates, rootPath)
    } else {
        printTextOutput(jsDuplicates, cssDuplicates, rootPath)
    }
}

fun printTextOutput(
    jsDuplicates: Map<String, Map<String, List<Pair<Int, String?>>>>,
    cssDuplicates: Map<String, Map<String, List<Pair<Int, String?>>>>,
    rootPath: String
) {
    val totalJsDupes = jsDuplicates.size
    val totalCssDupes = cssDuplicates.size
    val totalLocations = jsDuplicates.values.sumOf { it.values.sumOf { list -> list.size } } +
                         cssDuplicates.values.sumOf { it.values.sumOf { list -> list.size } }
    
    println("\n" + "═".repeat(70))
    println("  Duplicate Detection Report")
    println("  Root: $rootPath")
    println("═".repeat(70))
    println()
    println("  Summary:")
    println("     * Duplicate JS/TS functions: $totalJsDupes")
    println("     * Duplicate CSS selectors:   $totalCssDupes")
    println("     * Total duplicate locations:  $totalLocations")
    println()
    
    var dupFound = totalJsDupes > 0 || totalCssDupes > 0
    
    if (jsDuplicates.isNotEmpty()) {
        println("-".repeat(70))
        println("  JavaScript/TypeScript Functions")
        println("-".repeat(70))
        
        jsDuplicates.toSortedMap().forEach { (name, fileMap) ->
            println()
            println("  Function: '$name'")
            println("  Files: ${fileMap.keys.size}")
            
            fileMap.toSortedMap().forEach { (filePath, locations) ->
                val fileName = File(filePath).name
                locations.sortedBy { it.first }.forEach { (line, context) ->
                    println("     @ $fileName:$line")
                    context?.let {
                        println("     " + it.replace("\n", "\n     "))
                    }
                }
            }
        }
    }
    
    if (cssDuplicates.isNotEmpty()) {
        println()
        println("-".repeat(70))
        println("  CSS Selectors")
        println("-".repeat(70))
        
        cssDuplicates.toSortedMap().forEach { (selector, fileMap) ->
            println()
            println("  Selector: '$selector'")
            println("  Files: ${fileMap.keys.size}")
            
            fileMap.toSortedMap().forEach { (filePath, locations) ->
                val fileName = File(filePath).name
                locations.sortedBy { it.first }.forEach { (line, context) ->
                    println("     📍 $fileName:$line")
                    context?.let {
                        println("     " + it.replace("\n", "\n     "))
                    }
                }
            }
        }
    }
    
    println()
    if (!dupFound) {
        println("  ✅ No file-spanning naming conflicts found.")
    } else {
        println("  ✅ Scan complete.")
    }
    println()
}

fun printJsonOutput(
    jsDuplicates: Map<String, Map<String, List<Pair<Int, String?>>>>,
    cssDuplicates: Map<String, Map<String, List<Pair<Int, String?>>>>,
    rootPath: String
) {
    val json = buildString {
        appendLine("{")
        appendLine("  \"rootPath\": \"$rootPath\",")
        appendLine("  \"summary\": {")
        appendLine("    \"duplicateJsFunctions\": ${jsDuplicates.size},")
        appendLine("    \"duplicateCssSelectors\": ${cssDuplicates.size},")
        appendLine("    \"totalLocations\": ${jsDuplicates.values.sumOf { it.values.sumOf { l -> l.size } } + cssDuplicates.values.sumOf { it.values.sumOf { l -> l.size } }}")
        appendLine("  },")
        
        appendLine("  \"javascript\": {")
        jsDuplicates.entries.forEachIndexed { idx, (name, fileMap) ->
            appendLine("    \"$name\": {")
            appendLine("      \"count\": ${fileMap.keys.size},")
            appendLine("      \"locations\": {")
            fileMap.entries.forEachIndexed { fIdx, (file, locations) ->
                val comma = if (fIdx < fileMap.entries.size - 1) "," else ""
                appendLine("        \"$file\": [")
                locations.sortedBy { it.first }.forEachIndexed { lIdx, (line, _) ->
                    val lComma = if (lIdx < locations.size - 1) "," else ""
                    appendLine("          {\"line\": $line}$lComma")
                }
                appendLine("        ]$comma")
            }
            appendLine("      }")
            append("    }")
            if (idx < jsDuplicates.size - 1) appendLine(",") else appendLine()
        }
        appendLine("  },")
        
        appendLine("  \"css\": {")
        cssDuplicates.entries.forEachIndexed { idx, (selector, fileMap) ->
            val escapedSelector = selector.replace("\"", "\\\"")
            appendLine("    \"$escapedSelector\": {")
            appendLine("      \"count\": ${fileMap.keys.size},")
            appendLine("      \"locations\": {")
            fileMap.entries.forEachIndexed { fIdx, (file, locations) ->
                val comma = if (fIdx < fileMap.entries.size - 1) "," else ""
                appendLine("        \"$file\": [")
                locations.sortedBy { it.first }.forEachIndexed { lIdx, (line, _) ->
                    val lComma = if (lIdx < locations.size - 1) "," else ""
                    appendLine("          {\"line\": $line}$lComma")
                }
                appendLine("        ]$comma")
            }
            append("      }")
            append("    }")
            if (idx < cssDuplicates.size - 1) appendLine(",") else appendLine()
        }
        appendLine("  }")
        appendLine("}")
    }
    
    println(json)
}