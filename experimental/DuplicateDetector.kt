import java.io.File
import java.io.IOException
import kotlin.system.measureTimeMillis

object DuplicateDetector {

    private val jsExtensions = setOf("js", "mjs", "jsx", "ts", "tsx", "mts", "cts")
    private val cssExtensions = setOf("css", "scss", "sass", "less")

    private val jsKeywords = setOf(
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

    private val cssBlacklist = setOf(
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

    private val defaultExcludes = setOf(
        "node_modules", ".git", ".svn", ".hg", "dist", "build", "target",
        ".cache", ".tmp", ".temp", "vendor", "bower_components", ".idea",
        ".vscode", ".vs", "coverage", ".nyc_output", ".parcel-cache",
        "__pycache__", ".next", ".nuxt", ".svelte-kit"
    )

    private val jsFuncPatterns by lazy { listOf(
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
    )}

    private val cssRulePattern = Regex("""([^{}+>~[\],]+)\s*\{""")
    private val singleLineCommentPattern = Regex("//[^\n\r]*")
    private val multiLineCommentPattern = Regex("/\\*[^*]*\\*+(?:[^/*][^*]*\\*+)*/")
    private val templateLiteralPattern = Regex("""`(?:[^`\\]|\\.)*`""")
    private val singleQuotePattern = Regex("""'(?:[^'\\]|\\.)*'""")
    private val doubleQuotePattern = Regex(""""(?:[^"\\]|\\.)*"""")

    private const val MIN_NAME_LENGTH = 2
    private const val DEFAULT_MIN_OCCURRENCES = 2
    private const val CONTEXT_LINES = 2
    private const val REPORT_WIDTH = 70
    private const val NAME = "DuplicateDetector"
    private const val VERSION = "2.0"

    data class Config(
        val rootPath: String = ".",
        val verbose: Boolean = false,
        val outputJson: Boolean = false,
        val showContext: Boolean = false,
        val minOccurrences: Int = DEFAULT_MIN_OCCURRENCES,
        val excludePatterns: List<String> = emptyList()
    ) {
        init {
            require(minOccurrences >= 2) { "minOccurrences must be at least 2" }
        }
    }

    data class Location(
        val file: File,
        val line: Int,
        val context: String? = null
    )

    data class DuplicateResult(
        val name: String,
        val locationsByFile: Map<String, List<Location>>
    ) {
        val fileCount: Int get() = locationsByFile.size
        val totalLocations: Int get() = locationsByFile.values.sumOf { it.size }
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val config = parseArgs(args)

        if (config == null) {
            printHelp()
            return
        }

        val root = File(config.rootPath).absoluteFile
        if (!root.isDirectory) {
            System.err.println("Error: '${config.rootPath}' is not a directory.")
            return
        }

        runDetection(config.copy(rootPath = root.path), root)
    }

    private fun runDetection(config: Config, root: File) {
        val allExcludes = defaultExcludes + config.excludePatterns.toSet()

        if (config.verbose) {
            println("$NAME v$VERSION")
            println("   Root: ${config.rootPath}")
            println("   Min occurrences: ${config.minOccurrences}")
            println("   Excludes: $allExcludes")
            println()
        }

        val scanTime = measureTimeMillis {
            val (jsFiles, cssFiles) = collectFiles(root, jsExtensions, cssExtensions, allExcludes)

            if (config.verbose) {
                println("Found ${jsFiles.size} JS/TS files and ${cssFiles.size} CSS files")
                println()
            }

            val jsResults = scanJsFiles(jsFiles, config.verbose, config.showContext)
            val cssResults = scanCssFiles(cssFiles, config.verbose, config.showContext)

            printResults(jsResults, cssResults, config)
        }

        if (config.verbose) {
            println("\nScan completed in ${scanTime}ms")
        }
    }

    private fun parseArgs(args: Array<String>): Config? {
        if (args.contains("--help")) return null

        val excludes = mutableListOf<String>()
        var min: Int? = null

        for (arg in args) {
            when {
                arg == "--verbose" -> {}
                arg == "--json" -> {}
                arg == "--context" -> {}
                arg.startsWith("--min=") -> min = arg.substring(6).toIntOrNull()
                arg.startsWith("--exclude=") -> excludes.add(arg.substring(10))
            }
        }

        val rootPath = args.firstOrNull { !it.startsWith("--") } ?: "."
        val parsedMin = min ?: DEFAULT_MIN_OCCURRENCES

        if (parsedMin < 2) {
            System.err.println("Error: --min must be at least 2")
            return null
        }

        return Config(
            rootPath = rootPath,
            verbose = "--verbose" in args,
            outputJson = "--json" in args,
            showContext = "--context" in args,
            minOccurrences = parsedMin,
            excludePatterns = excludes.toList()
        )
    }

    private fun printHelp() {
        println("""
            |$NAME v$VERSION
            |
            |Scans JavaScript/TypeScript and CSS/SCSS files for duplicate function names
            |and CSS selectors across a project tree.
            |
            |USAGE
            |  kotlin DuplicateDetector.kt [options] [path]
            |
            |ARGUMENTS
            |  path    Directory to scan (default: current working directory)
            |
            |OPTIONS
            |  --verbose
            |          Show detailed scanning progress including file count and
            |          scan duration. Useful for monitoring large codebases.
            |
            |  --json
            |          Output results in machine-readable JSON format.
            |          Useful for integration with other tools or scripts.
            |
            |  --min=N
            |          Only report items appearing in N or more distinct files.
            |          Default is 2. Use higher values to filter out less
            |          common duplicates.
            |
            |  --context
            |          Show code snippet context (2 lines before and after)
            |          for each duplicate location found.
            |
            |  --exclude=PAT
            |          Exclude paths containing PAT. Can be specified multiple
            |          times. Supports directory names like 'node_modules'.
            |          Default exclusions include node_modules, .git, dist, etc.
            |
            |  --help
            |          Display this help message and exit.
            |
            |SUPPORTED FILE TYPES
            |  JavaScript/TypeScript:  .js, .jsx, .ts, .tsx, .mjs, .mts, .cts
            |  CSS:                   .css, .scss, .sass, .less
            |
            |EXIT CODES
            |  0   Scan completed successfully
            |  1   Error (e.g., invalid path or arguments)
            |
            |EXAMPLES
            |  # Scan current directory
            |  kotlin DuplicateDetector.kt .
            |
            |  # Scan with progress output
            |  kotlin DuplicateDetector.kt --verbose ./src
            |
            |  # JSON output for automation
            |  kotlin DuplicateDetector.kt --json --verbose > results.json
            |
            |  # Only report duplicates in 3+ files
            |  kotlin DuplicateDetector.kt --min=3 .
            |
            |  # Show code context for each duplicate
            |  kotlin DuplicateDetector.kt --context .
            |
            |  # Exclude specific directories
            |  kotlin DuplicateDetector.kt --exclude=node_modules --exclude=dist .
            |
            |  # Combined usage
            |  kotlin DuplicateDetector.kt --verbose --json --min=3 --context ./src
            |
            |DETECTED PATTERNS
            |  JavaScript/TypeScript:
            |    - Function declarations (function foo())
            |    - Arrow functions (const foo = () => {})
            |    - Class methods (foo(): void {})
            |    - Object methods (foo(): void {})
            |    - Export declarations (export const foo = ...)
            |    - Accessor methods (get foo(), set bar())
            |    - Static methods (static foo())
            |    - TypeScript-specific (abstract, private, protected)
            |
            |  CSS:
            |    - Element selectors (div, span, p)
            |    - Class selectors (.button, .container)
            |    - ID selectors (#header)
            |    - Attribute selectors [type="text"]
            |    - Combinators (div > p, .parent .child)
            |
        """.trimMargin())
    }

    private fun collectFiles(
        root: File,
        jsExtensions: Set<String>,
        cssExtensions: Set<String>,
        excludes: Set<String>
    ): Pair<List<File>, List<File>> {
        val jsFiles = mutableListOf<File>()
        val cssFiles = mutableListOf<File>()
        val separator = File.separator

        root.walkTopDown().forEach { file ->
            if (file.isFile) {
                val path = file.absolutePath
                val shouldExclude = excludes.any { ex ->
                    path.contains("$separator$ex$separator") ||
                    path.endsWith("$separator$ex")
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

    private fun stripJsCommentsAndStrings(code: String): String {
        var result = code
        result = singleLineCommentPattern.replace(result) { " ".repeat(it.value.length) }
        result = multiLineCommentPattern.replace(result) { " ".repeat(it.value.length) }
        result = templateLiteralPattern.replace(result) { " ".repeat(it.value.length) }
        result = singleQuotePattern.replace(result) { " ".repeat(it.value.length) }
        result = doubleQuotePattern.replace(result) { " ".repeat(it.value.length) }
        return result
    }

    private fun getContext(code: String, lineNum: Int, contextLines: Int = CONTEXT_LINES): String {
        val lines = code.lines()
        val start = (lineNum - contextLines - 1).coerceAtLeast(0)
        val end = (lineNum + contextLines).coerceAtMost(lines.size)

        return lines.subList(start, end).mapIndexed { idx, line ->
            val currentLine = start + idx + 1
            val marker = if (currentLine == lineNum) ">>>" else "   "
            "$marker $line"
        }.joinToString("\n")
    }

    private fun countNewLinesBeforeIndex(text: String, index: Int): Int {
        val endIndex = index.coerceAtMost(text.length)
        var count = 0
        for (i in 0 until endIndex) {
            if (text[i] == '\n') count++
        }
        return count
    }

    private fun isPascalCase(name: String): Boolean =
        name.first().isUpperCase() && name.any { it.isLowerCase() }

    private fun isValidIdentifier(name: String): Boolean =
        name.length >= MIN_NAME_LENGTH &&
        name.first().isLetter() &&
        name.all { it.isLetterOrDigit() || it == '_' }

    private fun scanJsFiles(
        files: List<File>,
        verbose: Boolean,
        showContext: Boolean
    ): Map<String, Map<String, List<Location>>> {
        val duplicates = mutableMapOf<String, MutableMap<String, MutableList<Location>>>()

        files.forEachIndexed { idx, file ->
            if (verbose) {
                print("\r   Scanning JS/TS: ${idx + 1}/${files.size} - ${file.name}")
            }

            try {
                val raw = file.readText()
                val cleaned = stripJsCommentsAndStrings(raw)
                val foundOffsets = mutableSetOf<Int>()

                jsFuncPatterns.forEach { pattern ->
                    pattern.findAll(cleaned).forEach findAll@{ match ->
                        val name = match.groupValues[1]
                        val offset = match.range.first

                        if (offset in foundOffsets) return@findAll
                        if (name in jsKeywords) return@findAll
                        if (isPascalCase(name)) return@findAll
                        if (!isValidIdentifier(name)) return@findAll

                        foundOffsets.add(offset)
                        val lineNum = countNewLinesBeforeIndex(raw, offset) + 1
                        val context = if (showContext) getContext(raw, lineNum) else null

                        duplicates
                            .getOrPut(name) { mutableMapOf() }
                            .getOrPut(file.absolutePath) { mutableListOf() }
                            .add(Location(file, lineNum, context))
                    }
                }
            } catch (e: IOException) {
                if (verbose) println("\n   Warning: Could not read ${file.name}: ${e.message}")
            } catch (e: SecurityException) {
                if (verbose) println("\n   Warning: No permission to read ${file.name}")
            }
        }

        if (verbose) println()
        return duplicates
    }

    private val pseudoSelectorPattern = Regex("""^:[^ (]+$""")
    private val attributeSelectorPattern = Regex("""^\[.+\]$""")

    private fun scanCssFiles(
        files: List<File>,
        verbose: Boolean,
        showContext: Boolean
    ): Map<String, Map<String, List<Location>>> {
        val duplicates = mutableMapOf<String, MutableMap<String, MutableList<Location>>>()

        files.forEachIndexed { idx, file ->
            if (verbose) {
                print("\r   Scanning CSS: ${idx + 1}/${files.size} - ${file.name}")
            }

            try {
                val text = file.readText()
                val noComments = multiLineCommentPattern.replace(text) { " ".repeat(it.value.length) }

                cssRulePattern.findAll(noComments).forEach { match ->
                    val rawSelector = match.groupValues[1]
                    val offset = match.range.first
                    val lineNum = countNewLinesBeforeIndex(text, offset) + 1

                    val individualSelectors = rawSelector.split(",").map { selector ->
                        selector.trim()
                            .replace(Regex("""\s+"""), " ")
                            .replace(Regex("""\s*\+\s*"""), "+")
                            .replace(Regex("""\s*~\s*"""), "~")
                            .replace(Regex("""\s*>\s*"""), ">")
                    }

                    individualSelectors.forEach selectorLoop@{ selector ->
                        when {
                            selector.isBlank() -> return@selectorLoop
                            selector.startsWith("@") -> return@selectorLoop
                            selector == "*" -> return@selectorLoop
                            selector in cssBlacklist -> return@selectorLoop
                            pseudoSelectorPattern.matches(selector) -> return@selectorLoop
                            attributeSelectorPattern.matches(selector) -> return@selectorLoop
                        }

                        val context = if (showContext) getContext(text, lineNum) else null

                        duplicates
                            .getOrPut(selector) { mutableMapOf() }
                            .getOrPut(file.absolutePath) { mutableListOf() }
                            .add(Location(file, lineNum, context))
                    }
                }
            } catch (e: IOException) {
                if (verbose) println("\n   Warning: Could not read ${file.name}: ${e.message}")
            } catch (e: SecurityException) {
                if (verbose) println("\n   Warning: No permission to read ${file.name}")
            }
        }

        if (verbose) println()
        return duplicates
    }

    private fun printResults(
        jsResults: Map<String, Map<String, List<Location>>>,
        cssResults: Map<String, Map<String, List<Location>>>,
        config: Config
    ) {
        val jsDuplicates = jsResults.filter { it.value.size >= config.minOccurrences }
        val cssDuplicates = cssResults.filter { it.value.size >= config.minOccurrences }

        if (config.outputJson) {
            printJsonOutput(jsDuplicates, cssDuplicates, config.rootPath)
        } else {
            printTextOutput(jsDuplicates, cssDuplicates, config.rootPath)
        }
    }

    private fun printTextOutput(
        jsDuplicates: Map<String, Map<String, List<Location>>>,
        cssDuplicates: Map<String, Map<String, List<Location>>>,
        rootPath: String
    ) {
        val totalJsDupes = jsDuplicates.size
        val totalCssDupes = cssDuplicates.size
        val totalLocations = (jsDuplicates.values.sumOf { it.values.sumOf { l -> l.size } } +
                              cssDuplicates.values.sumOf { it.values.sumOf { l -> l.size } })

        val divider = "═".repeat(REPORT_WIDTH)

        println("\n$divider")
        println("  Duplicate Detection Report")
        println("  Root: $rootPath")
        println(divider)
        println()
        println("  Summary:")
        println("     * Duplicate JS/TS functions: $totalJsDupes")
        println("     * Duplicate CSS selectors:   $totalCssDupes")
        println("     * Total duplicate locations:  $totalLocations")
        println()

        val hasResults = jsDuplicates.isNotEmpty() || cssDuplicates.isNotEmpty()

        if (jsDuplicates.isNotEmpty()) {
            println("-".repeat(REPORT_WIDTH))
            println("  JavaScript/TypeScript Functions")
            println("-".repeat(REPORT_WIDTH))

            jsDuplicates.toSortedMap().forEach { (name, fileMap) ->
                println()
                println("  Function: '$name'")
                println("  Files: ${fileMap.size}")

                fileMap.toSortedMap().forEach { (filePath, locations) ->
                    val fileName = File(filePath).name
                    locations.sortedBy { it.line }.forEach { location ->
                        println("     @ $fileName:${location.line}")
                        location.context?.let {
                            println("     " + it.replace("\n", "\n     "))
                        }
                    }
                }
            }
        }

        if (cssDuplicates.isNotEmpty()) {
            println()
            println("-".repeat(REPORT_WIDTH))
            println("  CSS Selectors")
            println("-".repeat(REPORT_WIDTH))

            cssDuplicates.toSortedMap().forEach { (selector, fileMap) ->
                println()
                println("  Selector: '$selector'")
                println("  Files: ${fileMap.size}")

                fileMap.toSortedMap().forEach { (filePath, locations) ->
                    val fileName = File(filePath).name
                    locations.sortedBy { it.line }.forEach { location ->
                        println("     @ $fileName:${location.line}")
                        location.context?.let {
                            println("     " + it.replace("\n", "\n     "))
                        }
                    }
                }
            }
        }

        println()
        println("  [OK] ${if (hasResults) "Scan complete." else "No file-spanning naming conflicts found."}")
        println()
    }

    private fun printJsonOutput(
        jsDuplicates: Map<String, Map<String, List<Location>>>,
        cssDuplicates: Map<String, Map<String, List<Location>>>,
        rootPath: String
    ) {
        val totalLocations = jsDuplicates.values.sumOf { it.values.sumOf { l -> l.size } } +
                             cssDuplicates.values.sumOf { it.values.sumOf { l -> l.size } }

        val json = buildString {
            appendLine("{")
            appendLine("  \"rootPath\": \"${escapeJson(rootPath)}\",")
            appendLine("  \"summary\": {")
            appendLine("    \"duplicateJsFunctions\": ${jsDuplicates.size},")
            appendLine("    \"duplicateCssSelectors\": ${cssDuplicates.size},")
            appendLine("    \"totalLocations\": $totalLocations")
            appendLine("  },")

            appendLine("  \"javascript\": {")
            jsDuplicates.entries.forEachIndexed { idx, (name, fileMap) ->
                appendLine("    \"${escapeJson(name)}\": {")
                appendLine("      \"count\": ${fileMap.size},")
                appendLine("      \"locations\": {")
                fileMap.entries.forEachIndexed { fIdx, (file, locations) ->
                    val comma = if (fIdx < fileMap.entries.size - 1) "," else ""
                    appendLine("        \"${escapeJson(file)}\": [")
                    locations.sortedBy { it.line }.forEachIndexed { lIdx, location ->
                        val lComma = if (lIdx < locations.size - 1) "," else ""
                        appendLine("          {\"line\": ${location.line}}$lComma")
                    }
                    appendLine("        ]$comma")
                }
                append("      }")
                append(if (idx < jsDuplicates.size - 1) "}," else "}")
                appendLine()
            }
            appendLine("  },")

            appendLine("  \"css\": {")
            cssDuplicates.entries.forEachIndexed { idx, (selector, fileMap) ->
                appendLine("    \"${escapeJson(selector)}\": {")
                appendLine("      \"count\": ${fileMap.size},")
                appendLine("      \"locations\": {")
                fileMap.entries.forEachIndexed { fIdx, (file, locations) ->
                    val comma = if (fIdx < fileMap.entries.size - 1) "," else ""
                    appendLine("        \"${escapeJson(file)}\": [")
                    locations.sortedBy { it.line }.forEachIndexed { lIdx, location ->
                        val lComma = if (lIdx < locations.size - 1) "," else ""
                        appendLine("          {\"line\": ${location.line}}$lComma")
                    }
                    appendLine("        ]$comma")
                }
                append("      }")
                append(if (idx < cssDuplicates.size - 1) "}," else "}")
                appendLine()
            }
            appendLine("  }")
            appendLine("}")
        }

        println(json)
    }

    private fun escapeJson(text: String): String = text
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
}