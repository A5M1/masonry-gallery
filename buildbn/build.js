import esbuild from "esbuild";
import fs from "fs";

const outfile = "dist/libs.bundle.js";
const mapUrl = "//# sourceMappingURL=/bundled/libs.bundle.js.map";

try {
  await esbuild.build({
    entryPoints: ["src/libs.js"],
    bundle: true,
    outfile,
    format: "iife",
    globalName: "Libs",
    platform: "browser",
    minify: false,
    sourcemap: true
  });

  let code = fs.readFileSync(outfile, "utf8");
  code = code.replace(/^\/\/# sourceMappingURL=.*\r?\n?/gm, "");
  if (!code.endsWith("\n")) code += "\n";
  code += mapUrl + "\n";
  fs.writeFileSync(outfile, code);
} catch {
  process.exit(1);
}