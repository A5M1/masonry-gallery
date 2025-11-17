import esbuild from "esbuild";
import fs from "fs";

esbuild.build({
  entryPoints: ["src/libs.js"],
  bundle: true,
  outfile: "dist/libs.bundle.js",
  format: "iife",
  globalName: "Libs",
  platform: "browser",
  minify: false,
  sourcemap: true,
  footer: { js: "//# sourceMappingURL=/bundled/libs.bundle.js.map" }
}).catch(() => process.exit(1));