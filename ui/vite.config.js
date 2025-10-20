import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import svgr from "vite-plugin-svgr";
import { compression } from "vite-plugin-compression2";

export default defineConfig(() => {
  return {
    base: "",
    plugins: [
      react(),
      svgr({
        svgrOptions: {
          ref: true,
        },
      }),
      // build pre-compressed gzip assets
      compression(),
      // build pre-compressed brotli assets
      compression({ algorithm: "brotliCompress", exclude: [/\.(br)$/, /\.(gz)$/] }),
    ],
    server: {
      proxy: {
        // proxy API calls to the backend
        "^/(jobs|post).*": {
          target: `http://127.0.0.1:${process.env.WEBAPP_PORT ?? 5000}`,
        },
      },
    },
  };
});
