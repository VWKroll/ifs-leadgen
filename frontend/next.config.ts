import type { NextConfig } from "next";

const apiHost = process.env.IDC_API_HOST ?? "127.0.0.1";
const apiPort = process.env.IDC_API_PORT ?? "8001";

const nextConfig: NextConfig = {
  reactStrictMode: true,
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: `http://${apiHost}:${apiPort}/api/:path*`,
      },
    ];
  },
};

export default nextConfig;
