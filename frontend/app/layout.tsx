import "./globals.css";

import type { Metadata } from "next";
import { IBM_Plex_Sans } from "next/font/google";
import { ReactNode } from "react";

const ibmPlexSans = IBM_Plex_Sans({
  subsets: ["latin"],
  weight: ["400", "500", "600", "700"],
  variable: "--font-ui",
});

export const metadata: Metadata = {
  title: "IDC Event Intelligence",
  description: "Next.js frontend for IDC opportunity graphs and maps backed by Databricks.",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body className={ibmPlexSans.variable}>{children}</body>
    </html>
  );
}
