import type { Metadata } from "next";
import localFont from "next/font/local";
import Sidebar from "@/components/sidebar/sidebar";
import "./globals.css";

const geistSans = localFont({
	src: "./fonts/GeistVF.woff",
	variable: "--font-geist-sans",
	weight: "100 900",
});
const geistMono = localFont({
	src: "./fonts/GeistMonoVF.woff",
	variable: "--font-geist-mono",
	weight: "100 900",
});

export const metadata: Metadata = {
	title: "Geometrics",
};

export default function RootLayout({
	children,
}: Readonly<{
	children: React.ReactNode;
}>) {
	return (
		<html lang="es">
			<body className={`${geistSans.variable} ${geistMono.variable}`}>
				<div className="layout">
					<Sidebar />
					<main className="content">{children}</main>
				</div>
			</body>
		</html>
	);
}
