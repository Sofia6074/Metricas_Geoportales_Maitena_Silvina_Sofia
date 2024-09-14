import Image from "next/image";
import styles from "./page.module.css";
import Sidebar from "@/components/sidebar/sidebar";

export default function Home() {
  return (
    <div>
      <Sidebar />
      <h1>Hi</h1>
    </div>
  );
}
