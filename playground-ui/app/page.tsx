import type { Metadata } from "next";
import { Playground } from "./playground";

export const metadata: Metadata = {
  title: "Flipbench Control Room",
  description: "Live PostgreSQL, Debezium and Kafka hot-to-warm flip playground",
};

export default function Home() {
  return <Playground />;
}
