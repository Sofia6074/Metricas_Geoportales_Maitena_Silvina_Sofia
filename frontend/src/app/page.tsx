"use client";
import { useState, useEffect } from "react";
import Overview from "./overview/page";
import { JsonData } from "@/types/json.type";

export default function Home() {
  const [metrics, setMetrics] = useState({});

  useEffect(() => {
    fetch('/metrics_results.json')
      .then((response) => {
        debugger
        if (!response.ok) {
          throw new Error('Error fetching metrics');
        }
        return response.json();
      })
      .then((data: JsonData) => {
        setMetrics(data);
      })
      .catch((error) => {
        console.error('Error fetching metrics:', error);
      });
  }, []);

  return (
    <Overview />
  );
}
