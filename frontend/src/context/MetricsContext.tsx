"use client";

import React, { createContext, useState, useEffect, ReactNode } from "react";
import { JsonData } from "@/types/json.type";

interface MetricsContextType {
    metrics: JsonData | null;
    loading: boolean;
    error: string | null;
}

export const MetricsContext = createContext<MetricsContextType>({
    metrics: null,
    loading: true,
    error: null,
});

export const MetricsProvider = ({ children }: { children: ReactNode }) => {
    const [metrics, setMetrics] = useState<JsonData | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        fetch('/metrics_results.json')
            .then((response) => {
                if (!response.ok) {
                    throw new Error('Error fetching metrics');
                }
                return response.json();
            })
            .then((data: JsonData) => {
                setMetrics(data);
                setLoading(false);
            })
            .catch((err) => {
                setError(err.message);
                setLoading(false);
            });
    }, []);

    return (
        <MetricsContext.Provider value={{ metrics, loading, error }}>
            {children}
        </MetricsContext.Provider>
    );
};
