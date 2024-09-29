"use client";

import { useContext } from "react";
import styles from "./search.module.css"
import Breadcrumb from "@/components/breadcrumb/breadcrumb";
import Card from "@/components/card/card";
import { ResponsiveContainer, Bar, BarChart, Tooltip, XAxis, YAxis, YAxisProps } from 'recharts';
import { MetricsContext } from "@/context/MetricsContext";
import Spinner from "@/components/spinner/spinner";

export default function Search() {
    const { metrics, loading, error } = useContext(MetricsContext);

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const CustomYAxisTick = ({ x, y, payload }: any) => {
        const text = payload.value;
        const maxLength = 20;

        return (
            <g transform={`translate(${x},${y})`}>
                <title>{text}</title>
                <text
                    x={0}
                    y={0}
                    dy={4}
                    textAnchor="end"
                    fill="#666"
                    style={{
                        fontSize: '16px',
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                        maxWidth: `${maxLength}ch`,
                    }}
                >
                    {text.length > maxLength ? `${text.substring(0, maxLength)}...` : text}
                </text>
            </g>
        );
    };


    return (
        <div className={styles.flex}>
            <Breadcrumb text={"Search"} />
            {loading ? <Spinner /> :
                error ? <p>Error: {error}</p> : metrics &&
                    <div className={styles.grid}>
                        <Card title="Related ratio of consecutive search parameters" infoIcon className={`${styles.item1}`}>
                            <div className={styles.chart}>
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart
                                        layout="vertical"
                                        width={500}
                                        height={300}
                                        data={metrics.related_search_parameters}
                                        margin={{ top: 20, right: 20, left: 100, bottom: 5 }}
                                    >
                                        <XAxis type="number" dataKey="jaccard_similarity" />
                                        <YAxis
                                            type="category"
                                            dataKey="search_pair"
                                            width={80}
                                            tick={<CustomYAxisTick />}
                                        />                                        <Tooltip />
                                        <Bar dataKey="jaccard_similarity" fill="#8884d8" />
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>

                        <Card title="Repeated words in consecutive searches" infoIcon className={`${styles.item2}`}>
                            <div className={styles.chart}>
                                351.15s
                            </div>
                        </Card>
                    </div>}
        </div>
    )
}
