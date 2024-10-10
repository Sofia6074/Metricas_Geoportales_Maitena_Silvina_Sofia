"use client";

import { useContext, useEffect, useRef, useState } from "react";
import styles from "./search.module.css"
import Breadcrumb from "@/components/breadcrumb/breadcrumb";
import Card from "@/components/card/card";
import { ResponsiveContainer, Bar, BarChart, Tooltip, XAxis, YAxis } from 'recharts';
import { MetricsContext } from "@/context/MetricsContext";
import Spinner from "@/components/spinner/spinner";
import { Wordcloud } from "@visx/wordcloud";
import { scaleLinear } from "d3-scale";

export default function Search() {
    const [primaryChartColor, setPrimaryChartColor] = useState<string>('');

    const { metrics, loading, error } = useContext(MetricsContext);
    const colorScale = useRef<ReturnType<typeof scaleLinear<string>> | null>(null);

    useEffect(() => {
        const rootStyle = getComputedStyle(document.documentElement);
        const purpleColor = rootStyle.getPropertyValue('--color-chart-purple').trim();
        setPrimaryChartColor(purpleColor);
    }, [])

    useEffect(() => {
        if (metrics) {
            const words = metrics.most_repeated_words;

            colorScale.current = scaleLinear<string>()
                .domain([Math.min(...words.map((d) => d.value)), Math.max(...words.map((d) => d.value))])
                .range(['#8884d8', '#58508d']);
        }
    }, [metrics]);

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
                        <Card title="Related ratio of consecutive search parameters" infoIcon tooltipText="Ratio of related search parameters in consecutive manner." tooltipDirection="left" className={`${styles.item1}`}>
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
                                        />
                                        <Tooltip />
                                        <Bar dataKey="jaccard_similarity" fill={primaryChartColor} />
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>

                        <Card title="Repeated words in consecutive searches" infoIcon tooltipText="Most repeated words in consecutive searches." tooltipDirection="left" className={`${styles.item2}`}>
                            <div className={styles.chart}>
                                <Wordcloud
                                    words={metrics.most_repeated_words}
                                    width={500}
                                    height={300}
                                    fontSize={(word) => word.value * 10}
                                    spiral="rectangular"
                                    rotate={() => 0}
                                    padding={2}
                                >
                                    {(cloudWords) =>
                                        // eslint-disable-next-line @typescript-eslint/no-explicit-any
                                        cloudWords.map((w: any, i) => (
                                            <text
                                                key={i}
                                                fontSize={w.size}
                                                textAnchor="middle"
                                                transform={`translate(${w.x}, ${w.y})`}
                                                fill={colorScale.current!(w.value)}
                                            >
                                                {w.text}
                                            </text>
                                        ))
                                    }
                                </Wordcloud>
                            </div>
                        </Card>
                    </div>}
        </div>
    )
}
