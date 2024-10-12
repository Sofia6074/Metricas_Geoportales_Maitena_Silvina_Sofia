"use client";

import { useContext, useEffect, useRef, useState } from "react";
import styles from "./navigability.module.css";
import Breadcrumb from "@/components/breadcrumb/breadcrumb";
import Card from "@/components/card/card";
import { MetricsContext } from "@/context/MetricsContext";
import Spinner from "@/components/spinner/spinner";
import dynamic from "next/dynamic";

const ForceGraph2D = dynamic(() => import("react-force-graph").then(mod => mod.ForceGraph2D), { ssr: false });

export default function Navigability() {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const fgRef = useRef<any>(null);
    const { metrics, loading, error } = useContext(MetricsContext);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const [data, setData] = useState<any>({});

    useEffect(() => {
        if (metrics) {
            setData(transformData(metrics.top_pages_node_map));
        }
    }, [metrics]);

    const transformData = (data: [string, [string, number][]][]) => {
        const nodes: { id: string; name: string }[] = [];
        const links: { source: string; target: string }[] = [];

        data.forEach((item, index) => {
            const sourceId = `Home ${index + 1}`;
            const sourceName = item[0];

            nodes.push({ id: sourceId, name: sourceName });

            item[1].forEach((targetArray, targetIndex) => {
                const targetId = `${sourceId} - Page ${String.fromCharCode(65 + targetIndex)}`;
                const targetName = targetArray[0];

                nodes.push({ id: targetId, name: targetName });
                links.push({ source: sourceId, target: targetId });
            });
        });

        return { nodes, links };
    };

    useEffect(() => {
        if (fgRef.current) {
            const simulation = fgRef.current.d3Force();

            if (simulation) {
                simulation.force("link").strength(1);
                simulation.force("charge").strength(-20);
                simulation.force("link").distance(30);

                simulation.alpha(1).restart();
                fgRef.current.zoomToFit(300, 50);
            }
        }
    }, [data]);

    return (
        <div className={styles.flex}>
            <Breadcrumb text={"Navigability"} />
            {loading ? <Spinner /> :
                error ? <p>Error: {error}</p> : metrics &&
                    <div className={styles.grid}>
                        <Card title="Most visited pages" infoIcon tooltipText="Top 10 pages that are most visited." tooltipDirection="top" className={styles.item1}>
                            <div className={styles.chart}>
                                <table className={styles.table}>
                                    <thead>
                                        <tr>
                                            <th>Base url</th>
                                            <th>Visits</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {metrics.most_visited_pages.map((page, index) => (
                                            <tr key={index}>
                                                <td>{page.base_url}</td>
                                                <td>{page.count}</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </Card>

                        <Card title="Navigability between home and top pages" infoIcon tooltipText="Nodes map that recreates the navigability between the home pages and the top 5 visited pages." tooltipDirection="left" className={styles.item2}>
                            <div className={styles.chart}>
                                {data && data.nodes?.length > 0 && data.links?.length > 0 && (
                                    <ForceGraph2D
                                        ref={fgRef}
                                        graphData={data}
                                        nodeAutoColorBy="id"
                                        linkDirectionalArrowLength={4}
                                        linkWidth={2}
                                    />
                                )}
                            </div>
                        </Card>
                    </div>}
        </div>
    );
}
