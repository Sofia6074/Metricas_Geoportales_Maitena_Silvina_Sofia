"use client";

import { useContext, useEffect, useRef, useState } from "react";
import styles from "./navigability.module.css";
import Breadcrumb from "@/components/breadcrumb/breadcrumb";
import Card from "@/components/card/card";
import { MetricsContext } from "@/context/MetricsContext";
import Spinner from "@/components/spinner/spinner";
import { ForceGraph2D } from "react-force-graph";

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
                        <Card title="Navigability between home and top pages" infoIcon className={styles.item1}>
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
