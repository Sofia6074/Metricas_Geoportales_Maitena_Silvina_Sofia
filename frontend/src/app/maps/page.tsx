"use client";

import { useContext, useMemo } from "react";
import styles from "./maps.module.css"
import Breadcrumb from "@/components/breadcrumb/breadcrumb";
import Card from "@/components/card/card";
import { LineChart, Line, ResponsiveContainer, Tooltip, XAxis, TooltipProps } from 'recharts';
import { MetricsContext } from "@/context/MetricsContext";
import Spinner from "@/components/spinner/spinner";
import AutoSizeText from "@/components/autosizeText/autosizeText";
import { ValueType, NameType } from "recharts/types/component/DefaultTooltipContent";

export default function Maps() {
    const { metrics, loading, error } = useContext(MetricsContext);

    const sortedZoomData = useMemo(() => {
        if (!metrics?.maximum_stable_zoom) return [];

        return [...metrics.maximum_stable_zoom].sort((a, b) => a.zoom_level - b.zoom_level);
    }, [metrics]);

    const CustomTooltip = ({ active, payload, label }: TooltipProps<ValueType, NameType>) => {
        if (active && payload && payload.length) {
            return (
                <div className={styles.customTooltip}>
                    <p><b>Zoom level: </b> {label}</p>
                    <p><b>Total count: </b> {payload[0].value}</p>
                </div>
            );
        }

        return null;
    };

    return (
        <div className={styles.flex}>
            <Breadcrumb text={"Maps"} />
            {loading ? <Spinner /> :
                error ? <p>Error: {error}</p> : metrics &&
                    <div className={styles.grid}>
                        {/* Primera Fila */}
                        <Card title="Average zoom response time" infoIcon className={`${styles.item1}`}>
                            <div className={styles.display}>
                                <AutoSizeText maxSize="40px" colorVar="color-text" textAlign="center">
                                    {metrics.average_zoom_response_time.toFixed(2)}s
                                </AutoSizeText>
                            </div>
                        </Card>

                        <Card title="Stable zooms" infoIcon className={`${styles.item2}`}>
                            <div className={styles.chart}>
                                <ResponsiveContainer width="100%" height="100%">
                                    <LineChart width={300} height={100} data={sortedZoomData}>
                                        <XAxis dataKey="zoom_level" />
                                        <Tooltip content={<CustomTooltip />} />
                                        <Line type="monotone" dataKey="total_count" stroke="#8884d8" strokeWidth={2} />
                                    </LineChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>

                        <Card title="Maximum zoom reached" infoIcon className={`${styles.item3}`}>
                            <div className={styles.display}>
                                <AutoSizeText maxSize="40px" colorVar="color-text" textAlign="center">
                                    {metrics.maximum_zoom_reached.toFixed(0)}
                                </AutoSizeText>
                            </div>
                        </Card>
                    </div>}
        </div>
    )
}
