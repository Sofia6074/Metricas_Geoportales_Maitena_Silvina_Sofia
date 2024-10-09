"use client";

import AutoSizeText from "@/components/autosizeText/autosizeText";
import styles from "./users.module.css";
import Breadcrumb from "@/components/breadcrumb/breadcrumb";
import Card from "@/components/card/card";
import Spinner from "@/components/spinner/spinner";
import { MetricsContext } from "@/context/MetricsContext";
import { useContext, useEffect, useMemo, useState } from "react";
import { ResponsiveContainer, BarChart, Bar, Tooltip, XAxis } from "recharts";

export default function Users() {
    const [primaryChartColor, setPrimaryChartColor] = useState<string>('');
    const { metrics, loading, error } = useContext(MetricsContext);

    useEffect(() => {
        const rootStyle = getComputedStyle(document.documentElement);
        const purpleColor = rootStyle.getPropertyValue('--color-chart-purple').trim();
        setPrimaryChartColor(purpleColor);
    }, [])

    const userProfilesWithNames = useMemo(() => {
        if (!metrics?.user_profiles) return [];

        return metrics.user_profiles.user_profile_counts.map((profile) => {
            let profileName = "";

            switch (profile.user_profile) {
                case 1:
                    profileName = "Low Profile";
                    break;
                case 2:
                    profileName = "Medium Profile";
                    break;
                case 3:
                    profileName = "High Profile";
                    break;
                default:
                    profileName = "Unknown Profile";
            }

            return {
                ...profile,
                name: profileName,
            };
        });
    }, [metrics]);

    return (
        <div className={styles.flex}>
            <Breadcrumb text={"Users"} />
            {loading ? <Spinner /> :
                error ? <p>Error: {error}</p> : metrics &&
                    <div className={styles.grid}>
                        {/* Primera Fila */}
                        <Card title="Average pages viewed per visitor" infoIcon className={`${styles.item1}`}>
                            <div className={styles.chart}>
                                <AutoSizeText maxSize="40px" colorVar="color-text" textAlign="center">
                                    {metrics.average_pages_viewed.toFixed(2)} pages
                                </AutoSizeText>
                            </div>
                        </Card>

                        <Card title="Retio of new visitors to all visitors" infoIcon className={`${styles.item2}`}>
                            <div className={styles.chart}>
                                <AutoSizeText maxSize="40px" colorVar="color-text" textAlign="center">
                                    {metrics.new_visitors_vs_all_visitors.toFixed(2)}%
                                </AutoSizeText>
                            </div>
                        </Card>

                        <Card title="User categories" infoIcon className={`${styles.item3}`}>
                            <div className={styles.chart}>
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart width={60} height={150} data={userProfilesWithNames}>
                                        <XAxis dataKey="name" />
                                        <Tooltip />
                                        <Bar dataKey="count" fill={primaryChartColor} />
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>

                        {/* Segunda Fila */}
                        <Card title="Average stepbacks actions" infoIcon className={`${styles.item4}`}>
                            <AutoSizeText maxSize="40px" colorVar="color-text" textAlign="center">
                                {metrics.average_stepbacks.toFixed(0)}
                            </AutoSizeText>
                        </Card>
                    </div>}
        </div>
    )
}
