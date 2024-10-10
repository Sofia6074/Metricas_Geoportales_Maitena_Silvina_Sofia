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

    const getProfileNameByProfileType = (profile: number) => {
        let profileName = "";

        switch (profile) {
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

        return profileName;
    }

    const userProfilesWithNames = useMemo(() => {
        if (!metrics?.user_profiles) return [];

        return metrics.user_profiles.user_profile_counts
            .map((profile) => ({
                ...profile,
                name: getProfileNameByProfileType(profile.user_profile),
            })).sort((a, b) => a.user_profile - b.user_profile);
    }, [metrics]);

    const averagePagesViewedWithNames = useMemo(() => {
        if (!metrics?.user_profiles) return [];

        return metrics.user_profiles.user_categorized_metrics.average_pages_viewed_per_session_per_user_category
            .map((profile) => ({
                ...profile,
                name: getProfileNameByProfileType(profile.user_profile),
            }))
            .sort((a, b) => a.user_profile - b.user_profile);
    }, [metrics]);

    const averagePageTimeWithNames = useMemo(() => {
        if (!metrics?.user_profiles) return [];

        return metrics.user_profiles.user_categorized_metrics.average_time_spent_per_page_per_user_category
            .map((profile) => ({
                ...profile,
                name: getProfileNameByProfileType(profile.user_profile),
            })).sort((a, b) => a.user_profile - b.user_profile);;
    }, [metrics]);

    const averageSiteTimeWithNames = useMemo(() => {
        if (!metrics?.user_profiles) return [];

        return metrics.user_profiles.user_categorized_metrics.average_time_spent_on_site_per_user_category
            .map((profile) => ({
                ...profile,
                name: getProfileNameByProfileType(profile.user_profile),
            })).sort((a, b) => a.user_profile - b.user_profile);;
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
                                    {metrics.average_pages_viewed.toFixed(0)} pages
                                </AutoSizeText>
                            </div>
                        </Card>

                        <Card title="Ratio of new visitors to all visitors" infoIcon className={`${styles.item2}`}>
                            <div className={styles.chart}>
                                <AutoSizeText maxSize="40px" colorVar="color-text" textAlign="center">
                                    {metrics.new_visitors_vs_all_visitors.toFixed(2)}%
                                </AutoSizeText>
                            </div>
                        </Card>

                        <Card title="Average stepbacks actions" infoIcon className={`${styles.item3}`}>
                            <AutoSizeText maxSize="40px" colorVar="color-text" textAlign="center">
                                {metrics.average_stepbacks.toFixed(0)}
                            </AutoSizeText>
                        </Card>

                        <Card title="User categories" infoIcon className={`${styles.item4}`}>
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

                        <Card title="Average pages viewed" infoIcon className={`${styles.item5}`}>
                            <div className={styles.chart}>
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart width={60} height={150} data={averagePagesViewedWithNames}>
                                        <XAxis dataKey="name" />
                                        <Tooltip />
                                        <Bar dataKey="avg_pages_viewed" fill={primaryChartColor} />
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>

                        <Card title="Average time spent per page" infoIcon className={`${styles.item6}`}>
                            <div className={styles.chart}>
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart width={60} height={150} data={averagePageTimeWithNames}>
                                        <XAxis dataKey="name" />
                                        <Tooltip />
                                        <Bar dataKey="avg_time_per_user" fill={primaryChartColor} />
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>

                        <Card title="Average time spent on site" infoIcon className={`${styles.item7}`}>
                            <div className={styles.chart}>
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart width={60} height={150} data={averageSiteTimeWithNames}>
                                        <XAxis dataKey="name" />
                                        <Tooltip />
                                        <Bar dataKey="avg_time_spent_on_site" fill={primaryChartColor} />
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>
                    </div>}
        </div>
    )
}
