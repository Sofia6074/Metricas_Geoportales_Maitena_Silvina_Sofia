"use client";

import { useContext, useEffect } from "react";
import Breadcrumb from "@/components/breadcrumb/breadcrumb";
import styles from "./overview.module.css"
import Card from "@/components/card/card";
import { LineChart, Line, ResponsiveContainer, Bar, BarChart, PieChart, Pie } from 'recharts';
import { MetricsContext } from "@/context/MetricsContext";
import Spinner from "@/components/spinner/spinner";
import AutoSizeText from "@/components/autosizeText/autosizeText";

export default function Overview() {
    const data = [
        {
            name: 'Page A',
            uv: 4000,
            pv: 2400,
            amt: 2400,
        },
        {
            name: 'Page B',
            uv: 3000,
            pv: 1398,
            amt: 2210,
        },
        {
            name: 'Page C',
            uv: 2000,
            pv: 9800,
            amt: 2290,
        },
        {
            name: 'Page D',
            uv: 2780,
            pv: 3908,
            amt: 2000,
        },
        {
            name: 'Page E',
            uv: 1890,
            pv: 4800,
            amt: 2181,
        },
        {
            name: 'Page F',
            uv: 2390,
            pv: 3800,
            amt: 2500,
        },
        {
            name: 'Page G',
            uv: 3490,
            pv: 4300,
            amt: 2100,
        },
    ];
    const { metrics, loading, error } = useContext(MetricsContext);

    useEffect(() => {
        console.log(metrics);
    }, [metrics])

    const successRateErrorRateData = [
        { name: "Success", value: metrics?.error_rate_success_rate.success_rate, fill: "#accc9c" },
        { name: "Error", value: metrics?.error_rate_success_rate.error_rate, fill: "#bb3f46" },
    ];

    const stickAndSlipData = [
        { name: "Stick", value: metrics?.stick_and_slip_pages.stick, fill: "#58508d" },
        { name: "Slip", value: metrics?.stick_and_slip_pages.slip, fill: "#ffa600" },
    ];

    return (
        <div className={styles.flex}>
            <Breadcrumb text={"Overview"} />
            {loading ? <Spinner /> :
                error ? <p>Error: {error}</p> : metrics &&
                    <div className={styles.grid}>
                        {/* Primera Fila */}
                        <Card title="Success Rate & Error Rate" infoIcon className={`${styles.item1}`}>
                            <div className={styles.display}>
                                <div>
                                    <AutoSizeText minSize="18px" maxSize="36px" colorVar="color-chart-green" textAlign="right">
                                        {metrics.error_rate_success_rate.success_rate.toFixed(2)}%
                                    </AutoSizeText>
                                    <AutoSizeText minSize="18px" maxSize="36px" colorVar="color-chart-red" textAlign="right">
                                        {metrics.error_rate_success_rate.error_rate.toFixed(2)}%
                                    </AutoSizeText>
                                </div>
                                <ResponsiveContainer width="100%" height={100}>
                                    <PieChart>
                                        <Pie
                                            data={successRateErrorRateData}
                                            cx="50%"
                                            cy="50%"
                                            innerRadius={20}
                                            outerRadius={40}
                                            fill="#8884d8"
                                            paddingAngle={0}
                                            dataKey="value"
                                        />
                                    </PieChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>

                        <Card title="Average Response Time" infoIcon className={`${styles.item2}`}>
                            <div className={styles.display}>
                                <AutoSizeText colorVar="color-text" textAlign="center">
                                    {metrics.average_response_time.avg_response_time.toFixed(2)}s
                                </AutoSizeText>
                            </div>
                        </Card>

                        <Card title="Average Time Spent on Site" infoIcon className={`${styles.item3}`}>
                            <div className={styles.display}>
                                <AutoSizeText colorVar="color-text" textAlign="center">
                                    {metrics.average_time_spent_on_site.toFixed(2)}s
                                </AutoSizeText>
                            </div>
                        </Card>

                        {/* Segunda Fila */}
                        <Card title="Pages Stick and Slip" infoIcon className={`${styles.item4}`}>
                            <div className={styles.display}>
                                <div>
                                    <AutoSizeText minSize="18px" maxSize="36px" colorVar="color-chart-purple" textAlign="right">
                                        {(metrics?.stick_and_slip_pages.stick * 100).toFixed(2)}%
                                    </AutoSizeText>
                                    <AutoSizeText minSize="18px" maxSize="36px" colorVar="color-chart-orange" textAlign="right">
                                        {(metrics?.stick_and_slip_pages.slip * 100).toFixed(2)}%
                                    </AutoSizeText>
                                </div>
                                <ResponsiveContainer width="100%" height={100}>
                                    <PieChart>
                                        <Pie
                                            data={stickAndSlipData}
                                            cx="50%"
                                            cy="50%"
                                            innerRadius={20}
                                            outerRadius={40}
                                            fill="#8884d8"
                                            paddingAngle={0}
                                            dataKey="value"
                                        />
                                    </PieChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>

                        <Card title="Average time spent per page" infoIcon className={`${styles.item5}`}>
                            <div className={styles.display}>
                                <AutoSizeText colorVar="color-text" textAlign="center">
                                    {metrics.average_time_spent_per_page.toFixed(2)}s
                                </AutoSizeText>
                            </div>
                        </Card>

                        <Card title="Downloading hits ratio" infoIcon className={`${styles.item6}`}>
                            <div className={styles.display}>
                                <AutoSizeText colorVar="color-text" textAlign="center">
                                    {metrics.downloadable_resources_hits_ratio} hits
                                </AutoSizeText>
                            </div>
                        </Card>
                    </div>}
        </div>
    )
}
