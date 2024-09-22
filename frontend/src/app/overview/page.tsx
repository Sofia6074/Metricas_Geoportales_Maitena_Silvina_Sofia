"use client";

import { useContext, useEffect } from "react";
import Breadcrumb from "@/components/breadcrumb/breadcrumb";
import styles from "./overview.module.css"
import Card from "@/components/card/card";
import { LineChart, Line, ResponsiveContainer, Bar, BarChart, PieChart, Pie } from 'recharts';
import { MetricsContext } from "@/context/MetricsContext";
import Loading from "@/components/loading/loading";

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

    const donutData = [
        { name: "Group B", value: 50, fill: "#bb3f46" },
        { name: "Group A", value: 400, fill: "#accc9c" },
    ];

    const { metrics, loading, error } = useContext(MetricsContext);

    useEffect(() => {
        console.log(metrics);
    }, [metrics])

    return (
        <div className={styles.flex}>
            <Breadcrumb text={"Overview"} />
            {loading || true ? <Loading /> :
                error ? <p>Error: {error}</p> :
                    <div className={styles.grid}>
                        {/* Primera Fila */}
                        <Card title="Quality Score" infoIcon className={`${styles.item1}`}>
                            <div className={styles.chart}>
                                78%
                            </div>
                        </Card>

                        <Card title="Success Rate & Error Rate" infoIcon className={`${styles.item2}`}>
                            <div className={styles.chart}>
                                <ResponsiveContainer width="100%" height={100}>
                                    <PieChart>
                                        <Pie
                                            data={donutData}
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

                        <Card title="Average Response Time" infoIcon className={`${styles.item3}`}>
                            <div className={styles.chart}>
                                351.15s
                            </div>
                        </Card>

                        <Card title="Average Time Spent on Site" infoIcon className={`${styles.item4}`}>
                            <div className={styles.chart}>
                                8 min
                            </div>
                        </Card>

                        {/* Segunda Fila */}
                        <Card title="Average time spent per page" infoIcon className={`${styles.item5}`}>
                            <div className={styles.chart}>
                                <ResponsiveContainer width="100%" height="100%">
                                    <LineChart width={300} height={100} data={data}>
                                        <Line type="monotone" dataKey="pv" stroke="#8884d8" strokeWidth={2} />
                                    </LineChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>

                        <Card title="Most Viewed Pages" infoIcon className={`${styles.item6}`}>
                            <table className={styles.table}>
                                <thead>
                                    <tr>
                                        <th>Base URL</th>
                                        <th>Visits</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr>
                                        <td>/geoserver-raster/gwc/service/secto...</td>
                                        <td>72577</td>
                                    </tr>
                                </tbody>
                            </table>
                        </Card>

                        {/* Tercera Fila */}
                        <Card title="Pages Stick and Slip" infoIcon className={`${styles.item7}`}>
                            <div className={styles.chart}>
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart width={150} height={40} data={data}>
                                        <Bar dataKey="uv" fill="#8884d8" />
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>

                        <Card title="Downloading hits ratio" infoIcon className={`${styles.item8}`}>
                            <div className={styles.chart}>
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart width={150} height={40} data={data}>
                                        <Bar dataKey="uv" fill="#8884d8" />
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>

                        <Card title="Average Time Spent on Site - Displays" infoIcon className={`${styles.item9}`}>
                            <div className={styles.chart}>
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart width={150} height={40} data={data}>
                                        <Bar dataKey="uv" fill="#8884d8" />
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>
                    </div>}
        </div>
    )
}
