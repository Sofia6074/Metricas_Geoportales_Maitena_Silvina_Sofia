"use client";

import Breadcrumb from "@/components/breadcrumb/breadcrumb";
import styles from "./overview.module.css"
import Card from "@/components/card/card";
import { LineChart, Line, ResponsiveContainer, Bar, BarChart, PieChart, Pie, Cell } from 'recharts';


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
        { name: "Group A", value: 400, fill: "#0088FE" },
        { name: "Group B", value: 300, fill: "#00C49F" },
        { name: "Group C", value: 300, fill: "#FFBB28" },
        { name: "Group D", value: 200, fill: "#FF8042" }
    ];

    return (
        <div className={styles.flex}>
            <Breadcrumb text={"Overview"} />
            <div className={styles.grid}>
                {/* Primera Fila */}
                <div className="grid-item item1">
                    <Card title="Quality Score" infoIcon>
                        <div className={styles.chart}>
                            <ResponsiveContainer width="100%" height="100%">
                                <BarChart width={150} height={40} data={data}>
                                    <Bar dataKey="uv" fill="#8884d8" />
                                </BarChart>
                            </ResponsiveContainer>
                        </div>
                    </Card>
                </div>

                <div className="grid-item item2">
                    <Card title="Success Rate & Error Rate" infoIcon>
                        <div className={styles.chart}>
                            <ResponsiveContainer width="100%" height={200}>
                                <PieChart>
                                    <Pie
                                        data={donutData}
                                        cx="50%"
                                        cy="50%"
                                        innerRadius={40}
                                        outerRadius={60}
                                        fill="#8884d8"
                                        paddingAngle={0}
                                        dataKey="value"
                                    />
                                </PieChart>
                            </ResponsiveContainer>
                        </div>
                    </Card>
                </div>

                <div className="grid-item item3">
                    <Card title="Average Response Time" infoIcon>
                        <div className={styles.chart}>
                            <ResponsiveContainer width="100%" height="100%">
                                <BarChart width={150} height={40} data={data}>
                                    <Bar dataKey="uv" fill="#8884d8" />
                                </BarChart>
                            </ResponsiveContainer>
                        </div>
                    </Card>
                </div>

                <div className="grid-item item4">
                    <Card title="Average Time Spent on Site" infoIcon>
                        <div className={styles.chart}>
                            <ResponsiveContainer width="100%" height="100%">
                                <BarChart width={150} height={40} data={data}>
                                    <Bar dataKey="uv" fill="#8884d8" />
                                </BarChart>
                            </ResponsiveContainer>
                        </div>
                    </Card>
                </div>

                {/* Segunda Fila */}
                <div className="grid-item item5">
                    <Card title="Average time spent per page" infoIcon>
                        <div className={styles.chart}>
                            <ResponsiveContainer width="100%" height="100%">
                                <LineChart width={300} height={100} data={data}>
                                    <Line type="monotone" dataKey="pv" stroke="#8884d8" strokeWidth={2} />
                                </LineChart>
                            </ResponsiveContainer>
                        </div>
                    </Card>
                </div>
                <div className="grid-item item6">
                    <Card title="Most Viewed Pages" infoIcon>
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
                </div>

                {/* Tercera Fila */}
                <div className="grid-item item7">
                    <Card title="Pages Stick and Slip" infoIcon>
                        <div className={styles.chart}>
                            <ResponsiveContainer width="100%" height="100%">
                                <BarChart width={150} height={40} data={data}>
                                    <Bar dataKey="uv" fill="#8884d8" />
                                </BarChart>
                            </ResponsiveContainer>
                        </div>
                    </Card>
                </div>

                <div className="grid-item item8">
                    <Card title="Downloading hits ratio" infoIcon>
                        <div className={styles.chart}>
                            <ResponsiveContainer width="100%" height="100%">
                                <BarChart width={150} height={40} data={data}>
                                    <Bar dataKey="uv" fill="#8884d8" />
                                </BarChart>
                            </ResponsiveContainer>
                        </div>
                    </Card>
                </div>

                <div className="grid-item item9">
                    <Card title="Average Time Spent on Site - Displays" infoIcon>
                        <div className={styles.chart}>
                            <ResponsiveContainer width="100%" height="100%">
                                <BarChart width={150} height={40} data={data}>
                                    <Bar dataKey="uv" fill="#8884d8" />
                                </BarChart>
                            </ResponsiveContainer>
                        </div>
                    </Card>
                </div>
            </div>
        </div>
    )
}