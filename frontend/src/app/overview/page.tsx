"use client";

import { useContext, useEffect, useState } from "react";
import Breadcrumb from "@/components/breadcrumb/breadcrumb";
import styles from "./overview.module.css"
import Card from "@/components/card/card";
import { ResponsiveContainer, PieChart, Pie, Tooltip, Bar, BarChart, XAxis, TooltipProps } from 'recharts';
import { MetricsContext } from "@/context/MetricsContext";
import Spinner from "@/components/spinner/spinner";
import AutoSizeText from "@/components/autosizeText/autosizeText";
import { ValueType, NameType } from "recharts/types/component/DefaultTooltipContent";

export default function Overview() {
    const [primaryChartColor, setPrimaryChartColor] = useState<string>('');
    const { metrics, loading, error } = useContext(MetricsContext);

    const [colors, setColors] = useState({
        green: '',
        red: '',
        orange: '',
        purple: ''
    });

    const getCssVariable = (variableName: string): string => {
        const rootStyle = getComputedStyle(document.documentElement);
        return rootStyle.getPropertyValue(variableName).trim();
    };

    const successRateErrorRateData = [
        { name: "Success", value: metrics?.error_rate_success_rate.success_rate, fill: colors.green },
        { name: "Error", value: metrics?.error_rate_success_rate.error_rate, fill: colors.red },
    ];

    const stickAndSlipData = [
        { name: "Stick", value: metrics?.stick_and_slip_pages.stick, fill: colors.purple },
        { name: "Slip", value: metrics?.stick_and_slip_pages.slip, fill: colors.orange },
    ];

    const CustomTooltip = ({ active, payload, label }: TooltipProps<ValueType, NameType>) => {
        if (active && payload && payload.length) {
            return (
                <div className={styles.customTooltip}>
                    <p className={styles.tooltipTitle}>{label}</p>
                    <p>{`Count: ${payload[0].value}`}</p>
                </div>
            );
        }

        return null;
    };

    useEffect(() => {
        const rootStyle = getComputedStyle(document.documentElement);
        const purpleColor = rootStyle.getPropertyValue('--color-chart-purple').trim();
        setPrimaryChartColor(purpleColor);

        setColors({
            green: getCssVariable('--color-chart-green'),
            red: getCssVariable('--color-chart-red'),
            orange: getCssVariable('--color-chart-orange'),
            purple: getCssVariable('--color-chart-purple'),
        });
    }, []);

    return (
        <div className={styles.flex}>
            <Breadcrumb text={"Overview"} />
            {loading ? <Spinner /> :
                error ? <p>Error: {error}</p> : metrics &&
                    <div className={styles.grid}>
                        {/* Primera Fila */}
                        <Card title="Success rate & error rate" infoIcon tooltipText="Rate of responses that result in error and in success." className={`${styles.item1}`}>
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
                                            paddingAngle={0}
                                            dataKey="value"
                                        />
                                        <Tooltip />
                                    </PieChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>

                        <Card title="Average response time" infoIcon tooltipText="Average response time of all the requests made to the server." className={`${styles.item2}`}>
                            <div className={styles.display}>
                                <AutoSizeText maxSize="40px" colorVar="color-text" textAlign="center">
                                    {metrics.average_response_time.avg_response_time.toFixed(2)}s
                                </AutoSizeText>
                            </div>
                        </Card>

                        <Card title="Average time spent on site" infoIcon tooltipText="Average time each user spends on the site." tooltipDirection="left" className={`${styles.item3}`}>
                            <div className={styles.display}>
                                <AutoSizeText maxSize="40px" colorVar="color-text" textAlign="center">
                                    {metrics.average_time_spent_on_site.toFixed(2)}s
                                </AutoSizeText>
                            </div>
                        </Card>

                        {/* Segunda Fila */}
                        <Card title="Pages stick and slip" infoIcon tooltipText="Slip shows the ratio of single access views to entry views. Stick indicates page retention ability." className={`${styles.item4}`}>
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
                                            paddingAngle={0}
                                            dataKey="value"
                                        />
                                        <Tooltip
                                            formatter={(value) => `${((+value) * 100).toFixed(2)}%`}
                                        />
                                    </PieChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>

                        <Card title="Average time spent per page" infoIcon tooltipText="Average time the visitors spends per page." className={`${styles.item5}`}>
                            <div className={styles.display}>
                                <AutoSizeText maxSize="40px" colorVar="color-text" textAlign="center">
                                    {metrics.average_time_spent_per_page.toFixed(2)}s
                                </AutoSizeText>
                            </div>
                        </Card>

                        <Card title="Downloading hits" infoIcon tooltipText="Amount of times downloadable resources are downloaded." tooltipDirection="left" className={`${styles.item6}`}>
                            <div className={styles.display}>
                                <AutoSizeText maxSize="40px" colorVar="color-text" textAlign="center">
                                    {metrics.downloadable_resources_hits} hits
                                </AutoSizeText>
                            </div>
                        </Card>

                        <Card title="Devices usage" infoIcon tooltipText="Device classification to count how many times each device type was used." tooltipDirection="right" className={`${styles.item7}`}>
                            <div className={styles.display}>
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart width={60} height={150} data={metrics.device_usage
                                        .filter(device => device.device_type !== "other")
                                        .map(device => ({
                                            ...device,
                                            device_type: device.device_type.charAt(0).toUpperCase() + device.device_type.slice(1)
                                        }))
                                        .sort((a, b) => a.device_usage_count - b.device_usage_count)
                                    }>
                                        <XAxis dataKey="device_type" />
                                        <Tooltip content={<CustomTooltip />} />
                                        <Bar dataKey="device_usage_count" fill={primaryChartColor} />
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </Card>
                    </div>}
        </div>
    )
}
