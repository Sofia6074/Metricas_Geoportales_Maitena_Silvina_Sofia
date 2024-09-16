import Breadcrumb from "@/components/breadcrumb/breadcrumb";
import styles from "./overview.module.css"
import Card from "@/components/card/card";

export default function Overview() {
    return (
        <div className={styles.flex}>
            <Breadcrumb text={"Overview"} />
            <div className={styles.grid}>
                <Card title="Average time spent per page" infoIcon>
                    <div className={styles.chart}>[Gráfico]</div>
                </Card>

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
        </div>
    )
}