import styles from "./spinner.module.css";

export default function Spinner() {
    return (
        <div className={styles.container}>
            <span className={styles.loader}></span>
        </div>
    )
}
