import styles from "./breadcrumb.module.css"

type BreadcrumbProps = {
    text: string;
};

export default function Breadcrumb({ text }: BreadcrumbProps) {
    return (
        <header className={styles.breadcrumb}>
            <h1>{text}</h1>
        </header>
    )
}