import { ReactNode } from 'react';
import styles from './Card.module.css';
import Icon from '../icon/icon';

type CardProps = {
    title?: string;
    children: ReactNode;
    infoIcon?: boolean;
};

export default function Card({ title, children, infoIcon }: CardProps) {
    return (
        <div className={styles.card}>
            {title && (
                <div className={styles.header}>
                    <h3 className={styles.title}>{title}</h3>
                    {infoIcon && <Icon iconName={'info-outline'} className={styles.infoIcon}></Icon>}
                </div>
            )}
            <div className={styles.content}>{children}</div>
        </div>
    );
}
