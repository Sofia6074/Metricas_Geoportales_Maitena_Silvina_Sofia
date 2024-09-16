import { ReactNode } from 'react';
import styles from './Card.module.css';
import Icon from '../icon/icon';
import Tooltip from '../tooltip/tooltip';

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
                    {infoIcon &&
                        <Tooltip content="This is where we explain what each metric means." direction="right">
                            <Icon iconName={'info-outline'} className={styles.infoIcon}></Icon>
                        </Tooltip>
                    }
                </div>
            )}
            <div className={styles.content}>{children}</div>
        </div>
    );
}
