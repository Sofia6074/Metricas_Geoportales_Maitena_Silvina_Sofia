import { ReactNode } from 'react';
import styles from './card.module.css';
import Icon from '../icon/icon';
import Tooltip from '../tooltip/tooltip';

type CardProps = {
    title?: string;
    children: ReactNode;
    infoIcon?: boolean;
    className?: string;
};

export default function Card({ title, children, infoIcon, className }: CardProps) {
    return (
        <div className={`${styles.card} ${className || ''}`}>
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
