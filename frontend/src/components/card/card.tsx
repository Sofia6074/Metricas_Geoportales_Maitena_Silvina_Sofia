import { ReactNode } from 'react';
import styles from './card.module.css';
import Icon from '@/components/icon/icon';
import Tooltip from '@/components/tooltip/tooltip';

type CardProps = {
    title?: string;
    children: ReactNode;
    infoIcon?: boolean;
    tooltipText?: string;
    tooltipDirection?: "top" | "right" | "bottom" | "left";
    className?: string;
};

export default function Card({ title, children, infoIcon, tooltipText, tooltipDirection, className }: CardProps) {
    return (
        <div className={`${styles.card} ${className || ''}`}>
            {title && (
                <div className={styles.header}>
                    <h3 className={styles.title}>{title}</h3>
                    {infoIcon &&
                        <Tooltip content={tooltipText ?? ''} direction={tooltipDirection ?? 'top'}>
                            <Icon iconName={'info-outline'} className={styles.infoIcon}></Icon>
                        </Tooltip>
                    }
                </div>
            )}
            <div className={styles.content}>{children}</div>
        </div>
    );
}
