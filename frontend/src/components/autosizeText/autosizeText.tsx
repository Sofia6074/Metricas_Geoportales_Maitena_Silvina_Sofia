import React, { ReactNode } from 'react';
import styles from './AutoSizeText.module.css';

type AutoSizeTextProps = {
    children: ReactNode;
    colorVar?: string;
    minSize?: string;
    maxSize?: string;
    textAlign?: 'left' | 'center' | 'right';
};

export default function AutoSizeText({
    children,
    minSize = '1rem',
    maxSize = '5rem',
    colorVar = 'color-text',
    textAlign = 'center',
}: AutoSizeTextProps) {
    return (
        <p
            className={styles.autoSizeText}
            style={{
                fontSize: `clamp(${minSize}, 5vw, ${maxSize})`,
                color: `var(--${colorVar})`,
                textAlign,
                maxWidth: '100%',
                maxHeight: '100%',
            }}
        >
            {children}
        </p>
    );
}
