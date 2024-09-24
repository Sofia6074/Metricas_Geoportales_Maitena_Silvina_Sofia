import Image from 'next/image';
import styles from './icon.module.css';

type IconProps = {
    iconName: string;
    size?: number;
    className?: string;
    color?: string;
};

export default function Icon({ iconName, size = 24, className, color }: IconProps) {
    const iconPath = `/assets/${iconName}.svg`;

    return (
        <Image
            src={iconPath}
            alt={`${iconName} icon`}
            width={size}
            height={size}
            className={`${styles.icon} ${className || ''}`}
            style={{ filter: color === 'white' ? 'invert(1)' : 'none' }}
        />
    );
}