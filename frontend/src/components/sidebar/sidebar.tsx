"use client";

import styles from './sidebar.module.css';
import Link from 'next/link';
import Icon from '@/components/icon/icon';
import { usePathname } from 'next/navigation';
import { useState, useEffect } from 'react';

export default function Sidebar() {
	const pathname = usePathname();
	const [selectedMenuItem, setSelectedMenuItem] = useState(pathname);

	const menuItems = [
		{ name: 'Overview', icon: 'boxes-outline', path: '/' },
		{ name: 'Users', icon: 'person-outline', path: '/users' },
		{ name: 'Maps', icon: 'location-outline', path: '/maps' },
		{ name: 'Navigability', icon: 'compass-outline', path: '/navigability' },
		{ name: 'Search', icon: 'search-outline', path: '/search' },
	];

	const handleMenuClick = (path: string) => {
		setSelectedMenuItem(path);
	};

	useEffect(() => {
		setSelectedMenuItem(pathname);
	}, [pathname]);

	return (
		<div className={styles.sidebar}>
			<div className={styles.logo}>
				<Icon iconName="logo" size={30} />
				<h1>Geometrics</h1>
			</div>
			<div className={styles.upload}>
				<input type="file" id="file" className={styles.inputFile} />
				<label htmlFor="file" className={styles.fileLabel}>
					geo-web-server-logs.csv
				</label>
			</div>
			<nav className={styles.menu}>
				<p>MENU</p>
				<ul>
					{menuItems.map((item) => (
						<li key={item.name}>
							<Link href={item.path} passHref legacyBehavior>
								<a
									className={`${styles.menuItem} ${selectedMenuItem === item.path ? styles.active : ''}`}
									onClick={() => handleMenuClick(item.path)}
								>
									<Icon iconName={item.icon} size={20} color={selectedMenuItem === item.path ? 'white' : 'inherit'} />
									{item.name}
								</a>
							</Link>
						</li>
					))}
				</ul>
			</nav>
		</div>
	);
}
