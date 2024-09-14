import styles from './Sidebar.module.css';
import Link from 'next/link';
import Icon from '@/components/icon/icon';

export default function Sidebar() {
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
					<li>
						<Link href="/" className={styles.menuItem}>
							<Icon iconName="boxes-outline" size={20} />
							Overview
						</Link>
					</li>
					<li>
						<Link href="/users" className={styles.menuItem}>
							<Icon iconName="person-outline" size={20} />Users
						</Link>
					</li>
					<li>
						<Link href="/maps" className={styles.menuItem}>
							<Icon iconName="location-outline" size={20} />Maps
						</Link>
					</li>
					<li>
						<Link href="/navigability" className={styles.menuItem}>
							<Icon iconName="compass-outline" size={20} />Navigability
						</Link>
					</li>
					<li>
						<Link href="/search" className={styles.menuItem}>
							<Icon iconName="search-outline" size={20} />Search
						</Link>
					</li>
				</ul>
			</nav>
		</div>
	);
}
