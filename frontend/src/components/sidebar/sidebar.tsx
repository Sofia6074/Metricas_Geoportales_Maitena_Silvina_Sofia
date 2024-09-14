import styles from './Sidebar.module.css';
import Link from 'next/link';
import Logo from '@/components/logo/logo';

export default function Sidebar() {
	return (
		<div className={styles.sidebar}>
			<div className={styles.logo}>
				<Logo />
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
						<Link href="/overview" className={styles.menuItem}>
							<span role="img" aria-label="Overview Icon">🗂️</span>Overview
						</Link>
					</li>
					<li>
						<Link href="/users" className={styles.menuItem}>
							<span role="img" aria-label="Users Icon">👤</span>Users
						</Link>
					</li>
					<li>
						<Link href="/maps" className={styles.menuItem}>
							<span role="img" aria-label="Maps Icon">📍</span>Maps
						</Link>
					</li>
					<li>
						<Link href="/navigability" className={styles.menuItem}>
							<span role="img" aria-label="Navigability Icon">🧭</span>Navigability
						</Link>
					</li>
					<li>
						<Link href="/search" className={styles.menuItem}>
							<span role="img" aria-label="Search Icon">🔍</span>Search
						</Link>
					</li>
				</ul>
			</nav>
		</div>
	);
}
