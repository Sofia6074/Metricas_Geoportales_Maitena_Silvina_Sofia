import styles from './Sidebar.module.css';
import Link from 'next/link';

export default function Sidebar() {
	return (
		<div className={styles.sidebar}>
			<div className={styles.logo}>
				<img src="/logo.png" alt="Geometrics Logo" />
				<h1>Geometrics</h1>
			</div>
			<div className={styles.upload}>
				<input type="file" id="file" className={styles.inputFile} />
				<label htmlFor="file" className={styles.fileLabel}>
					geo-web-server-logs.csv
				</label>
			</div>
			<nav className={styles.menu}>
				<p>Menu</p>
				<ul>
					<li>
						<Link href="/overview">
							<a className={styles.menuItem}>
								<span role="img" aria-label="Overview Icon">🗂️</span> Overview
							</a>
						</Link>
					</li>
					<li>
						<Link href="/users">
							<a className={styles.menuItem}>
								<span role="img" aria-label="Users Icon">👤</span> Users
							</a>
						</Link>
					</li>
					<li>
						<Link href="/maps">
							<a className={styles.menuItem}>
								<span role="img" aria-label="Maps Icon">📍</span> Maps
							</a>
						</Link>
					</li>
					<li>
						<Link href="/navigability">
							<a className={styles.menuItem}>
								<span role="img" aria-label="Navigability Icon">🔍</span> Navigability
							</a>
						</Link>
					</li>
					<li>
						<Link href="/search">
							<a className={styles.menuItem}>
								<span role="img" aria-label="Search Icon">🔍</span> Search
							</a>
						</Link>
					</li>
				</ul>
			</nav>
		</div>
	);
}
