import matplotlib.pyplot as plt
import networkx as nx
import polars as pl
from collections import Counter

from metrics.metrics_utils import filter_session_outliers


def node_map_between_top_content_pages(logs_df):
    logs_df = filter_session_outliers(logs_df)

    data = logs_df.select([
        pl.col('ip'),
        pl.col('timestamp'),
        pl.col('request_url').alias('url'),
        pl.col('referer')
    ])

    homepage_url_keywords = ['https://visualizador.ide.uy/', 'https://visualizador.ide.uy/ideuy/core/load_public_project/ideuy/']

    data_sorted = data.sort(["ip", "timestamp"])

    G = nx.DiGraph()

    for homepage_url in homepage_url_keywords:
        if homepage_url not in G:
            G.add_node(homepage_url)

    next_url_counter = Counter()

    for session_group in data_sorted.group_by("ip"):
        session_data = session_group[1]
        session_urls = session_data["url"].to_list()
        referers = session_data["referer"].to_list()

        for i in range(len(session_urls) - 1):
            current_url = session_urls[i]
            next_url = session_urls[i + 1]
            current_referer = referers[i]

            current_referer_normalized = current_referer.strip().lower()
            homepage_url_keywords_normalized = [url.strip().lower() for url in homepage_url_keywords]

            if any(keyword in current_referer_normalized for keyword in homepage_url_keywords_normalized):

                next_url_counter[next_url] += 1

                if G.has_edge(current_url, next_url):
                    G.edges[current_url, next_url]['weight'] += 1
                else:
                    G.add_edge(current_url, next_url, weight=1)

    top_5_next_urls = set(url for url, _ in next_url_counter.most_common(5))

    subgraph_nodes = set(homepage_url_keywords).union(top_5_next_urls)
    subgraph = G.subgraph(subgraph_nodes)

    plt.figure(figsize=(18, 12))
    pos = nx.spring_layout(subgraph, seed=42, k=1.5)
    nx.draw_networkx_nodes(subgraph, pos, node_size=700)
    nx.draw_networkx_edges(
        subgraph, pos, edgelist=subgraph.edges(), arrowstyle='->', arrowsize=20, connectionstyle='arc3,rad=0.2'
    )
    nx.draw_networkx_labels(subgraph, pos, font_size=12)

    edge_labels = {(u, v): f"{d['weight']}" for u, v, d in subgraph.edges(data=True)}
    nx.draw_networkx_edge_labels(subgraph, pos, edge_labels=edge_labels, font_size=10, rotate=True)

    plt.title('Mapa de Navegabilidad para las Top 5 URLs de Destino desde la Página Principal')
    plt.show()

