from collections import Counter
import networkx as nx
import polars as pl
from scripts_py.common.log_cleaner import convert_timestamp


def node_map_between_top_content_pages(logs_df):
    """
    Creates a node map that shows transitions between the home page and the
    most visited pages on the website.
    """

    logs_df = convert_timestamp(logs_df)

    data = logs_df.select([
        pl.col('ip'),
        pl.col('timestamp'),
        pl.col('request_url').alias('url'),
        pl.col('referer')
    ])

    homepage_url_keywords = [
        'https://visualizador.ide.uy/',
        'https://visualizador.ide.uy/ideuy/core/load_public_project/ideuy/'
    ]

    data_sorted = data.sort(["ip", "timestamp"])

    graph = nx.DiGraph()

    home_to_urls_counter = {home: Counter() for home in homepage_url_keywords}

    for session_group in data_sorted.group_by("ip"):
        session_data = session_group[1]
        session_urls = session_data["url"].to_list()
        referers = session_data["referer"].to_list()

        for i, current_url in enumerate(session_urls):
            current_referer = referers[i] if i < len(referers) else ''

            if current_referer in homepage_url_keywords:
                home_to_urls_counter[current_referer][current_url] += 1

                if graph.has_edge(current_referer, current_url):
                    graph.edges[current_referer, current_url]['weight'] += 1
                else:
                    graph.add_edge(current_referer, current_url, weight=1)

    result = []

    for home, counter in home_to_urls_counter.items():
        top_5_next_urls = counter.most_common(5)

        if top_5_next_urls:
            destinations = [(url, count) for url, count in top_5_next_urls]
            result.append((home, destinations))

    for origin, destinations in result:
        dest_str = ", ".join([f"{des[0]} ({des[1]})" for des in destinations])
        print(f"Origin(Home Page): {origin} -> Top 5 Destinations: {dest_str}")

    return result
