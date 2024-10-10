import difflib
from datetime import datetime
from itertools import combinations

import polars as pl


def calculate_related_search_parameters(logs_df):
    """
    Calculate the similarity ratio between all possible search combinations
    within a session.
    Returns a DataFrame with columns: session, similar search terms, and
    Jaccard similarity index.

    This function first processes the logs by extracting search parameters,
    filtering unnecessary results, and calculating the sessions.
    It then computes the Jaccard similarity for the search
    terms within each session and stores the results in a DataFrame.
    """
    map_requests_df = convert_timestamp(logs_df)

    map_requests_df = extract_search_params(map_requests_df)

    results = []

    for row in map_requests_df.iter_rows(named=True):
        search_list = row["search_list"]
        search_time = row["search_time"]
        session_id = row["unique_session_id"]

        # Unnest lists if necessary.
        if isinstance(search_list[0], list):
            search_list = [item for sublist in search_list for item in sublist]

        if isinstance(search_time[0], list):
            search_time = [item for sublist in search_time for item in sublist]

        search_list, search_time = zip(*sorted(zip(search_list, search_time)))

        search_list = filter_search_terms(search_list)

        session_results = calculate_jaccard_similarity(
            search_list, search_time, session_id
        )

        results.extend(session_results)

    results_df = pl.DataFrame(
        results,
        schema=["unique_session_id", "search_pair", "jaccard_similarity"]
    )

    sorted_results_df = results_df.sort("jaccard_similarity", descending=True)
    print(sorted_results_df.head(10))
    return sorted_results_df.head(10)


def extract_search_params(map_requests_df):
    """
    Extracts the search parameters from the URLs and filters out irrelevant
    requests.

    This method specifically targets URLs that contain
    search parameters (e.g., "q=") and extracts those values.
    Additionally, it filters out non-successful HTTP responses,
    autocompleted requests based on their response size, and requests
    with short response times.
    """
    map_requests_df = map_requests_df.with_columns(
        pl.when(pl.col("request_url").str.contains(r"q="))
        .then(pl.col("request_url").str.extract(r"q=([^&]+)", 1))
        .otherwise(None).alias("search_params")
    )

    map_requests_df = map_requests_df.filter(pl.col("search_params").is_not_null())
    map_requests_df = map_requests_df.filter(pl.col("status_code") == 200)
    map_requests_df = map_requests_df.filter(pl.col("response_size") > 100)
    map_requests_df = map_requests_df.filter(pl.col("response_time") > 50)

    grouped_sessions = map_requests_df.group_by("unique_session_id").agg(
        [
            pl.col("search_params").implode().alias("search_list"),
            pl.col("search_time").implode().alias("search_time")
        ]
    )

    return grouped_sessions


def calculate_jaccard_similarity(
        search_list, search_time, session_id, time_threshold=10,
        max_distance=2, max_comb_size=3):
    """
    Calculate Jaccard similarity between all possible combinations of searches
    within a session.

    The function generates combinations of search terms from the session and
    checks if they are autocompleted versions of one another.
    If not, it calculates the Jaccard similarity between
    the search terms. The results are returned in a list containing
    the session ID, the search
    pairs, and their corresponding Jaccard similarity score.
    """
    session_results = []
    for size in range(2, min(max_comb_size + 1, len(search_list) + 1)):
        for combo in combinations(range(len(search_list)), size):
            sorted_combo = sorted(combo, key=lambda i: search_time[i])

            terms = [search_list[i] for i in sorted_combo]
            times = [search_time[i] for i in sorted_combo]

            if any(is_autocomplete(terms[i], terms[j], max_distance=max_distance)
                   for i, j in combinations(range(len(terms)), 2)):
                continue

            time_diffs = [
                (convert_to_datetime(times[i]) - convert_to_datetime(times[i - 1])).total_seconds()
                for i in range(1, len(times))
            ]
            if any(diff > time_threshold for diff in time_diffs):
                continue

            list_combo = [set(term.split('+')) for term in terms]
            intersection = list_combo[0].intersection(*list_combo[1:])
            union = list_combo[0].union(*list_combo[1:])

            jaccard_sim = len(intersection) / len(union) if union else 0

            if jaccard_sim > 0.1:
                session_results.append({
                    "unique_session_id": session_id,
                    "search_pair": " - ".join(terms),
                    "jaccard_similarity": round(jaccard_sim, 2)
                })

    return session_results


def is_autocomplete(term1, term2, max_distance=1):
    """
    Determines whether one search term is an autocompleted version of another.

    The function checks if the edit distance between two terms is small enough
    to be considered
    an autocompletion. It also considers the length difference
    between the terms.
    """
    distance = levenshtein_distance(term1, term2)
    length_difference = abs(len(term1) - len(term2))

    return distance <= max_distance and length_difference <= max_distance


def levenshtein_distance(a, b):
    """
    Calculate the Levenshtein distance between two strings.

    The Levenshtein distance measures the minimum number of
    single-character edits required
    to transform one string into another.
    """
    return len(list(difflib.ndiff(a, b))) // 2


def convert_to_datetime(date):
    """
    Convert a string to a datetime object if necessary.
    """
    return datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f")


def filter_search_terms(search_list):
    """
    Filter search terms that have more than two characters.
    """
    return [term for term in search_list if len(term) > 2]


def convert_timestamp(logs_df):
    """
    Convert the 'timestamp' column to datetime format, including microseconds.
    """
    logs_df = logs_df.with_columns(
        pl.col("timestamp").str.replace(" UTC", "").alias("search_time")
    )
    return logs_df
