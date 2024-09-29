from collections import Counter
import polars as pl
from metrics.metrics_utils import filter_session_outliers


def calculate_most_repeated_words_filtered(logs_df):
    """
    Calculate the most repeated words in a given DataFrame of logs.

    The method filters log entries to focus on search queries, cleans and
    processes the terms,removes autocompleted terms, splits search terms
    by commas, and finally counts the occurrences
    of each unique term.

    Args:
        logs_df (DataFrame): Input DataFrame containing log entries.

    Returns:
        DataFrame: A DataFrame containing the top 5 most
        repeated words and their counts.
    """

    df = filter_session_outliers(logs_df)

    df_search = df.filter(pl.col('request_url').str.contains(r'q='))

    df_search = df_search.with_columns(
        pl.col('request_url').str.extract(r'q=([^&]*)')
        .str.replace_all(r'\+', ' ')
        .str.replace_all(r'%2f', '/')
        .str.replace_all(r'%2F', '/')
        .str.replace_all(r'%20', ' ')
        .str.replace_all(r'%2520', ' ')
        .str.replace_all(r'%c3%91', 'ñ')
        .str.replace_all(r',\s+', ',')
        .str.strip_chars()
        .str.to_lowercase()
        .alias('search_term')
    )

    if 'timestamp' not in df_search.columns:
        df_search = df_search.with_row_count(name='temp_order')

    df_search_filtered = df_search.filter(
        (~pl.col('search_term').str.contains("xxxx")) &
        (~pl.col('search_term').str.contains(r'\d')) &
        (pl.col('search_term').str.len_chars() > 2) &
        (pl.col('search_term').str.contains(r'[a-zA-Z]'))
    )

    sort_columns = ['unique_session_id', 'timestamp', 'search_term'] \
        if 'timestamp' in df_search_filtered.columns else [
        'unique_session_id', 'temp_order', 'search_term']
    df_search_filtered = df_search_filtered.sort(sort_columns)

    df_search_filtered = df_search_filtered.with_columns([
        pl.col('search_term').shift(-1).over('unique_session_id')
        .alias('next_term')
    ])

    df_search_filtered = df_search_filtered.filter(
        (pl.col('next_term').is_null()) |
        (~pl.col('next_term').str.starts_with(pl.col('search_term'))) |
        (pl.col('next_term').str.len_chars() - pl.col('search_term')
         .str.len_chars() <= 2)
    )
    df_search_filtered = df_search_filtered.with_columns([
        pl.col('search_term').str.split(',').alias('split_terms')
    ]).explode('split_terms')

    df_search_filtered = df_search_filtered.with_columns([
        pl.col('split_terms').str.strip_chars().str.to_lowercase()
    ]).filter(
        pl.col('split_terms').str.len_chars() > 0
    )
    df_search_filtered = df_search_filtered.unique(
        subset=['unique_session_id', 'split_terms'])

    all_search_terms = df_search_filtered['split_terms']

    word_counts = Counter(all_search_terms)

    word_counts_df = pl.DataFrame({
        'text': list(word_counts.keys()),
        'value': list(word_counts.values())
    })

    word_counts_df_sorted = word_counts_df.sort('value', descending=True)
    top_5_words = word_counts_df_sorted.head(5)

    print("Top 5 Words:")
    print(top_5_words)
    
    return top_5_words
