from collections import Counter
import polars as pl


def calculate_most_repeated_words_filtered(logs_df):
    """
    Calculate the most repeated words in a given DataFrame of logs.

    The method filters log entries to focus on search queries, cleans and
    processes the terms,removes autocompleted terms, splits search terms
    by commas, and finally counts the occurrences
    of each unique term.
    """
    stopwords = ["de", "la", "y", "el", "o", "a", "en", "por", "con", "que", "los", "las", "del", "al"]

    df_search = logs_df.filter(pl.col('request_url').str.contains(r'q='))

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

    df_search_filtered = df_search_filtered.with_columns([
        (
            (
                    (pl.col('unique_session_id') != pl.col('unique_session_id').shift(1)) |
                    (pl.col('next_term').is_null()) |
                    (~pl.col('next_term').str.starts_with(pl.col('search_term'))) |
                    (pl.col('search_term').str.slice(0, 5) != pl.col('next_term').str.slice(0, 5))
            ).cum_sum().fill_null(0)
        ).alias('search_group')
    ])

    df_search_filtered = df_search_filtered.with_columns([
        pl.col('search_term').str.len_chars().alias('search_term_length')
    ])

    df_search_filtered = df_search_filtered.with_columns([
        pl.col('search_term').str.strip_chars().str.to_lowercase().alias('cleaned_search_term')
    ])

    df_search_filtered = df_search_filtered.unique(
        subset=['unique_session_id', 'cleaned_search_term'])

    df_search_filtered = df_search_filtered.with_columns(
        df_search_filtered["cleaned_search_term"].str.split(by=" ").alias("splitted_search_terms")
    )
    all_search_terms = df_search_filtered["splitted_search_terms"].to_list()
    flattened_search_terms = [word for sublist in all_search_terms for word in sublist]
    filtered_search_terms = [word for word in flattened_search_terms if word.lower() not in stopwords]

    word_counts = Counter(filtered_search_terms)

    word_counts_df = pl.DataFrame({
        'text': list(word_counts.keys()),
        'value': list(word_counts.values())
    })

    word_counts_df_sorted = word_counts_df.sort('value', descending=True)
    top_words = word_counts_df_sorted.head(20)

    print("Top 20 Words:")
    print(top_words)

    return top_words
