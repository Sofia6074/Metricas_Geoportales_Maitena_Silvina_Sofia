"""
Calculates "slip" and "stick" metrics.
"""

import polars as pl

def define_stick_and_slip_pages(logs_df):
    """
    Calculates "slip" and "stick" metrics.

    Identifies entry pages as the first page of each session.
    Counts total entry page views and single access
    page views (sessions with only one page viewed).

    - Slip: ratio of single access views to entry views.
    - Stick: 1 - slip, indicating page retention ability.
    """

    # Umbral de tiempo en microsegundos (3 segundos = 3 * 1_000_000 microsegundos)
    threshold_duration_us = 3 * 1_000_000

    logs_df = logs_df.sort(['unique_session_id', 'timestamp'])
    logs_with_diff = logs_df.with_columns([
        (pl.col('timestamp') - pl.col('timestamp').shift(1)).alias('time_diff'),
        (pl.col('time_diff') > threshold_duration_us).fill_null(True).alias('new_entry')
    ])

    entry_pages_df = logs_with_diff.filter(pl.col('new_entry')).group_by('unique_session_id').agg([
        pl.col('request_url').first().alias('entry_page')
    ])
    total_entry_page_views = entry_pages_df.shape[0]

    single_page_sessions_df = logs_with_diff.filter(pl.col('new_entry')).group_by('unique_session_id').agg([
        pl.col('request_url').n_unique().alias('page_view_count')
    ])
    single_access_views = single_page_sessions_df.filter(pl.col('page_view_count') == 1).shape[0]

    slip = single_access_views / total_entry_page_views
    stick = 1 - slip

    print(f"Slip: {slip}")
    print(f"Stick: {stick}")

    return {
        'slip': slip,
        'stick': stick
    }
