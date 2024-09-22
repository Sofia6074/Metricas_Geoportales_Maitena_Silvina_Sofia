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
    entry_pages_df = logs_df.group_by('unique_session_id').agg([
        pl.col('request_url').first().alias('entry_page')
    ])
    total_entry_page_views = entry_pages_df.shape[0]

    single_access_df = logs_df.group_by('unique_session_id').agg([
        pl.col('request_url').n_unique().alias('unique_page_views')
    ])
    single_access_df = single_access_df.filter(pl.col('unique_page_views') == 1)
    total_single_access_page_views = single_access_df.shape[0]

    # Using the Slip and Stick formula
    slip = (total_single_access_page_views / total_entry_page_views
            if total_entry_page_views > 0 else 0)
    stick = 1 - slip

    metrics_df = pl.DataFrame({
        'total_entry_page_views': [total_entry_page_views],
        'total_single_access_page_views': [total_single_access_page_views],
        'slip': [slip],
        'stick': [stick]
    })

    print("Slip and Stick:")
    print(metrics_df)
