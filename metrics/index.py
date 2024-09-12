"""
This module runs all metrics using a DataFrame of logs.
"""

from metrics.general.average_response_time import (
    calculate_average_response_time)
from metrics.general.average_time_spent_on_site import (
    calculate_average_time_spent_on_site)
from metrics.general.average_time_spent_per_page import (
    calculate_average_time_spent_per_page)
from metrics.general.count_device_usage import count_device_usage
from metrics.general.error_rate_success_rate import (
    calculate_error_rate_success_rate)
from metrics.general.stick_and_slip_pages import define_stick_and_slip_pages
from metrics.maps.maximum_stable_value_zoom import (
    calculate_maximum_stable_value_zoom)
from metrics.nav.most_visited_pages import calculate_nav_most_visited_pages
from metrics.users.average_pages_viewed_per_visitor import (
    calculate_average_pages_viewed_per_session)
from metrics.users.average_stepback_actions import calculate_average_stepback_actions
from metrics.users.ratio_of_new_visitors_to_all_visitors import (
    calculate_ratio_of_new_visitors_to_all_visitors)
from metrics.users.user_categorization.index import classify_user_profiles


def run_all_metrics(logs_df):
    """
    Runs all metrics using the provided logs DataFrame.
    """
    print("Running all metrics...")

    print("General metrics:")
    calculate_error_rate_success_rate(logs_df)
    calculate_average_time_spent_per_page(logs_df)
    calculate_average_response_time(logs_df)
    calculate_average_time_spent_on_site(logs_df)
    count_device_usage(logs_df)
    define_stick_and_slip_pages(logs_df)

    print("Time-related metrics:")
    calculate_maximum_stable_value_zoom(logs_df)

    print("Navigation metrics:")
    calculate_nav_most_visited_pages(logs_df)

    print("User metrics:")
    calculate_average_pages_viewed_per_session(logs_df)
    calculate_ratio_of_new_visitors_to_all_visitors(logs_df)
    calculate_average_stepback_actions(logs_df)
    classify_user_profiles(logs_df)

    print("All metrics have been executed.")
