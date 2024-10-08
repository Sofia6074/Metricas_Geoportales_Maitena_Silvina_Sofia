"""
This module runs all metrics using a DataFrame of logs.
"""

from metrics.general.average_response_time import (
    calculate_average_response_time
)
from metrics.general.average_time_spent_on_site import (
    calculate_average_time_spent_on_site
)
from metrics.general.average_time_spent_per_page import (
    calculate_average_time_spent_per_page
)
from metrics.general.count_device_usage import count_device_usage
from metrics.general.downloading_hits_ratio import downloadable_resources_hits
from metrics.general.error_rate_success_rate import (
    calculate_error_rate_success_rate
)
from metrics.general.most_visited_pages import calculate_most_visited_pages
from metrics.general.stick_and_slip_pages import define_stick_and_slip_pages
from metrics.maps.average_zoom_response_time import (
    calculate_average_response_time_during_zoom
)
from metrics.maps.maximum_stable_value_zoom import (
    calculate_maximum_stable_value_zoom)
from metrics.maps.maximum_zoom_value import calculate_maximum_zoom
from metrics.search.most_repeated_words_in_consecutive_searches import (
    calculate_most_repeated_words_filtered)
from metrics.search.related_search_parameter_consecutive import (
    calculate_related_search_parameters)
from metrics.users.average_pages_viewed_per_visitor import (
    calculate_average_pages_viewed_per_session)
from metrics.users.average_stepback_actions import (
    calculate_average_stepback_actions)
from metrics.users.ratio_of_new_visitors_to_all_visitors import (
    calculate_ratio_of_new_visitors_to_all_visitors
)
from metrics.users.user_categorization.index import (
    classify_user_profiles
)
from Utils.create_json import create_json


def run_all_metrics(logs_df):
    """
    Runs all metrics using the provided logs DataFrame.
    """
    results = {}
    print("Running all metrics...")


    print("General metrics:")
    results['error_rate_success_rate'] = calculate_error_rate_success_rate(logs_df)
    results['average_time_spent_on_site'] = calculate_average_time_spent_on_site(logs_df)
    results['average_time_spent_per_page'] = calculate_average_time_spent_per_page(logs_df)
    results['average_response_time'] = calculate_average_response_time(logs_df)
    results['device_usage'] = count_device_usage(logs_df)
    results['downloadable_resources_hits'] = downloadable_resources_hits(logs_df)
    results['stick_and_slip_pages'] = define_stick_and_slip_pages(logs_df)
    results['count_device_usage'] = count_device_usage(logs_df)
    results['most_visited_pages'] = calculate_most_visited_pages(logs_df)

    print("Time-related metrics:")
    results['maximum_zoom'] = calculate_maximum_zoom(logs_df)
    results['maximum_stable_zoom'] = calculate_maximum_stable_value_zoom(logs_df)
    results['zoom_response_time'] = calculate_average_response_time_during_zoom(logs_df)

    print("Search metrics:")
    results['most_repeated_words'] = calculate_most_repeated_words_filtered(logs_df)

    print("Search metrics:")
    results['related_search_parameters'] = calculate_related_search_parameters(logs_df)

    print("User metrics:")
    results['average_pages_viewed'] = calculate_average_pages_viewed_per_session(logs_df)
    results['new_visitors_vs_all_visitors'] = (
        calculate_ratio_of_new_visitors_to_all_visitors(logs_df))
    results['average_stepbacks'] = calculate_average_stepback_actions(logs_df)
    results['user_profiles'] = classify_user_profiles(logs_df)

    print("All metrics have been executed.")
    create_json(results)
