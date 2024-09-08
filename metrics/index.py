"""
Este módulo ejecuta todas las métricas utilizando un DataFrame de logs.
"""

from metrics.general.average_response_time import calculate_average_response_time
from metrics.general.average_time_spent_on_site import (
    calculate_average_time_spent_on_site,
)
from metrics.general.downloading_hits_ratio import downloadable_resources_hits_ratio
from metrics.general.error_rate_success_rate import calculate_error_rate_success_rate
from metrics.maps.maximum_stable_value_zoom import (
    calculate_maximum_stable_value_zoom,
)
from metrics.nav.most_visited_pages import calculate_nav_most_visited_pages
from metrics.user.average_stepback_actions import calculate_average_stepback_actions
from metrics.users.average_pages_viewed_per_visitor import (
    calculate_average_pages_viewed_per_session,
)
from metrics.users.ratio_of_new_visitors_to_all_visitors import (
    calculate_ratio_of_new_visitors_to_all_visitors,
)
from metrics.users.user_categorization.index import classify_user_profiles


def run_all_metrics(logs_df):
    """
    Ejecuta todas las métricas usando el dataframe de logs proporcionado.
    """
    print("Ejecutando todas las métricas...")

    print("Métricas generales:")
    calculate_error_rate_success_rate(logs_df)
    calculate_average_time_spent_on_site(logs_df)
    calculate_average_response_time(logs_df)
    downloadable_resources_hits_ratio(logs_df)

    print("Métricas de tiempo:")
    calculate_maximum_stable_value_zoom(logs_df)

    print("Métricas de navegación:")
    calculate_nav_most_visited_pages(logs_df)

    print("Métricas de usuarios:")
    calculate_average_pages_viewed_per_session(logs_df)
    calculate_ratio_of_new_visitors_to_all_visitors(logs_df)
    calculate_average_stepback_actions(logs_df)
    classify_user_profiles(logs_df)

    print("Todas las métricas han sido ejecutadas.")
