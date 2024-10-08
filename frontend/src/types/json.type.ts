export interface JsonData {
    error_rate_success_rate: ErrorRateSuccessRate
    average_time_spent_on_site: number
    average_time_spent_per_page: number
    average_response_time: AverageResponseTime
    device_usage: DeviceUsage[]
    downloadable_resources_hits: number
    stick_and_slip_pages: StickAndSlipPages
    count_device_usage: CountDeviceUsage[]
    most_visited_pages: MostVisitedPage[]
    maximum_zoom: number
    maximum_stable_zoom: MaximumStableZoom[]
    zoom_response_time: number
    most_repeated_words: MostRepeatedWord[]
    related_search_parameters: RelatedSearchParameter[]
    average_pages_viewed: number
    new_visitors_vs_all_visitors: number
    average_stepbacks: number
    user_profiles: UserProfiles
}

export interface ErrorRateSuccessRate {
    success_rate: number
    error_rate: number
}

export interface AverageResponseTime {
    avg_response_time: number
    max_response_time: number
    min_response_time: number
}

export interface DeviceUsage {
    device_type: string
    device_usage_count: number
}

export interface StickAndSlipPages {
    total_entry_page_views: number
    total_single_access_page_views: number
    slip: number
    stick: number
}

export interface CountDeviceUsage {
    device_type: string
    device_usage_count: number
}

export interface MostVisitedPage {
    base_url: string
    count: number
}

export interface MaximumStableZoom {
    zoom_level: number
    total_count: number
}

export interface MostRepeatedWord {
    word: string
    count: number
}

export interface RelatedSearchParameter {
    unique_session_id: string
    search_pair: string
    jaccard_similarity: number
}

export interface UserProfiles {
    user_profile_counts: UserProfileCount[]
    user_categorized_metrics: UserCategorizedMetrics
}

export interface UserProfileCount {
    user_profile: number
    count: number
    percentage: number
}

export interface UserCategorizedMetrics {
    average_time_spent_on_site_per_user_category: AverageTimeSpentOnSitePerUserCategory[]
    average_pages_viewed_per_session_per_user_category: AveragePagesViewedPerSessionPerUserCategory[]
    average_time_spent_per_page_per_user_category: AverageTimeSpentPerPagePerUserCategory[]
}

export interface AverageTimeSpentOnSitePerUserCategory {
    user_profile: number
    avg_time_spent_on_site: number
}

export interface AveragePagesViewedPerSessionPerUserCategory {
    user_profile: number
    avg_pages_viewed: number
}

export interface AverageTimeSpentPerPagePerUserCategory {
    user_profile: number
    avg_time_per_user: number
}
