export interface JsonData {
    error_rate_success_rate: ErrorRateSuccessRate
    average_time_spent_on_site: number
    average_time_spent_per_page: number
    average_response_time: AverageResponseTime
    device_usage: DeviceUsage[]
    downloadable_resources_hits_ratio: number
    stick_and_slip_pages: StickAndSlipPages
    maximum_zoom: number
    maximum_stable_zoom: MaximumStableZoom[]
    zoom_response_time: number
    most_repeated_words: MostRepeatedWord[]
    related_search_parameters: RelatedSearchParameter[]
    average_pages_viewed: number
    new_visitors_vs_all_visitors: number
    avrg_stepbacks: number
    user_profiles: UserProfile[]
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
    slip: number[]
    stick: number[]
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

export interface UserProfile {
    user_profile: number
    count: number
    percentage: number
}
