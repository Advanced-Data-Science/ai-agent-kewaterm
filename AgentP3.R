
library(httr)
library(jsonlite)
library(dplyr)
library(lubridate)

# Load config 
config <- list(
    years = 2017:2020,
    grades = c(3, 8),
    states = c(6, 36, 48),
    base_delay = 1.0
)

# Initialize variables 
data_store <- list()
collection_stats <- list(
    total_requests = 0,
    successful_requests = 0,
    failed_requests = 0
)

delay_multiplier <- 1.0

# Helper Functions 

make_api_request <- function(year, grade, fips) {
    url <- paste0("https://educationdata.urban.org/api/v1/schools/ccd/enrollment/", 
                  year, "/grade-", grade, "/?fips=", fips)
    collection_stats$total_requests <<- collection_stats$total_requests + 1
    
    tryCatch({
        res <- GET(url)
        if (status_code(res) == 200) {
            collection_stats$successful_requests <<- collection_stats$successful_requests + 1
            return(fromJSON(content(res, "text"))$results)
        } else {
            collection_stats$failed_requests <<- collection_stats$failed_requests + 1
            return(NULL)
        }
    }, error = function(e) {
        collection_stats$failed_requests <<- collection_stats$failed_requests + 1
        return(NULL)
    })
}

respectful_delay <- function() {
    delay <- config$base_delay * delay_multiplier * runif(1, 0.5, 1.5)
    Sys.sleep(delay)
}

assess_data_quality <- function(data) {
    complete_rows <- sum(complete.cases(data))
    round(complete_rows / nrow(data), 2)
}

adjust_strategy <- function(success_rate) {
    if (success_rate < 0.5) {
        delay_multiplier <<- delay_multiplier * 2
    } else if (success_rate > 0.9) {
        delay_multiplier <<- delay_multiplier * 0.8
    }
}

# Main Collection Loop 
for (year in config$years) {
    for (grade in config$grades) {
        for (state in config$states) {
            cat("Fetching year:", year, "grade:", grade, "state (FIPS):", state, "\n")
            results <- make_api_request(year, grade, state)
            if (!is.null(results) && length(results) > 0) {
                data_store[[length(data_store) + 1]] <- results
            }
            success_rate <- collection_stats$successful_requests / collection_stats$total_requests
            if (success_rate < 0.8) {
                adjust_strategy(success_rate)
            }
            respectful_delay()
        }
    }
}

# Combine data
edu_data <- bind_rows(data_store)
dir.create("data/raw", recursive = TRUE, showWarnings = FALSE)
write_json(edu_data, "data/raw/education_data.json", pretty = TRUE)

# Metadata 
generate_metadata <- function(data) {
    meta <- list(
        collection_date = as.character(Sys.time()),
        agent_version = "R-1.0",
        collector = "Katy Waterman",
        total_records = nrow(data),
        variables = names(data)
    )
    dir.create("data/metadata", recursive = TRUE, showWarnings = FALSE)
    write_json(meta, "data/metadata/metadata.json", pretty = TRUE)
    return(meta)
}

metadata <- generate_metadata(edu_data)

# Quality Report 
generate_quality_report <- function(data) {
    completeness <- colMeans(!is.na(data))
    report <- list(
        summary = list(
            total_records = nrow(data),
            collection_success_rate = collection_stats$successful_requests / collection_stats$total_requests,
            overall_quality_score = assess_data_quality(data)
        ),
        completeness = completeness
    )
    dir.create("reports", showWarnings = FALSE)
    write_json(report, "reports/quality_report.json", pretty = TRUE)
    return(report)
}

quality_report <- generate_quality_report(edu_data)

# Final Summary
cat("\nCollection Summary:\n")
cat("Total records:", nrow(edu_data), "\n")
cat("Success rate:", round(collection_stats$successful_requests / collection_stats$total_requests, 2), "\n")
cat("Quality score:", quality_report$summary$overall_quality_score, "\n")

