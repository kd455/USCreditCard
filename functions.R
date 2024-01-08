library(tidyverse)
#using the https://tidyverts.org/ world
library(fable)
library(feasts)
library(tsibble)
library(glue)
library(arrow)
library(dplyr)
library(tidyr)
library(readr)
library(memoise)
library(ggrepel)
library(ggtext)
#library(prophet)
library(broom)
library(tseries)

#set seed for reproducibility
set.seed(42)

get_features <- function(data, ftags, value_name = "Value") {
    stat_features <- data |>
                      features(!!as.name(value_name), feature_set(tags = ftags))
    
    #find columns that are zero or constant
    zero_const <- stat_features |> 
                    select(-key_vars(data)) |>
                    summarise_all(sd) |>   #calcuate standard deviation of each column
                    select_if(~.x == 0 | is.na(.)) |> names() #select columns that are zero or NA
      
    stat_features |>
        select(!all_of(zero_const))
        #  select(-key_vars(data)) |> 
        # prcomp(scale = TRUE) |>
        # broom::augment(data)
}


matrix.to.tstibble <- function(matrix, names_to = "Path", values_to = "Value") {
  matrix |> 
    as.data.frame() |>
    mutate(Period = row_number()) |>
    pivot_longer(cols = -Period, names_to = names_to, values_to = values_to) |>
    mutate(Path = as.factor(Path)) |>
    as_tsibble(key = Path, index = Period) 
}

.read_ubpr <- function(file) {
  arrow::read_parquet(file) |>
    rename(BankName = "Financial Institution Name", Value = Numeric_Value) |>
    mutate(BankName = paste0(BankName, " (", IDRSSD, ")"),
           Quarter = yearquarter(ReportingPeriod)) |>
    select(-ReportingPeriod) |>
    as_tsibble(key = c(BankName,Measure), index = Quarter) 
}

summary_ratios <- memoise(function() {
  .read_ubpr("data/UBPR_Ratios_V.parquet") 
})

credit_card <- memoise(function(apply_filters = FALSE) {
  if(apply_filters) {
    .read_ubpr("data/UBPR_CreditCard_V.parquet") |>
      filter(BankType %in% c("LargeBank","LargeCreditCardBank")) |>
      tsibble::group_by_key() |>
      filter(Value != 0) |>
      slice(3:n()) |>
      fill_gaps()  |> 
      filter_index(get_regulation_cutoff() ~ .)
  } else {
    .read_ubpr("data/UBPR_CreditCard_V.parquet") 
  }
})

get_regulation_cutoff <- function() {
  "2010 Q2"
}

.measure_to_tsibble <- function(measure_data, measure_name) {
  measure_data |>
  filter(Measure == measure_name) |>
  as_tsibble(index = Quarter, key = (-c(Quarter,Value))) |>
    group_by_key() |>
    mutate(value_diff = difference(Value)) |>
    ungroup() |>
    fill_gaps()
}

credit_card.data_types_ref <- function() {
  tibble(
    mtype = c("M", "P"),
    data = list(
      tibble(Measure = c("UBPR3815", "UBPRB538", "UBPRD659")),
      tibble(Measure = c("UBPRE524", "UBPRE263", "UBPRE425"))
    )) |> unnest(data)
}
#' Credit Card Plans-30-89 DAYS P/D %
#' 
#' Credit card loans that are 30-89 days past due divided by total credit card loans.
#' 
credit_card.overdue_3089 <- memoise(function(apply_filters = FALSE) {
  credit_card(apply_filters) |>
    .measure_to_tsibble("UBPRE524")
})

#'Unused Commitments on Credit Cards
#' 
#' The unused portions of all commitments to extend credit both to individuals for household, family, and other personal 
#' expenditures and to other customers, including commercial or industrial enterprises, through credit cards.
#' 
credit_card.unused <- function(apply_filters = FALSE) {
  credit_card(apply_filters) |>
    .measure_to_tsibble("UBPR3815")
}

#'Unused Commitments on Credit Cards as a percent of Total Assets
#' 
#' The unused portions of all commitments to extend credit both to individuals for household, family, and other personal
#' expenditures and to other customers, including commercial or industrial enterprises, through credit cards divided by total
#' assets.
#' 
credit_card.unused_ratio <- function(apply_filters = FALSE) {
  credit_card(apply_filters) |>
    .measure_to_tsibble("UBPRE263")
}

#' Total credit card loans
#' 
#' Loans to Individuals for Household, Family, and Other Personal Expenditures 
#' (I.E., Consumer Loans)(Includes Purchased Paper): Credit Cards
#' 
credit_card.loan_amount <- function(apply_filters = FALSE) {
  credit_card(apply_filters) |>
    .measure_to_tsibble("UBPRB538") 
}

credit_card.loan_proportion <- function(apply_filters = FALSE) {
  credit_card(apply_filters) |>
    .measure_to_tsibble("UBPRE425")
}


#' Average Total Assets
#'
#' A year-to-date average of the average assets reported in the Call Report Schedule RC-K. 
#' Thus for the first quarter of the year the average assets from Call Report Schedule RC-K quarter-1 will appear, 
#' while at the end of-year, assets for all four quarters would be averaged.
#'
#' @return A tsibble with the average total assets for each quarter.
summary_ratios.assets <- function() {
  # if (file.exists("data/UBPR_Calc_Ratios.parquet")) {
  #   calculated_measures() |> filter(Measure == "UBPRD659")
  # } else {
    summary_ratios() |> filter(Measure == "UBPRD659") |>
      .measure_to_tsibble("UBPRD659")
  # }
}

# credit_card.loan_amount_ratio_calc <- function() {
#   calculated_measures() |>
#     filter(Measure == "UBPRB538C")
# }

# credit_card.unused_ratio_calc <- function() {
#   calculated_measures() |>
#     filter(Measure == "UBPR3815C")
# }
# credit_card.unused_proportion <- function() {
#   calculated_measures() |>
#     filter(Measure == "UBPR3815CP")
# }

# calculate_new_measures <- function() {
#   avg_assets <- summary_ratios.assets()
#   cc_unused_r <- as_percent_avg_assets(credit_card.unused(), avg_assets)
#   cc_loans_r <- as_percent_avg_assets(credit_card.loan_amount(), avg_assets)
#   cc_loans_p <- as_percent_loans(credit_card.unused(),credit_card.loan_amount())

#   dplyr::bind_rows(avg_assets, cc_unused_r,cc_loans_r,cc_loans_p) |>
#     arrow::write_parquet("data/UBPR_Calc_Ratios.parquet")
# }

# calculated_measures <- memoise(function() {
#   arrow::read_parquet("data/UBPR_Calc_Ratios.parquet")
# })

# as_percent_loans <- function(numerator_df, loans_df) {
#   numerator_df |> add_column(calc_value = round((numerator_df$Value/loans_df$Value)*100,2)) |>
#               mutate(Value = calc_value,
#                       value_diff = difference(Value),
#                       Measure = paste0(Measure,"CP"),
#                       Label = paste0(Label,"_CP"),
#                       Description = paste(Description,", % Credit Card Loans")) |> 
#                       select(-calc_value)
# }

# as_percent_avg_assets <- function(numerator_df, assets_df) {  
#   numerator_df |> add_column(calc_value = round((numerator_df$Value/assets_df$Value)*100,2)) |>
#                 mutate(Value = calc_value,
#                         value_diff = difference(Value),
#                         Measure = paste0(Measure,"C"),
#                         Label = paste0(Label,"_CALC"),
#                         Description = paste(Description,", % Avg Assets")) |> 
#                         select(-calc_value)
# }

us_economy <- memoise(function(freq = "M") {
  read_csv(glue("data/US_Economic_Data_{freq}.csv"), show_col_types = FALSE) |>
    mutate(Month = yearmonth(Date)) |>
    select(-Date) |> na.omit() |> 
    as_tsibble(index = Month) |>
    fill_gaps()
})

us_economy.recession <- memoise(function() {
  read_csv("data/JHDUSRGDPBR_M.csv", col_names = c("Month", "Flag"), show_col_types = FALSE) |>
    na.omit() |>
    as_tsibble(index = Month) |>
    fill_gaps()
})

us_economy.psavert <- function() {
  us_economy() |>
    select(PSAVERT) |>
      na.omit()
}

us_economy.confidence <- function() {
  us_economy() |>
    select(CSCICP03USM665S) |>
    na.omit()
}

# over_30_89_days <- function(threshold_pct = 25) {
#   #select firms that have non-zero values for UBPRE524
#   all_measures <- credit.tsibble()
#   select_firms <-  all_measures |>
#     filter(Measure == "UBPRE524") |>
#     group_by(BankName) |>
#     summarise(Avg = mean(Value)) |>
#     filter(Avg != 0) |> 
#     distinct(BankName)  
  
#   #select firms that have % of their loans in credit cards
#   select_firms <- all_measures |>
#     filter(Measure == "UBPRE425", BankName %in% select_firms$BankName) |>
#     group_by(BankName) |>
#     filter(Value >= threshold_pct) |>
#     distinct(BankName) 
  
#   #return 30-89 overdue  
#   all_measures |>
#     filter(Measure == "UBPRE524", BankName %in% select_firms$BankName)
# }

white.noise.sample <- function() {
  tsibble(Period = 1:100, Value = rnorm(100), index = Period)
}

white.noise.plot <- function() {
  white.noise.sample() |> 
    ggplot(aes(x = Period, y = Value)) +
    geom_line(linewidth = .2) +
    labs(title="White noise")
  
}

credit_card.partnerships <- function() {
  tibble::tibble_row(name = "Cosco",
                old="AMERICAN EXPRESS NATIONAL BANK (1394676)", 
                new="CITIBANK, N.A. (476810)",
                acquired = "2016-04-01",
                available = "2016-06-20") |>
                tibble::add_row(name = "Walmart",
                old="SYNCHRONY BANK (1216022)",
                new="CAPITAL ONE, NATIONAL ASSOCIATION (112837)",
                acquired = "2019-07-01",
                available = "2019-09-24")|>
                tibble::add_row(name = "GAP",
                old="SYNCHRONY BANK (1216022)", 
                new="BARCLAYS BANK DELAWARE (2980209)",
                acquired = "2022-05-01",
                available = "2022-06-20")
}

credit_card.target_label <- function() {
  "Credit Card Plans-30-89 DAYS P/D %"
}

bank_type.desc <- function() {
  tibble(BankType = 'LargeBank', PeerId = '1', Criteria = 'Insured commercial banks having assets greater than $100 billion', Examples = 'CITIBANK, JPMORGAN CHASE BANK, MORGAN STANLEY') |> 
    add_row(BankType = 'LargeCreditCardBank', PeerId = '201', Criteria = 'Large Credit Card Banks: Credit card specialty banks having assets greater than $ 3 billion', Examples = 'BARCLAYS BANK, SYNCHRONY BANK, DISCOVER BANK')|> 
    add_row(BankType = 'OtherBank', PeerId = '', Criteria = 'Other - all other Banks that have reported UBPRE524', Examples = '')
}

bank_type.colours <- function() {
  c(LargeBank = "#1B9E77", 
                    "LargeBank/Before" = "#1B9E77", 
                    "LargeBank/After" = "#1B9E77", 
                    LargeCreditCardBank = "#D95F02",
                    "LargeCreditCardBank/Before" = "#D95F02", 
                    "LargeCreditCardBank/After" =  "#D95F02", 
                    OtherBank = "#999999",
                    "OtherBank/Before" = "#999999",
                    "OtherBank/After" = "#999999")
} 

feature_data <- function(data, feature_names) {
  data |> fabletools::features(Value, feature_set(tags = feature_names))
}

feature_prcomp <- function(f_data) {
  #identify banks where we could not generate feature data
  #ids <- f_data |> filter_all(any_vars(is.na(.))) |> pull(IDRSSD)
  data <- f_data |> na.omit()
  
  data|>
    na.omit() |> 
    select(-c(IDRSSD,BankName, Measure, Label, Description, BankType)) |>
    prcomp(scale = TRUE) |> 
    broom::augment(data)
}


pcs.plot <- function(pcs) {
  pcs |>
        ggplot(aes(x = .fittedPC1, y = .fittedPC2, colour = BankType)) +
        geom_point(size = 5) +
        theme(aspect.ratio = 1,plot.title = element_markdown()) +
        labs(subtitle = "US Credit Cards 30-89 days",
            x = "PC1",
            y = "PC2")+ theme(legend.position="none")   + 
        scale_color_manual(values = bank_type.colours())
}

features.plot_violin <-function(data, feature_name) {
  data |>
    ggplot(aes(x = BankType, y = !!as.name(feature_name))) +
                geom_violin(trim = FALSE) +
                geom_boxplot(width = 0.1, fill = "white") + 
                theme_light() +
                labs(x = "", y = "")
}

outlier.plot <- function(data, pca_output, P1_lower, P1_upper, P2_lower, P2_upper, value_name = "Value") {

  outliers <- pca_output |>
                filter( (.fittedPC2 < P2_lower) | 
                        (.fittedPC2 > P2_upper) | 
                        (.fittedPC1 < P1_lower) |
                        (.fittedPC1 > P1_upper)) |>
                select(IDRSSD) 

  if (!purrr::is_empty(outliers))  {
    data |>
      filter(IDRSSD %in% outliers$IDRSSD) |>
      ggplot(aes(x = Quarter, y = !!as.name(value_name), col = BankName)) +
      geom_line() +
      facet_wrap(~BankName, ncol = 1) +
      theme(legend.position = "none")  +
      labs(subtitle = glue("Outliers: {P1_lower} < PC1 < {P1_upper}, {P2_lower} < PC2 < {P2_upper}"),
           y = paste0(credit_card.target_label(), "\n", value_name))
  }
}

get_stl <- function(data, value_name = "Value") {
  get_median_trend(data, value_name) |> 
    na.omit() |>
    model(stl = STL(!!as.name(value_name))) |> 
    components()
}

get_median_trend <- function(data, value_name = "Value") {
  data |> 
    group_by(Measure, Description)  |>
    summarise(!!quo_name(value_name) := median(!!as.name(value_name), na.rm = TRUE)) 
}

get_mean_trend <- function(data, value_name = "Value") {
  data |> 
    group_by(Measure, Description)  |>
    summarise(!!quo_name(value_name) := mean(!!as.name(value_name), na.rm = TRUE))
}

user_friendly_label <- function(measure, value_name) {
    mtype <- credit_card.data_types_ref() |> filter(Measure == measure) |> pull(mtype)
    case_when(
              value_name == "value_diff" & mtype == "M" ~ "difference from prior period ($)",
              value_name == "value_diff" & mtype == "P" ~ "difference from prior period (%)",
              value_name == "pct_change" ~ "% change from prior period",
              value_name == "Value" & mtype == "M" ~ "Value ($)",
              value_name == "Value" & mtype == "P" ~ "Value (%)",
              .default = value_name)
}

partnership.plot <- function(name, old, new, acquired, available, selected_measures,value_name, trend_calc = "median", date_obs_period = 2) {
  date_acquired <- as.Date(acquired)
  date_available <- as.Date(available)
  from_date <- year(date_acquired) -date_obs_period
  to_date <- year(date_acquired) +date_obs_period

  group.colours <- c(old = "#1B9E77", new = "#D95F02", aggregate = "#999999")
  trend_data <- NULL
  if (trend_calc == "stl") 
      trend_data <- get_stl(selected_measures, value_name) |> mutate(!!quo_name(value_name) := trend)
  if (trend_calc == "median")
      trend_data <- get_median_trend(selected_measures, value_name)
  if (trend_calc == "mean")
      trend_data <- get_mean_trend(selected_measures, value_name) 

  if (!is_empty(trend_data))
    trend_data <- trend_data |> add_column(BankName = "aggregate") |> na.omit()

  measures <- selected_measures |> as_tibble() |> distinct(Measure)
  measures_label <- paste0(pull(measures), collapse="_")
  y_label <- measures$Measure |> map_chr(user_friendly_label,value_name) |> unique()

  selected_measures |>
    filter(BankName %in% c(old, new)) |> 
    bind_rows(trend_data) |> 
    mutate(group_column = case_when(
                              BankName == old ~ "old",
                              BankName == new ~ "new")) |> 
    filter_index(as.character(from_date) ~ as.character(to_date)) |>
    autoplot(!!as.name(value_name), aes(color = group_column)) +
    scale_color_manual(values = group.colours) +
    facet_wrap(~ Description, scales = "free", ncol=1) +
    labs(title=glue("{name} Partnership: <span style='color:#D95F02'>{old}</span> to 
                    <span style='color:#1B9E77'>{new}</span>
                    against <span style='color:#333333'>{trend_calc} PEER Banks</span>"),
        subtitle = glue("Acquired {date_acquired}. Card available {date_available}"),
        y = y_label,
        x = "Year")  +
    theme(legend.position = "none",
          plot.title = element_markdown(),
          strip.text = element_text(size = 12))  +
    scale_y_continuous(labels = scales::comma) +
    scale_x_yearmonth(date_breaks = "1 year", date_labels = "%Y")   +
    geom_vline(xintercept = as.numeric(date_acquired), linetype=1,colour="#00259e") +
    annotate("text", x=as.numeric(date_acquired-10), y=0, label="acquired", angle=90, hjust = 0)+
    geom_vline(xintercept = as.numeric(date_available), linetype=4) +
    annotate("text", x=as.numeric(date_available-10), y=0, label="available", angle=90, hjust = 0) 
    #ggsave(glue("images/{name}_{measures_label}_{value_name}_{trend_calc}_partnership.png"), width = 16, height = nrow(measures)*4, dpi = 300)
}

