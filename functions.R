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
library(prophet)
library(broom)

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

credit_card <- memoise(function() {
  .read_ubpr("data/UBPR_CreditCard_V.parquet") 
})

.measure_to_tsibble <- function(measure_data, measure_name) {
  measure_data |>
  filter(Measure == measure_name) |>
  as_tsibble(index = Quarter, key = (-c(Quarter,Value))) |>
    group_by_key() |>
    mutate(value_scaled = scale(Value)) |>
    mutate(value_diff = difference(Value)) |>
    ungroup() |>
    fill_gaps()
}

#' Credit Card Plans-30-89 DAYS P/D %
#' 
#' Credit card loans that are 30-89 days past due divided by total credit card loans.
#' 
credit_card.overdue_3089 <- memoise(function() {
  credit_card() |>
    .measure_to_tsibble("UBPRE524")
})

#'Unused Commitments on Credit Cards
#' 
#' The unused portions of all commitments to extend credit both to individuals for household, family, and other personal 
#' expenditures and to other customers, including commercial or industrial enterprises, through credit cards.
#' 
credit_card.unused <- function() {
  credit_card() |>
    .measure_to_tsibble("UBPR3815")
}

#'Unused Commitments on Credit Cards as a percent of Total Assets
#' 
#' The unused portions of all commitments to extend credit both to individuals for household, family, and other personal
#' expenditures and to other customers, including commercial or industrial enterprises, through credit cards divided by total
#' assets.
#' 
credit_card.unused_ratio <- function() {
  credit_card() |>
    .measure_to_tsibble("UBPRE263")
}

#' Total credit card loans
#' 
#' Loans to Individuals for Household, Family, and Other Personal Expenditures 
#' (I.E., Consumer Loans)(Includes Purchased Paper): Credit Cards
#' 
credit_card.loan_amount <- function() {
  credit_card() |>
    .measure_to_tsibble("UBPRB538")
}

#' Average Total Assets
#'
#' A year-to-date average of the average assets reported in the Call Report Schedule RC-K. 
#' Thus for the first quarter of the year the average assets from Call Report Schedule RC-K quarter-1 will appear, 
#' while at the end of-year, assets for all four quarters would be averaged.
#'
#' @return A tsibble with the average total assets for each quarter.
summary_ratios.assets <- function() {
  summary_ratios() |>
    .measure_to_tsibble("UBPRD659")
}


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

over_30_89_days <- function(threshold_pct = 25) {
  #select firms that have non-zero values for UBPRE524
  all_measures <- credit.tsibble()
  select_firms <-  all_measures |>
    filter(Measure == "UBPRE524") |>
    group_by(BankName) |>
    summarise(Avg = mean(Value)) |>
    filter(Avg != 0) |> 
    distinct(BankName)  
  
  #select firms that have % of their loans in credit cards
  select_firms <- all_measures |>
    filter(Measure == "UBPRE425", BankName %in% select_firms$BankName) |>
    group_by(BankName) |>
    filter(Value >= threshold_pct) |>
    distinct(BankName) 
  
  #return 30-89 overdue  
  all_measures |>
    filter(Measure == "UBPRE524", BankName %in% select_firms$BankName)
}

white.noise.sample <- function() {
  tsibble(Period = 1:100, Value = rnorm(100), index = Period)
}

white.noise.plot <- function() {
  white.noise.sample() |> 
    ggplot(aes(x = Period, y = Value)) +
    geom_line(size = .2) +
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

partnership.plot <- function(name, old, new, acquired, available, selected_measures,value_name, date_obs_period = 2) {
  date_acquired <- as.Date(acquired)
  date_available <- as.Date(available)
  from_date <- year(date_acquired) -date_obs_period
  to_date <- year(date_acquired) +date_obs_period
  group.colours <- c(old = "#1B9E77", new = "#D95F02", aggregate = "#999999")
  agg_data <- selected_measures |>
              filter(BankType != "OtherBank") |>
              select(Measure, !!as.name(value_name)) |>
              group_by(Measure, Description) |>
                summarise(
                  !!quo_name(value_name) := mean(!!as.name(value_name), na.rm = TRUE)) |> 
                tibble::add_column(BankName = "aggregate")
  measures <- paste0(selected_measures |> as_tibble() |> distinct(Measure) |> pull(Measure), collapse="_")

  selected_measures |>
    filter(BankName %in% c(old, new)) |>
    bind_rows(agg_data) |> 
    mutate(group_column = case_when(
                              BankName == old ~ "old",
                              BankName == new ~ "new")) |> 
    filter_index(as.character(from_date) ~ as.character(to_date)) |> 
    autoplot(!!as.name(value_name), aes(color = group_column)) +
    scale_color_manual(values = group.colours)+
    facet_wrap(~ Description, scales = "free", ncol=1) +
    labs(title=glue("{name} Partnership: <span style='color:#D95F02'>{old}</span> to 
                    <span style='color:#1B9E77'>{new}</span>
                    against <span style='color:#333333'>PEER Banks</span>"),
        subtitle = glue("Acquired {date_acquired}. Card available {date_available}"),
        y = value_name,
        x = "Year")  +
    theme(legend.position = "none",
          plot.title = element_markdown(),
          strip.text = element_text(size = 12))  +
    scale_x_yearmonth(date_breaks = "1 year", date_labels = "%Y")   +
    geom_vline(xintercept = as.numeric(date_acquired), linetype=1,colour="#00259e") +
    annotate("text", x=as.numeric(date_acquired-10), y=0, label="acquired", angle=90, hjust = 0)+
    geom_vline(xintercept = as.numeric(date_available), linetype=4) +
    annotate("text", x=as.numeric(date_available-10), y=0, label="available", angle=90, hjust = 0) 
    ggsave(glue("{name}_{measures}_partnership.png"), width = 16, height = 12, dpi = 300)
}

