library(tidyverse)
#using the https://tidyverts.org/ world
library(fable)
library(caret)
library(lme4)
library(feasts)
library(tsibble)
library(glue)
library(arrow)
library(tidyr)
library(readr)
library(memoise)
library(ggrepel)
library(broom)
library(tseries)
library(lmtest)
library(ggthemes)
library(Metrics)
library(ggtext)
library(lme4)
library(broom.mixed)
library(MuMIn)
library(merTools)
library(dplyr)
#set seed for reproducibility
set.seed(42)

scale_fill_colorblind7 = function(.ColorList = 2L:8L, ...){
    scale_fill_discrete(..., type = colorblind_pal()(8)[.ColorList])
}

scale_colour_colorblind7 = function(.ColorList = 2L:8L, ...){
    scale_color_discrete(..., type = colorblind_pal()(8)[.ColorList])
}

get_features <- function(data, ftags, value_name = "Value") {
    stat_features <- data |>
                      features(!!as.name(value_name), feature_set(tags = ftags))
    
    #find columns that are zero or constant
    zero_const <- stat_features |> 
                    dplyr::select(-key_vars(data)) |>
                    summarise(across(everything(), sd)) |>   #calcuate standard deviation of each column
                    select_if(~.x == 0 | is.na(.)) |> names() #select columns that are zero or NA
      
    stat_features |>
        dplyr::select(!all_of(zero_const))
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
    dplyr::select(-ReportingPeriod) |>
    as_tsibble(key = c(BankName,Measure), index = Quarter) 
}

summary_ratios <- memoise(function() {
  .read_ubpr("data/UBPR_Ratios_V.parquet") 
})

credit_card <- memoise(function(apply_bank_filters = FALSE, post_regulation = FALSE) {
  data <- .read_ubpr("data/UBPR_CreditCard_V.parquet") 
  
  if(apply_bank_filters) {
    data <- data |>
              filter(BankType %in% c("LargeBank", "LargeCreditCardBank")) |>
              tsibble::group_by_key() |>
              filter(Value != 0) |>
              dplyr::slice(3:n())
  }

  if (post_regulation) {
    data <- data |> filter_index(get_regulation_cutoff() ~ .)
  }
  data 
})

get_regulation_cutoff <- function(f = "Q") {
  if(f=="M")
    "2010 Mar"
  else 
    "2010 Q1"
}

.measure_to_tsibble <- function(measure_data, measure_name) {
  measure_data |>
  filter(Measure == measure_name) |>
  as_tsibble(index = Quarter, key = (-c(Quarter,Value))) |>
    group_by_key() |>
    fill_gaps() |>
    mutate(value_diff = difference(Value)) |> 
    drop_na(value_diff) |>
    ungroup() 
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
credit_card.overdue_3089 <- memoise(function(apply_bank_filters = FALSE, post_regulation = FALSE) {
  credit_card(apply_bank_filters, post_regulation) |>
    .measure_to_tsibble("UBPRE524")
})

credit_card.overdue30to89.agg  <- function(apply_bank_filters = FALSE, post_regulation = FALSE) {
  credit_card.overdue_3089(apply_bank_filters, post_regulation) |> 
    summarise(value.all.mean = mean(Value, na.rm = TRUE),
              value.all.median = median(Value, na.rm = TRUE),
              value_diff.all.mean = mean(value_diff, na.rm = TRUE),
              value_diff.all.median = median(value_diff, na.rm = TRUE)) 
}

credit_card.overdue30to89.agg.group  <- function(apply_bank_filters = FALSE, post_regulation = FALSE) {
  credit_card.overdue_3089(apply_bank_filters, post_regulation) |> 
    group_by(BankType) |> 
    summarise(value.group.mean = mean(Value, na.rm = TRUE),
              value.group.median = median(Value, na.rm = TRUE),
              value_diff.group.mean = mean(value_diff, na.rm = TRUE),
              value_diff.group.median = median(value_diff, na.rm = TRUE))
}

credit_card.overdue30to89.agg.lbank  <- function(apply_bank_filters = FALSE, post_regulation = FALSE) {
  credit_card.overdue30to89.agg.group(apply_bank_filters, post_regulation) |>
    filter(BankType == "LargeBank") 
}

credit_card.overdue30to89.agg.lccbank  <- function(apply_bank_filters = FALSE, post_regulation = FALSE) {
  credit_card.overdue30to89.agg.group(apply_bank_filters, post_regulation) |>
    filter(BankType == "LargeCreditCardBank")
}

#'Unused Commitments on Credit Cards
#' 
#' The unused portions of all commitments to extend credit both to individuals for household, family, and other personal 
#' expenditures and to other customers, including commercial or industrial enterprises, through credit cards.
#' 
credit_card.unused <- function(apply_bank_filters = FALSE, post_regulation = FALSE) {
  credit_card(apply_bank_filters,post_regulation) |>
    .measure_to_tsibble("UBPR3815")
}

#'Unused Commitments on Credit Cards as a percent of Total Assets
#' 
#' The unused portions of all commitments to extend credit both to individuals for household, family, and other personal
#' expenditures and to other customers, including commercial or industrial enterprises, through credit cards divided by total
#' assets.
#' 
credit_card.unused_ratio <- function(apply_bank_filters = FALSE, post_regulation = FALSE) {
  credit_card(apply_bank_filters,post_regulation) |>
    .measure_to_tsibble("UBPRE263")
}

#' Total credit card loans
#' 
#' Loans to Individuals for Household, Family, and Other Personal Expenditures 
#' (I.E., Consumer Loans)(Includes Purchased Paper): Credit Cards
#' 
credit_card.loan_amount <- function(apply_bank_filters = FALSE, post_regulation = FALSE) {
  credit_card(apply_bank_filters,post_regulation) |>
    .measure_to_tsibble("UBPRB538") 
}

credit_card.loan_proportion <- function(apply_bank_filters = FALSE, post_regulation = FALSE) {
  credit_card(apply_bank_filters,post_regulation) |>
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

us_economy <- memoise(function(freq = "M") {
  read_csv(glue("data/US_Economic_Data_{freq}.csv"), show_col_types = FALSE) |>
    mutate(Month = yearmonth(Date)) |>
    dplyr::select(-Date)|>
    as_tsibble(index = Month)
})


us_economy.recession <- memoise(function() {
  read_csv("data/JHDUSRGDPBR_M.csv", col_names = c("Month", "Flag"), show_col_types = FALSE) |>
    as_tsibble(index = Month)    
})

us_economy.psavert <- function() {
  us_economy() |>
    dplyr::select(PSAVERT) |>
      na.omit()
}

us_economy.confidence <- function() {
  us_economy() |>
    dplyr::select(CSCICP03USM665S) |>
    na.omit()
}

credit_card.partnerships <- function() {
  tibble::tibble_row(Partner = "Costco",
                Old="AMERICAN EXPRESS NATIONAL BANK (1394676)", 
                New="CITIBANK, N.A. (476810)",
                Acquired = "2016-04-01",
                Available = "2016-06-20") |>
                tibble::add_row(Partner = "Walmart",
                Old="SYNCHRONY BANK (1216022)",
                New="CAPITAL ONE, NATIONAL ASSOCIATION (112837)",
                Acquired = "2019-07-01",
                Available = "2019-09-24")|>
                tibble::add_row(Partner = "GAP",
                Old="SYNCHRONY BANK (1216022)", 
                New="BARCLAYS BANK DELAWARE (2980209)",
                Acquired = "2022-05-01",
                Available = "2022-06-20")
}

credit_card.partnerships.long <- function() {
  credit_card.partnerships() |> 
    pivot_longer(cols = c(Old, New), names_to = "Type", values_to = "Bank") |>
    mutate(Acquired = as.Date(Acquired),
           Available = as.Date(Available))
}

credit_card.partnerships.all <- function() {
  tibble::tibble_row(Partner = "Costco",
                Old="AMERICAN EXPRESS NATIONAL BANK (1394676)", 
                New="CITIBANK, N.A. (476810)",
                Acquired = "2016-04-01",
                Available = "2016-06-20") |>
                tibble::add_row(Partner = "Walmart",
                Old="SYNCHRONY BANK (1216022)",
                New="CAPITAL ONE, NATIONAL ASSOCIATION (112837)",
                Acquired = "2019-07-01",
                Available = "2019-09-24")|>
                tibble::add_row(Partner = "GAP",
                Old="SYNCHRONY BANK (1216022)", 
                New="BARCLAYS BANK DELAWARE (2980209)",
                Acquired = "2022-05-01",
                Available = "2022-06-20")|>
                tibble::add_row(Partner = "GM",
                Old="CAPITAL ONE, NATIONAL ASSOCIATION (112837)", 
                New="GOLDMAN SACHS BANK USA (2182786)",
                Acquired = "2022-01-01",
                Available = "2022-01-10") |>
                tibble::add_row(Partner = "Apple",
                Old="BARCLAYS BANK DELAWARE (2980209)", 
                Acquired = "2020-09-15") |>
                tibble::add_row(Partner = "Apple",
                New="GOLDMAN SACHS BANK USA (2182786)", 
                Acquired = "2019-09-01")|>
                tibble::add_row(Partner = "Apple",
                New="GOLDMAN SACHS BANK USA (2182786)", 
                Acquired = "2019-09-01") |>
                tibble::add_row(Partner = "Wayfair",
                New ="CITIBANK, N.A. (476810)", 
                Acquired = "2020-09-11") |>
                tibble::add_row(Partner = "HSBCPortfolio",
                Old="HSBC BANK USA, NATIONAL ASSOCIATION (413208)", 
                New="CAPITAL ONE, NATIONAL ASSOCIATION (112837)",
                Acquired = "2012-05-01",
                Available = "2012-05-01") |>
                tibble::add_row(Partner = "BestBuy",
                Old="CAPITAL ONE, NATIONAL ASSOCIATION (112837)", 
                New="CITIBANK, N.A. (476810)",
                Acquired = "2013-09-01",
                Available = "2013-09-01")

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
    dplyr::select(-c(IDRSSD,BankName, Measure, Label, Description, BankType)) |>
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
        scale_colour_manual(values = bank_type.colours())
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
                dplyr::select(IDRSSD) 

  if (!purrr::is_empty(outliers))  {
    data |>
      filter(IDRSSD %in% outliers$IDRSSD) |>
      ggplot(aes(x = Quarter, y = !!as.name(value_name), col = BankName)) +
      geom_line() +
      facet_wrap(~BankName, ncol = 1) +
      theme(legend.position = "none")  +
      labs(subtitle = glue("Outliers: {P1_lower} < PC1 < {P1_upper}, {P2_lower} < PC2 < {P2_upper}"),
           y = paste0(credit_card.target_label(), "\n", value_name)) +
      scale_colour_colorblind7()
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
              value_name == "value_diff" & mtype == "M" ~ "difference from prior period",
              value_name == "value_diff" & mtype == "P" ~ "difference from prior period",
              value_name == "pct_change" ~ "% change from prior period",
              value_name == "Value" & mtype == "M" ~ "Value ($)",
              value_name == "Value" & mtype == "P" ~ "Value (%)",
              .default = value_name)
}

partnership.plot <- function(Partner, Old, New, Acquired, Available, selected_measures,value_name, trend_calc = "median", date_obs_period = 2) {
  date_acquired <- as.Date(Acquired)
  date_available <- as.Date(Available)
  from_date <- year(date_acquired) - date_obs_period
  to_date <- year(date_acquired) + date_obs_period

  group.colours <- c(Old = "#1B9E77", New = "#D95F02", aggregate = "#999999")
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
    filter(BankName %in% c(Old, New)) |> 
    bind_rows(trend_data) |> 
    mutate(group_column = case_when(
                              BankName == Old ~ "Old",
                              BankName == New ~ "New")) |> 
    filter_index(as.character(from_date) ~ as.character(to_date)) |>
    autoplot(!!as.name(value_name), aes(colour = group_column)) +
    scale_colour_manual(values = group.colours) +
    facet_wrap(~ Description, scales = "free", ncol=1) +
    theme(legend.position = "none",
          plot.title = element_markdown(),
          strip.text = element_text(size = 12))  +
    labs(title=glue("{Partner} Partnership: <span style='color:#D95F02'>{Old}</span> to 
                    <span style='color:#1B9E77'>{New}</span>
                    against <span style='color:#333333'>{trend_calc} PEER Banks</span>"),
        subtitle = glue("Acquired {date_acquired}. Card available {date_available}"),
        y = y_label,
        x = "Year")  +
    scale_y_continuous(labels = scales::comma) +
    scale_x_yearmonth(date_breaks = "1 year", date_labels = "%Y")   +
    geom_vline(xintercept = as.numeric(date_acquired), linetype=1,colour="#00259e") +
    annotate("text", x=as.numeric(date_acquired-10), y=0, label="acquired", angle=90, hjust = 0)+
    geom_vline(xintercept = as.numeric(date_available), linetype=4) +
    annotate("text", x=as.numeric(date_available-10), y=0, label="available", angle=90, hjust = 0) 
    ggsave(glue("images/{Partner}_{measures_label}_{value_name}_{trend_calc}_partnership.png"), width = 16, height = nrow(measures)*4, dpi = 300)
}

run_granger_test <- function(data, target_variable, exog_variable, order = 4, do_difference = TRUE) {
    if (is.numeric(data[[exog_variable]])) {
        if(do_difference) {
            data <- data |> mutate(across(where(is.numeric), ~difference(.)))              
        }
        lmtest::grangertest(data[[target_variable]], data[[exog_variable]], order = order) |> as_tibble() |> na.omit() |> tibble::add_column(exog_variable)
    }
}

run_ccf_test <- function(data, target_variable, exog_variable, do_difference = TRUE) {
    if (is.numeric(data[[exog_variable]])) {
        if(do_difference) {
            data <- data |> mutate(across(where(is.numeric), ~difference(.)))                
        }
        feasts::CCF(y = !!as.name(target_variable), x = !!as.name(exog_variable), .data = data) |> autoplot()
    }
}

us_economy.labels <- function() {
    read_csv("data/us_economy_labels.csv", show_col_types = FALSE)
}

min_tstibble <- function(data) {
    ts_index <- data |> index_var()
    data |> 
        as_tibble()  |>
        dplyr::select(-all_of(ts_index)) |>
        summarise(across(where(is.numeric), \(x) min(x, na.rm = TRUE))) |> min()
}

max_tstibble <- function(data) {
    ts_index <- data |> index_var()
    data |> 
        as_tibble()  |>
        dplyr::select(-all_of(ts_index)) |>
        summarise(across(where(is.numeric), \(x) max(x, na.rm = TRUE))) |> max()
}

plot_us_category <- function(category, measures, target_data, target_measure = "value_median") {
    target_label <-credit_card.target_label()
    index_var <- index_var(measures)
    recessions <- us_economy.recession() |> 
                    filter(Flag == 1, 
                           Month >= ym("1989-10"))
    data <- measures |> drop_na() |>
              mutate(across(!as.name(index_var), scale))
    max_y <- max_tstibble(data)     

    data |>        
        pivot_longer(cols = !Quarter) |>
        left_join(us_economy.labels(), by = join_by(name==Name)) |>
        filter(Category == category) |>
        mutate(comb_label = paste(name, "-", Desc, "(", Formula, ")")) |>
        ggplot(aes(x = Quarter, y = value, colour = comb_label)) + 
        geom_line() +
        theme(legend.direction = "vertical",legend.position = "top", legend.title=element_blank(),
              plot.title = element_markdown(size = 12),
              plot.subtitle = element_text(size = 9)) +
        labs(y = "Normalised (Normal=0)",
            title = glue("{category} measures against <span style='color:darkslategrey'>**{target_label}**</span>"),
            subtitle = "Recessions shown in boxed area") +     
        geom_rect(data = recessions,inherit.aes = FALSE, 
                  aes(xmin = Month, xmax = Month + 30, ymin = -Inf, ymax = Inf), fill = "lightgrey", alpha = 0.5) +
        geom_line(mapping = aes(y = !!as.name(target_measure)), data = target_data,
                  colour = 'darkslategrey') +
        geom_vline(xintercept = as.numeric(yq(get_regulation_cutoff())), linetype=1,colour="darkred") +
        annotate("text", x=as.numeric(yq(get_regulation_cutoff())), y=max_y-1, size = 8/.pt, label="Credit Card Regulations",vjust = -0.5, colour = "darkred") +
        scale_colour_colorblind7()                     
}

credit_card.udpr <- function() {
    read_csv("data/UBPR_codes_descriptions.csv", show_col_types = FALSE)
}

us_economy.quarterly_selected <- function() {
  us_economy() |> 
    mutate(B069RC1.Pop.CPI =B069RC1/POPTHM/PCEPI) |>
    mutate(PCEDG.Pop.CPI = PCEDG/POPTHM/PCEPI) |>
    mutate(PCE.Pop.CPI = PCE/POPTHM/PCEPI) |>
    mutate(RRSFS.Pop = RRSFS/POPTHM) |> 
    mutate(A229RC0.CPI = A229RC0/PCEPI) |> 
    dplyr::select(-c(RRSFS,A229RC0,CPILFESL,CPIAUCSL,DSPI,DSPIC96,B069RC1,PCEDG,PCE,POPTHM,PCEPI)) |>
    mutate(Quarter = yearquarter(Month)) |>
    index_by(Quarter) |>
    summarise(across(!Month, \(x) mean(x, na.rm = TRUE))) |> drop_na()  
}

generate_model_data <- function() {
  useful_cc_measures <- c("UBPR3815", "UBPRB538", "UBPRD659", "UBPRE524", "UBPRE263", "UBPRE425")
  cc_data <- credit_card(apply_bank_filters = TRUE, post_regulation = FALSE) |>
              filter(Measure %in% useful_cc_measures) |> 
              as_tibble() |> 
              dplyr::select(Quarter,IDRSSD, BankName, BankType, Measure, Value) |> 
              group_by(Measure, IDRSSD) |>
              mutate(diff = difference(Value),
                    log = log(Value),
                    log.diff = difference(log),
                    log.diff.lag1 = lag(log.diff, 1),
                    log.diff.lag2 = lag(log.diff, 2),
                    log.diff.lag3 = lag(log.diff, 3),
                    log.diff.lag4 = lag(log.diff, 4),
                    diff.lag1 = lag(diff, 1),
                    diff.lag2 = lag(diff, 2),
                    diff.lag3 = lag(diff, 3),
                    diff.lag4 = lag(diff, 4),
                    pct_change = diff/lag(Value),
                    pct_change.lag1 = lag(pct_change,1),
                    pct_change.lag2 = lag(pct_change,2),
                    pct_change.lag3 = lag(pct_change,3),
                    pct_change.lag4 = lag(pct_change,4)) |> 
                      pivot_wider(names_from = Measure, values_from = Value:last_col(), names_glue = "{Measure}.{.value}") 

  #group 
  cc_data <- cc_data |> left_join (group_by(cc_data, Quarter, BankType) |> 
                                    summarise(
                                      UBPRE524.group = mean(UBPRE524.Value, na.rm= TRUE),
                                      UBPRE524.group.diff = mean(UBPRE524.diff, na.rm= TRUE))) |>
                        mutate( UBPRE524.group.log = log(UBPRE524.group),
                                UBPRE524.group.log.diff = difference(UBPRE524.group.log),
                                UBPRE524.group.log.diff.lag1 = lag(UBPRE524.group.log.diff, 1),
                                UBPRE524.group.log.diff.lag2 = lag(UBPRE524.group.log.diff, 2),
                                UBPRE524.group.log.diff.lag3 = lag(UBPRE524.group.log.diff, 3),
                                UBPRE524.group.log.diff.lag4 = lag(UBPRE524.group.log.diff, 4),
                                UBPRE524.group.diff.lag1 = lag(UBPRE524.group.diff,1),
                                UBPRE524.group.diff.lag2 = lag(UBPRE524.group.diff,2),
                                UBPRE524.group.diff.lag3 = lag(UBPRE524.group.diff,3),
                                UBPRE524.group.diff.lag4 = lag(UBPRE524.group.diff,4),
                                UBPRE524.group.pct_change = UBPRE524.group.diff/lag(UBPRE524.group),
                                UBPRE524.group.pct_change.lag1 = lag(UBPRE524.group.pct_change,1),
                                UBPRE524.group.pct_change.lag2 = lag(UBPRE524.group.pct_change,2),
                                UBPRE524.group.pct_change.lag3 = lag(UBPRE524.group.pct_change,3),
                                UBPRE524.group.pct_change.lag4 = lag(UBPRE524.group.pct_change,4))
  #all
  cc_data <- cc_data |> left_join (group_by(cc_data, Quarter) |> 
                                    summarise(
                                      UBPRE524.all = mean(UBPRE524.Value, na.rm= TRUE),
                                      UBPRE524.all.diff = mean(UBPRE524.diff, na.rm= TRUE))) |>
                        mutate(UBPRE524.all.log = log(UBPRE524.all),
                                UBPRE524.all.log.diff = difference(UBPRE524.all.log),
                                UBPRE524.all.log.diff.lag1 = lag(UBPRE524.all.log.diff, 1),
                                UBPRE524.all.log.diff.lag2 = lag(UBPRE524.all.log.diff, 2),
                                UBPRE524.all.log.diff.lag3 = lag(UBPRE524.all.log.diff, 3),
                                UBPRE524.all.log.diff.lag4 = lag(UBPRE524.all.log.diff, 4),
                                UBPRE524.all.diff.lag1 = lag(UBPRE524.all.diff,1),
                                UBPRE524.all.diff.lag2 = lag(UBPRE524.all.diff,2),
                                UBPRE524.all.diff.lag3 = lag(UBPRE524.all.diff,3),
                                UBPRE524.all.diff.lag4 = lag(UBPRE524.all.diff,4),
                                UBPRE524.all.pct_change = UBPRE524.all.diff/lag(UBPRE524.all),
                                UBPRE524.all.pct_change.lag1 = lag(UBPRE524.all.pct_change,1),
                                UBPRE524.all.pct_change.lag2 = lag(UBPRE524.all.pct_change,2),
                                UBPRE524.all.pct_change.lag3 = lag(UBPRE524.all.pct_change,3),
                                UBPRE524.all.pct_change.lag4 = lag(UBPRE524.all.pct_change,4))

  # percentage change economic measures
  econ_measures <- us_economy.quarterly_selected() |> 
                    pivot_longer(cols=!Quarter, names_to = "Measure", values_to = "raw") |>
                            group_by(Measure) |>
                            mutate(diff = difference(raw),
                                   log = log(raw),
                                   log.diff = difference(log),
                                   log.diff.lag1 = lag(log.diff, 1),
                                   log.diff.lag2 = lag(log.diff, 2),
                                   log.diff.lag3 = lag(log.diff, 3),
                                   log.diff.lag4 = lag(log.diff, 4),
                                   diff.lag1 = lag(diff,1),
                                   diff.lag2 = lag(diff,2),
                                   diff.lag3 = lag(diff,3),
                                   diff.lag4 = lag(diff,4),
                                   pct_change = diff/lag(raw),
                                   pct_change.lag1 = lag(pct_change,1),
                                   pct_change.lag2 = lag(pct_change,2),
                                   pct_change.lag3 = lag(pct_change,3),
                                   pct_change.lag4 = lag(pct_change,4)) |>
                              pivot_wider(names_from = Measure, values_from = raw:last_col(), names_glue = "{Measure}.{.value}") 
  cc_data |> 
    left_join(econ_measures, by = join_by(Quarter)) |> 
      as_tsibble(index = Quarter, key = c(IDRSSD, BankName, BankType)) |> 
        filter_index(get_regulation_cutoff() ~ .) |> readr::write_csv("data/final_model_data.csv")
}

.generate_model_hierachy_data <- function() {
  model_data <- .read_all_model_data()
  partner_naics <- read_csv("data/Partner_NAICS.csv",show_col_types = FALSE) |> 
                   dplyr::select(Partner, NAICS)
                    
  partnerships <- credit_card.partnerships.all() |>
                        mutate(across(c("Acquired", "Available"), \(x) yearquarter(lubridate::ymd(x)))) |>
                        pivot_longer(cols = c(New,Old), names_to = "Partnership", values_to = "BankName") |> filter(!is.na(BankName))

  new_partnerships <- partnerships |> filter(Partnership == "New")|>
                        dplyr::select(Partner, BankName) |>
                        cross_join(
                          tibble(
                            Quarter = seq(as.Date("2010-01-01"), as.Date("2024-01-01"), by = "quarter")                        
                          )
                        ) |> mutate(Quarter = yearquarter(Quarter)) |> left_join(partnerships, by = join_by(Quarter == Acquired, BankName, Partner)) |> 
                        group_by(Partner, BankName) |>
                        tidyr::fill(Partnership,.direction = "down") |>  mutate(HasPartner = if_else(is.na(Partnership), 0, 1)) |> dplyr::select(-c(Available, Partnership)) |> ungroup()

  old_partnerships <- partnerships |> filter(Partnership == "Old")|>
                        dplyr::select(Partner, BankName) |>
                        cross_join(
                          tibble(
                            Quarter = seq(as.Date("2010-01-01"), as.Date("2024-01-01"), by = "quarter")                        
                          )
                        ) |> mutate(Quarter = yearquarter(Quarter)) |> left_join(partnerships, by = join_by(Quarter == Acquired, BankName, Partner)) |> 
                        group_by(Partner, BankName) |>
                        tidyr::fill(Partnership,.direction = "up") |>  mutate(HasPartner = if_else(is.na(Partnership), 0, 1)) |> dplyr::select(-c(Available, Partnership))|> ungroup()
  
  #have all the data for partnerships. As a bank can be involved in multiple partnerships we have to ensure the data is repeated per partner
  all_partnerships <- bind_rows(new_partnerships, old_partnerships) |> 
    left_join(model_data, by = join_by(Quarter, BankName)) |> filter(!is.na(IDRSSD))       

  final_data <- all_partnerships |> 
                    left_join(partner_naics, by = join_by(Partner)) |> 
                    add_row(model_data |> filter(!BankName %in% all_partnerships$BankName)) |> 
                    mutate(Partner = replace_na(Partner, "None"),
                           HasPartner = replace_na(HasPartner, 0))  

  final_data |> readr::write_csv("data/final_hierarchy_model_data.csv")
}

.model_data_with_partnerships <- function() {
  partnerships <- credit_card.partnerships() |> 
                    mutate(across(c("Acquired", "Available"), lubridate::ymd),
                            PeriodStart = yearquarter(Acquired),
                            PeriodEnd = yearquarter(today()))  |> 
                    pivot_longer(cols = c(New,Old), names_to = "Partnership", values_to = "BankName")|>                             
                    dplyr::select(Partner, PeriodStart, PeriodEnd, BankName, Partnership) |> 
                    pivot_longer(cols = starts_with("Period"), names_to = "PeriodName", values_to = "Quarter")|> 
                    as_tsibble(index=Quarter, key=c(BankName,Partner)) |>
                    group_by_key() |>
                    fill_gaps() |> tidyr::fill(Partnership,.direction = "down") |> 
                      mutate(Period = row_number()-1) |>
                    dplyr::select(-PeriodName) |> pivot_wider(names_from = "Partner", values_from=Period)
  
  .read_all_model_data() |> left_join(partnerships,by = join_by(Quarter, BankName)) |>
    drop_na(UBPRE524.diff)|> 
    mutate(Qtr = quarter(Quarter), Year = year(Quarter)) |>
    readr::write_csv("data/final_model_data_wpartner.csv")
}

.read_all_model_data <- function() {
  file_loc <- "data/final_model_data.csv"
  if (!file.exists(file_loc)) {
    generate_model_data()
  }
  read_csv("data/final_model_data.csv", show_col_types = FALSE) |> mutate(Quarter = yearquarter(Quarter)) |>
     as_tsibble(index = Quarter, key = c(IDRSSD, BankName, BankType))
}

get_hierarchy_model_data <- function() {
  file_loc <- "data/final_hierarchy_model_data.csv"
  if (!file.exists(file_loc)) {
    .generate_model_hierachy_data()
  }
  read_csv("data/final_hierarchy_model_data.csv", show_col_types = FALSE) |> 
    mutate(Quarter = yearquarter(Quarter))      
}

.read_model_data <- function() {
  file_loc <- "data/final_model_data_wpartner.csv"
  if (!file.exists(file_loc)) {
    .model_data_with_partnerships()
  }
  read_csv("data/final_model_data_wpartner.csv", show_col_types = FALSE) |> 
    mutate(Quarter = yearquarter(Quarter)) |>
     as_tsibble(index = Quarter, key = c(IDRSSD, BankName, BankType))
}

get_model_data <- function() {
  all_data <- .read_model_data() |>
                mutate(Qtr = as.factor(Qtr),
                       IDRSSD = as.factor(IDRSSD),
                       BankType = as.factor(BankType), 
                       BankName = as.factor(BankName)) |>
                       relocate(Qtr)

  estimation_data <- all_data |> 
                      filter(is.na(Partnership)) |> 
                        dplyr::select(-c(Partnership:last_col())) |>
                          drop_na() #|> tsibble::fill_gaps()
  
  observation_data <- all_data |> 
                        filter(!is.na(Partnership))
  #consistent levels
  observation_data$BankName <- factor(observation_data$BankName , levels = levels(estimation_data$BankName))
  observation_data$IDRSSD <- factor(observation_data$IDRSSD , levels = levels(estimation_data$IDRSSD))

  list(all_data = all_data, estimation_data = estimation_data, observation_data = observation_data)
}

plot_cc_measures <- function(bank_fuzzy, data, ubpr_labels) {
  data |> filter(grepl(bank_fuzzy,BankName)) |> 
  dplyr::select(UBPRE524 = UBPRE524.Value, UBPRE263 = UBPRE263.Value, UBPRE425 = UBPRE425.Value, UBPRB538 = UBPRB538.Value) |> 
   pivot_longer(cols= -Quarter, names_to = "UBPR_Code") |> 
   mutate(display_order = case_when(
                          UBPR_Code == "UBPRE524" ~ "(a)",
                          UBPR_Code == "UBPRE263" ~ "(b)",
                          UBPR_Code == "UBPRE425" ~ "(c)",
                          UBPR_Code == "UBPRB538" ~ "(d)"),
                          .default = "" ) |>
   left_join(ubpr_labels,by = join_by(UBPR_Code)) |> 
   mutate(Description = paste(display_order, Description)) |>
   ggplot(aes(x = Quarter, y =value, colour = UBPR_Code)) + 
   geom_line() + 
   labs(y='') +
   theme(legend.position = "none") +
   facet_wrap(~Description, scales = "free_y", ncol=1)
}

plot_model_fit <- function (data, target_name) {
  data |>
    autoplot(!!as.name(target_name), color = "darkgrey") + 
        geom_line(aes(y = .fitted, color = "#D55E00")) + facet_wrap(~BankName+BankType+.model, ncol = 1) +
        labs(title = "<span style='color:#D55E00'>Fitted</span> vs. <span style='color:darkgrey'>Observed</span>") +
        theme(legend.position = "none", plot.title = element_markdown(),plot.subtitle = element_markdown())
}

plot_bank_residual <- function(fuzzy_bankname, data) {
  bank_data <- data |> filter(grepl(fuzzy_bankname,BankName)) |> as_tibble()
  
  plot1 <- bank_data |> ggplot(aes(x = Quarter, y = .resid)) + geom_line() + geom_point()
  plot2 <- acf(bank_data$.resid,plot = FALSE) %>% forecast::autoplot() + labs(title = element_blank())
  plot3 <- bank_data |> ggplot(aes(x = .resid)) + geom_histogram(bins = 10)
  resid_plot <- plot1 / (patchwork::wrap_elements(plot2) | plot3) + patchwork::plot_layout(widths = c(7, 3))
  resid_plot       
}

save_arima_results <- function(table_results, model_cols, file_name) {
  table_results |> 
    pivot_longer(cols = !!model_cols, names_to = ".model", values_to = ".model_spec") |> 
      left_join(glance(table_results)) |>
      readr::write_csv(glue("data/results/{file_name}"))
}

read_arima_results <- function() {
  list.files("data/results", pattern = "arima.*results.csv", full.names = TRUE) |> 
    map(read_csv, show_col_types = FALSE) |> list_rbind()
}

read_tscv_results <- function(model_type = "arima") {
  list.files("data/results/tscv",pattern = glue("*tscv_{model_type}*") , full.names = TRUE) |> 
    map(read_csv, show_col_types = FALSE) |> list_rbind()
}

plot_prediction <- function(bank, partner, bank_fcasts, all_data) {
    bank_fcast_data <- bank_fcasts |> filter(BankName == bank)
    bank_data <- all_data |> filter(BankName == bank)

    fcast_data <- bank_fcast_data |> filter(Partner == partner) |> head(8) 
    est_data <- bank_data |> filter(is.na(!!as.name(partner)))
    est_data_trunc <- est_data |> tail(ifelse(nrow(est_data)<11,nrow(est_data),11))
    event_data <- bank_data |> filter(!!as.name(partner) >=0) |> head(8) 
    comb_data <- bind_rows(est_data_trunc,event_data)
    min_qtr <- event_data |> filter(Quarter == min(Quarter)) |> pull(Quarter)

    fcast_data |> 
    ggplot()  + 
    geom_line(aes(x = Quarter, y=predicted), color= "red") +
    geom_line(aes(x = Quarter, y=UBPRE524.Value), data = comb_data, color='darkslategrey',linetype = "longdash") + 
    geom_vline(xintercept = as.Date(min_qtr) , linetype=1,color="grey") +
    labs(title = bank, subtitle = partner, y = credit_card.target_label())   
}

original_scale <- function(fcast_bank_data, all_data, col_name = ".mean") {
  keys <- key_vars(fcast_bank_data)
  result <- fcast_bank_data
  bank_name <- fcast_bank_data[[1,"BankName"]]
  if ("UBPRE524.diff" %in% names(fcast_bank_data) & class(fcast_bank_data[[1, "UBPRE524.diff"]])[1] != "numeric") {
      print(paste("Bank uses Diff", bank_name))
      result <- result |> original_scale_diff(all_data, bank_name,col_name)       
  } else {
    print(paste("Bank uses Value", bank_name))
    result <- result |> rename(predicted = !!as.name(col_name))
  }
  result |> as_tsibble() |> dplyr::select(all_of(c(keys,"predicted")))
}

original_scale_diff <- function(fcast_bank_data, all_data, bank_name, col_name = ".fitted") {
  quarter0 <- fcast_bank_data[[1,"Quarter"]] - 1      
  value0 <- all_data |> 
              filter(BankName == bank_name, 
                      Quarter == quarter0) |> pull(UBPRE524.Value)
  fcast_bank_data |> mutate(predicted = value0 + cumsum(!!as.name(col_name)))    
}

print_ar <- function(prediction_result, partner, bank) {
  prediction_result |> filter(Partner == partner, BankName == bank) |>
  group_by(Partner,BankName) |> 
  mutate(AR = observed - predicted,
         Period = row_number(),
         cum_mean = cummean(AR),
         cum_sum = cumsum(AR)) |> as_tibble() |>
    dplyr::select(Period, observed, predicted, AR, 
              `Cumulative Mean AR` = cum_mean,
              `Cumulative AR` = cum_sum) |> 
      mutate(across(where(is.numeric), \(x) round(x,2))) |>               
      rmarkdown::paged_table(options = list(rows.print = 16))
}

nest_data_for_step_cv <- function(estimation_data) {
    data <- estimation_data |> as_tibble() |>
              mutate(Quarter = factor(Quarter, levels = unique(Quarter))) |>
              arrange(Quarter) |>
              mutate(Quarter_Index = as.integer(factor(Quarter))) |> 
              relocate(Quarter_Index)

    cumulative_data <- map(unique(data$Quarter_Index), ~ {
                        data |>
                            filter(Quarter_Index < .x)
                        })

    est_data_tr <- tibble(
                    Quarter_Index = unique(data$Quarter_Index),
                    data = cumulative_data
                   )

    step_data_tr <- data |> 
                      group_by(Quarter_Index) |> 
                      nest(new_data = -c(Quarter_Index))


    est_data_tr |> left_join(step_data_tr) |> drop_na(new_data)
}

get_ubpr_labels <- function() {
  read_csv("data/UBPR_codes_descriptions.csv", col_select = c(1:2),show_col_types = FALSE, col_names = c("Code","Desc")) |> add_column(Source = "FFIEC")
}

get_feature_labels <- function() {
  bind_rows(
    get_ubpr_labels(),
    read_csv("data/us_economy_labels.csv", col_select = c(1:2),show_col_types = FALSE,col_names = c("Code","Desc")) |> add_column(Source = "FED")
    )
}

get_summary_validation.xgboost <- function(model_name, banks, include_model_name = FALSE) {
  comb <- read_csv(glue("data/results/xgbTree_validation_{model_name}.csv"),show_col_types = FALSE)
  get_summary_validation(comb, banks, "xgboost", include_model_name)
}

get_summary_validation.arima <- function(banks, include_model_name = FALSE) {
  cv_results_est <- readr::read_csv("data/results/estimate_arima_metrics.csv",show_col_types = FALSE) |> mutate(across(where(is.numeric), \(x) round(x,4))) |> 
  dplyr::select(BankName, BankType, .type, RMSE, MAE)

  cv_results_test <- read_tscv_results("arima") |> group_by(.type,BankName,BankType) |> 
    summarise(across(where(is.numeric), \(x) mean(x, na.rm = TRUE))) |> 
    mutate(across(where(is.numeric), \(x) round(x,4))) |> 
    dplyr::select(BankName, BankType, .type, RMSE, MAE) |> mutate(.type = "CV") 
    
  comb <- bind_rows(cv_results_est, cv_results_test) 
  get_summary_validation(comb, banks, "arima", include_model_name)
}

get_summary_validation.market <- function(banks, include_model_name = FALSE) {
  cv_results_est <- readr::read_csv("data/results/estimate_market_metrics.csv",show_col_types = FALSE) |> 
  dplyr::select(-.model) |>
  mutate(across(where(is.numeric), \(x) round(x,4))) |> 
  dplyr::select(BankName, BankType, .type, RMSE, MAE)

  cv_results_test <-readr::read_csv("data/results/estimate_tscv_market_metrics.csv",show_col_types = FALSE) |> 
    dplyr::select(-.model) |>
    mutate(across(where(is.numeric), \(x) round(x,4))) |> 
      dplyr::select(BankName, BankType, .type, RMSE, MAE) |> mutate(.type = "CV")
        
  comb <- bind_rows(cv_results_est, cv_results_test) 
  get_summary_validation(comb, banks, "market", include_model_name)
}

get_summary_validation.hierarchy <- function(banks, include_model_name = FALSE) {
  cv_results_est <- readr::read_csv("data/results/estimate_hier_metrics.csv",show_col_types = FALSE) |> mutate(across(where(is.numeric), \(x) round(x,4))) |> 
  dplyr::select(BankName, BankType, .type, RMSE, MAE)

  cv_results_test <- read_tscv_results("hier") |> group_by(.type,BankName, BankType) |> 
    summarise(across(where(is.numeric), \(x) mean(x, na.rm = TRUE))) |> 
    mutate(across(where(is.numeric), \(x) round(x,4))) |> 
    dplyr::select(BankName, BankType, .type, RMSE, MAE) |> mutate(.type = "CV") 
    
  comb <- bind_rows(cv_results_est, cv_results_test) 
  get_summary_validation(comb, banks, "hierarchy", include_model_name)
}

get_summary_validation <- function(data, banks, model_name, include_model_name = FALSE) {
  summary_banks <- data |> 
                    filter(!is.null(banks) & BankName %in% banks) |> 
                    arrange(BankName,.type) |> mutate(across(where(is.numeric), \(x) round(x,3)))

  summary_mean_banks <- data |> filter(!is.null(banks) &BankName %in% banks) |> 
                          group_by(.type) |> summarise(across(where(is.numeric), \(x) mean(x, na.rm = TRUE))) |>
                          mutate(across(where(is.numeric), \(x) round(x,3)))
                            
  summary_mean_all <- data |> group_by(.type) |> 
                        summarise(across(where(is.numeric), \(x) mean(x, na.rm = TRUE))) |> 
                        mutate(across(where(is.numeric), \(x) round(x,3)))

  summary_mean_type <- data |> group_by(BankType, .type) |> 
                        summarise(across(where(is.numeric), \(x) mean(x, na.rm = TRUE))) |> 
                        mutate(across(where(is.numeric), \(x) round(x,3)))

  if (include_model_name) {
    summary_banks <- summary_banks |> mutate(Model = model_name)
    summary_mean_banks <- summary_mean_banks |> mutate(Model = model_name)
    summary_mean_all <- summary_mean_all |> mutate(Model = model_name)
    summary_mean_type <- summary_mean_type |> mutate(Model = model_name)
  }
  list(tbl1 = summary_banks, tbl2 = summary_mean_banks, tbl3 = summary_mean_all, tbl4 = summary_mean_type)
}
