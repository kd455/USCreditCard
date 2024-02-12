library(tidyverse)
#using the https://tidyverts.org/ world
library(fable)
library(caret)
#library(lmer4)
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
library(lmtest)
library(ggthemes)
library(Metrics)
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
                    select(-key_vars(data)) |>
                    summarise(across(everything(), sd)) |>   #calcuate standard deviation of each column
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

# white.noise.sample <- function() {
#   tsibble(Period = 1:100, Value = rnorm(100), index = Period)
# }

# white.noise.plot <- function() {
#   white.noise.sample() |> 
#     ggplot(aes(x = Period, y = Value)) +
#     geom_line(linewidth = .2) +
#     labs(title="White noise")
  
# }

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
                select(IDRSSD) 

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
    autoplot(!!as.name(value_name), aes(colour = group_column)) +
    scale_colour_manual(values = group.colours) +
    facet_wrap(~ Description, scales = "free", ncol=1) +
    labs(title=glue("{name} Partnership: <span style='colour:#D95F02'>{old}</span> to 
                    <span style='colour:#1B9E77'>{new}</span>
                    against <span style='colour:#333333'>{trend_calc} PEER Banks</span>"),
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
    ggsave(glue("images/{name}_{measures_label}_{value_name}_{trend_calc}_partnership.png"), width = 16, height = nrow(measures)*4, dpi = 300)
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
        select(-all_of(ts_index)) |>
        summarise(across(where(is.numeric), \(x) min(x, na.rm = TRUE))) |> min()
}

max_tstibble <- function(data) {
    ts_index <- data |> index_var()
    data |> 
        as_tibble()  |>
        select(-all_of(ts_index)) |>
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
            title = glue("{category} measures against <span style='colour:darkslategrey'>**{target_label}**</span>"),
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
    select(-c(RRSFS,A229RC0,CPILFESL,CPIAUCSL,DSPI,DSPIC96,B069RC1,PCEDG,PCE,POPTHM,PCEPI)) |>
    mutate(Quarter = yearquarter(Month)) |>
    index_by(Quarter) |>
    summarise(across(!Month, \(x) mean(x, na.rm = TRUE))) |> drop_na()  
}

generate_model_data <- function() {
  useful_cc_measures <- c("UBPR3815", "UBPRB538", "UBPRD659", "UBPRE524", "UBPRE263", "UBPRE425")
  cc_data <- credit_card(apply_bank_filters = TRUE, post_regulation = FALSE) |>
              filter(Measure %in% useful_cc_measures) |> 
              as_tibble() |> 
              select(Quarter,IDRSSD, BankName, BankType, Measure, Value) |> 
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
  partnerships <- credit_card.partnerships.all() |>
                        mutate(across(c("Acquired", "Available"), \(x) yearquarter(lubridate::ymd(x)))) |>
                        pivot_longer(cols = c(New,Old), names_to = "Partnership", values_to = "BankName") |> filter(!is.na(BankName))

  new_partnerships <- partnerships |> filter(Partnership == "New")|>
                        select(Partner, BankName) |>
                        cross_join(
                          tibble(
                            Quarter = seq(as.Date("2010-01-01"), as.Date("2024-01-01"), by = "quarter")                        
                          )
                        ) |> mutate(Quarter = yearquarter(Quarter)) |> left_join(partnerships, by = join_by(Quarter == Acquired, BankName, Partner)) |> 
                        group_by(Partner, BankName) |>
                        tidyr::fill(Partnership,.direction = "down") |>  mutate(HasPartner = if_else(is.na(Partnership), 0, 1)) |> select(-c(Available, Partnership)) |> ungroup()

  old_partnerships <- partnerships |> filter(Partnership == "Old")|>
                        select(Partner, BankName) |>
                        cross_join(
                          tibble(
                            Quarter = seq(as.Date("2010-01-01"), as.Date("2024-01-01"), by = "quarter")                        
                          )
                        ) |> mutate(Quarter = yearquarter(Quarter)) |> left_join(partnerships, by = join_by(Quarter == Acquired, BankName, Partner)) |> 
                        group_by(Partner, BankName) |>
                        tidyr::fill(Partnership,.direction = "up") |>  mutate(HasPartner = if_else(is.na(Partnership), 0, 1)) |> select(-c(Available, Partnership))|> ungroup()
  
  #have all the data for partnerships. As a bank can be involved in multiple partnerships we have to ensure the data is repeated per partner
  all_partnerships <- bind_rows(new_partnerships, old_partnerships) |> left_join(model_data, by = join_by(Quarter, BankName)) |> filter(!is.na(IDRSSD))       

  final_data <- all_partnerships |> 
                  add_row(model_data |> filter(!BankName %in% all_partnerships$BankName)) |> 
                    mutate(Partner = replace_na(Partner, "None"),
                           HasPartner = replace_na(HasPartner, 0))  

  final_data |> readr::write_csv("data/final_hierarchy_model_data.csv")
}

.model_data_with_partnerships <- function(qtrs_prior_event = 1) {
  partnerships <- credit_card.partnerships() |> 
                    mutate(across(c("Acquired", "Available"), lubridate::ymd),
                            PeriodStart = yearquarter(Acquired) - qtrs_prior_event,
                            PeriodEnd = yearquarter(today()))  |> 
                    pivot_longer(cols = c(New,Old), names_to = "Partnership", values_to = "BankName")|>                             
                    select(Partner, PeriodStart, PeriodEnd, BankName, Partnership) |> 
                    pivot_longer(cols = starts_with("Period"), names_to = "PeriodName", values_to = "Quarter")|> 
                    as_tsibble(index=Quarter, key=c(BankName,Partner)) |>
                    group_by_key() |>
                    fill_gaps() |> tidyr::fill(Partnership,.direction = "down") |> 
                      mutate(Period = row_number()-(1+qtrs_prior_event)) |>
                    select(-PeriodName) |> pivot_wider(names_from = "Partner", values_from=Period)
  
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

get_model_data <- function() {
  file_loc <- "data/final_model_data_wpartner.csv"
  if (!file.exists(file_loc)) {
    .model_data_with_partnerships()
  }
  read_csv("data/final_model_data_wpartner.csv", show_col_types = FALSE) |> 
    mutate(Quarter = yearquarter(Quarter)) |>
     as_tsibble(index = Quarter, key = c(IDRSSD, BankName, BankType))
}

run_timeseries_cv <- function(data, formula_string, model_func = TSLM, predict_func= predict, initial_window = 9, horizon = 1, dependent_var = "UBPRE524.diff", fixedWindow = TRUE) {
    set.seed(123)  # For reproducibility
    # Create 5-fold cross-validation indices
    slices <- caret::createTimeSlices(1:nrow(data), initial_window, horizon, fixedWindow = fixedWindow)
    results <- list()

    for(i in 1:length(slices$train)) {
        # Extract training and testing data using the indices
        training_indices <- slices$train[[i]]
        validation_indices <- slices$test[[i]]
        training_set <- data |> filter(Quarter %in% data$Quarter[training_indices])
        validation_set <- data |> filter(Quarter %in% data$Quarter[validation_indices])

        # Skip if validation set contains FirmID levels not present in training set
        if(any(!(validation_set$IDRSSD %in% training_set$IDRSSD))) {
          next
        }
        # Fit model on the training data
        model <- training_set |> fabletools::model(m = model_func(as.formula(formula_string)))

        # Predict on the validation data
        predictions <- predict_func(model, newdata = validation_set)
        
        # Evaluate the model (you can choose your evaluation metric, e.g., RMSE)
        rmse_value <- Metrics::rmse(validation_set[[dependent_var]], predictions)

        # Store the results
        results[[i]] <- rmse_value
    }

    # Calculate the average performance across all slices
    average_performance <- mean(unlist(results))
    average_performance
}

plot_cc_measures <- function(bank_fuzzy, data, ubpr_labels) {
  data |> filter(grepl(bank_fuzzy,BankName)) |> 
  select(UBPRE524 = UBPRE524.Value, UBPRE263 = UBPRE263.Value, UBPRE425 = UBPRE425.Value, UBPRB538 = UBPRB538.Value) |> 
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
    autoplot(!!as.name(target_name), colour = "darkgrey") + 
        geom_line(aes(y = .fitted, colour = "#D55E00")) + facet_wrap(~BankName+BankType, ncol = 2) +
        labs(title = "<span style='color:#D55E00'>Fitted</span> vs. <span style='color:darkgrey'>Observed</span>") +
        theme(legend.position = "none", plot.title = element_markdown(),plot.subtitle = element_markdown())
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

plot_prediction <- function(bank_name, partner_name, estimation_data, observation_data, models) {
  est_data <- estimation_data |> filter(BankName == bank_name)
  est_data_trunc <- est_data |> tail(ifelse(nrow(est_data)<11,nrow(est_data),11))
  event_data <- observation_data |> filter(BankName == bank_name) |> head(5)
  comb_data <- bind_rows(est_data_trunc, event_data)

  models |>
    filter(BankName == bank_name)  |>
    forecast(new_data= event_data) |>
    filter(.model=='tslm') |> 
    autoplot()  + 
    geom_line(aes(x = Quarter, y=UBPRE524.Value), data = comb_data, colour='darkslategrey',linetype = "longdash") +
    labs(title = bank_name, subtitle = partner_name, y = glue("{target_label} (differenced)"))
}
# run_timeseries_cv_multiple_firms <- function(data, formula_string, model_func = lm, predict_func= predict, initial_window = 3, horizon = 1, dependent_var = "value_diff") {
#     # Split the data by firm
#     firm_data_list <- split(data, data$IDRSSD)

#     # Perform cross-validation for each firm and calculate mean RMSE per firm
#     cv_results <- map(firm_data_list, ~time_series_cv_per_firm(.x, formula_string, model_func, predict_func, initial_window, horizon, dependent_var)) |>
#                   map_df(~mutate(.x, MeanRMSE = mean(RMSE)), .id = "IDRSSD")

#     # Calculate the overall mean RMSE across all firms
#     overall_mean_rmse <- mean(cv_results$MeanRMSE)

#     return(list(PerFirmRMSE = cv_results, OverallMeanRMSE = overall_mean_rmse))
# }


# time_series_cv_per_firm <- function(data, formula_string, model_func=lm, predict_func = predict, initial_window = 3, horizon = 1, dependent_var = "value_diff") {
#   total_points <- nrow(data)
#   results <- tibble()

#   for (start_idx in 1:(total_points - initial_window - horizon + 1)) {
#     end_idx <- start_idx + initial_window - 1 + horizon
#     if (end_idx > total_points) {
#       break
#     }

#     training_set <- data[start_idx:(end_idx - horizon), ]
#     validation_set <- data[(end_idx - horizon + 1):end_idx, ]

#     model <- model_func(formula_string, data = training_set)
#     validation_preds <- predict_func(model, newdata = validation_set)

#     rmse_value <- Metrics::rmse(validation_set[[dependent_var]], validation_preds)

#     results <- rbind(results, tibble(Start = start_idx, RMSE = rmse_value))
#   }

#   return(results)
# }

# run_kfold_validation <- function(fitted_model, data, predict_func = predict) {
#     set.seed(123)  # For reproducibility
#     folds <- caret::createFolds(data$IDRSSD, k = 5)
#     results <- data.frame()  # Data frame to store results
    
#     for(i in 1:length(folds)) {
#         # Split the data into training and validation sets
#         training_set <- data[-folds[[i]], ]
#         validation_set <- data[folds[[i]], ]
        
#         # Predict on the validation set
#         validation_preds <- predict_func(fitted_model, newdata = validation_set)

#         # Evaluate the model (using an appropriate metric like RMSE)
#         rmse_value <- rmse(validation_set$UBPRE524.diff, validation_preds)
#         mae_value <- mae(validation_set$UBPRE524.diff, validation_preds)
#         # Store the results
#         results <- rbind(results, data.frame(Fold = i, RMSE = rmse_value))
#     }

#     return(list(PerFirmRMSE = cv_results, OverallMeanRMSE = overall_mean_rmse))

#     # Calculate the average performance across all folds
#     average_performance <- mean(results$RMSE)
#     return(average_performance)
# }

