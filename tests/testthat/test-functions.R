library(testthat)
source("../../functions.R")

test_that("matrix.to.tstibble works correctly", {
  test_matrix <- matrix(1:9, nrow = 3)
  result <- matrix.to.tstibble(test_matrix)
  
  expect_is(result, "tbl_ts")
  expect_equal(nrow(result), 9)
  expect_equal(ncol(result), 3)
})

test_that("credit_data works correctly", {
  result = credit_data()  
  skip("Test not implemented. Requires 'data/UBPR_CreditCard_V.parquet' file.")
})

test_that("us_economy works correctly", {
  skip("Test not implemented. Requires 'data/US_Economic_Data_{freq}.csv' file.")
})


test_that("partnership.plot works correctly", {
  selected_cc <- credit_card.loan_amount()
  test_data <- credit_card.partnerships() |> 
    mutate(data = list(selected_cc)) |> 
    relocate(data)|> 
    rename(partnership = name)
  
  result <- pmap(test_data, partnership.plot) 
  
  expect_is(result, "list")
  expect_equal(length(result), 1)
  expect_is(result[[1]], "ggplot")
})
