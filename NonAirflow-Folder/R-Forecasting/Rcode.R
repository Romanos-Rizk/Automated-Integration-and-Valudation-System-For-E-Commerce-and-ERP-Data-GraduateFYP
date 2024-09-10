#Importing Libraries
library(urca)
library(fpp2)
library(zoo)
library(MuMIn)

#Reading the data
data <- read.csv("C:\\Users\\Lenovo\\AIRFLOW_DOCKER_1\\NonAirflow-Folder\\R-Forecasting\\ecom_orders2.csv")

#Preparing the Data:
data$date <- as.Date(data$date, format = "%Y-%m-%d")
full_dates <- seq.Date(from = as.Date("2023-11-01"), to = as.Date("2024-03-15"), by = "day")
full_data <- data.frame(date = full_dates)
merged_data <- merge(full_data, data, by = "date", all.x = TRUE)
merged_data$total_amount <- na.approx(merged_data$total_amount, rule = 2)

#Autoplot
ts_data <- ts(merged_data$total_amount, start = c(2023, as.numeric(format(as.Date("2023-11-01"), "%j"))), frequency = 365)

autoplot(ts_data)

#ACF, Kpss, nsdiffs, ndiffs
ggAcf(ts_data,lag.max=48)
summary(ur.kpss(ts_data)) #0.6732 
nsdiffs(ts_data) #0
ndiffs(ts_data) #1

#Dividing the data into two parts
dataTrain=window(ts_data, start=c(2023,305), end=c(2024,58))
dataTest=window(ts_data, start=c(2024,59))

#Modeling the training part
#checking if the series needs a boxcox transfromation
BoxCox.lambda(dataTrain) #1
autoplot(dataTrain)

#First Differencing
dataTrain_Firstdiff = diff(dataTrain)
autoplot(dataTrain_Firstdiff)
summary(ur.kpss(dataTrain_Firstdiff)) #0.0382
ggtsdisplay(dataTrain_Firstdiff,lag=48)

#Second Differencing
dataTrain_Seconddiff=diff(dataTrain_Firstdiff)
autoplot(dataTrain_Seconddiff)
summary(ur.kpss(dataTrain_Seconddiff)) #0.0224
ggtsdisplay(dataTrain_Seconddiff,lag=48)


#Modeling Part 1

#Model 1
fit_111 <- Arima(dataTrain, order=c(1,1,1))
checkresiduals(fit_111) #p-value = 0.6523
summary(fit_111)
Forecasted_111=forecast(fit_111, h=12)
accuracy(Forecasted_111,dataTest)
autoplot(Forecasted_111)
#                    ME     RMSE      MAE       MPE     MAPE MASE       ACF1 Theil's U
#Training set  33.51036 389.4278 237.1982 -17.81771 40.45807  NaN 0.02687191        NA
#Test set     -12.54904 339.1476 236.9571 -11.43517 24.47302  NaN 0.51176270  1.616587

#Model 2
fit_211 <- Arima(dataTrain, order=c(2,1,1))
checkresiduals(fit_211) #p-value = 0.769
summary(fit_211)
Forecasted_211=forecast(fit_211, h=12)
accuracy(Forecasted_211,dataTest)
#                   ME     RMSE      MAE        MPE     MAPE MASE        ACF1 Theil's U
#Training set 31.090565 387.7074 236.0119 -17.870070 40.61190  NaN -0.01298671        NA
#Test set      7.122459 354.1572 246.0496  -9.823014 24.85088  NaN  0.52870253  1.626001


#Model 4
fit_021 <- Arima(dataTrain, order=c(0,2,1))
checkresiduals(fit_021) #0.1155
summary(fit_021)
Forecasted_021=forecast(fit_021, h=12)
accuracy(Forecasted_021,dataTest)
#                      ME     RMSE      MAE       MPE     MAPE MASE       ACF1 Theil's U
#Training set   -7.678317 429.1419 257.1576 -19.42028 43.57496  NaN -0.2009615        NA
#Test set     -392.061003 561.1928 508.3934 -52.74599 58.83586  NaN  0.5959839  3.323031

#Model 7
auto.arima(dataTrain, approximation=FALSE) #ARIMA(1,1,1) 

#Coefficients:
#  ar1     ma1
#0.4575  -0.868
#s.e.  0.1143   0.062

#sigma^2 = 155576:  log likelihood = -872.08
#AIC=1750.15   AICc=1750.36   BIC=1758.46

# Table

# Create the data frame with performance metrics
performance_metrics <- data.frame(
  Model = c("ARIMA(1, 1, 1)", "ARIMA(2, 1, 1)", "ARIMA(0, 2, 1)"),
  MAPE = c(24.47302, 24.85088, 58.83586),
  RMSE = c(339.1476, 354.1572, 561.1928),
  AICc = c(AICc(fit_111), AICc(fit_211), AICc(fit_021)),
  Ljung_Box_pvalue = c(0.6523, 0.769, 0.1155),
  Residuals = c("No Autocorrelation", "No Autocorrelation", "No Autocorrelation")
)

# Round the AICc and Ljung-Box p-values for better readability
performance_metrics$AICc <- round(performance_metrics$AICc, 2)
performance_metrics$Ljung_Box_pvalue <- round(performance_metrics$Ljung_Box_pvalue, 4)

# Display the table
print(performance_metrics)


# Modeling Part 2


# Model 1: ETS(A, A, N)
ets_AAN <- ets(dataTrain, model = "AAN", damped = FALSE)
checkresiduals(ets_AAN, lag.max = 12)
summary(ets_AAN)
Forecast_ets_AAN <- forecast(ets_AAN, h = 12)
accuracy(Forecast_ets_AAN, dataTest)

# Model 2: ETS(M, A, N)
ets_MAN <- ets(dataTrain, model = "MAN", damped = FALSE)
checkresiduals(ets_MAN, lag.max = 12)
summary(ets_MAN)
Forecast_ets_MAN <- forecast(ets_MAN, h = 12)
accuracy(Forecast_ets_MAN, dataTest)

# Model 3: ETS(M, M, N)
ets_MMN <- ets(dataTrain, model = "MMN", damped = FALSE)
checkresiduals(ets_MMN, lag.max = 12)
summary(ets_MMN)
Forecast_ets_MMN <- forecast(ets_MMN, h = 12)
accuracy(Forecast_ets_MMN, dataTest)

# Model 3: ETS(A, N, N)
ets_ANN <- ets(dataTrain, model = "ANN", damped = FALSE)
checkresiduals(ets_ANN, lag.max = 12)
summary(ets_ANN)
Forecast_ets_ANN <- forecast(ets_ANN, h = 12)
accuracy(Forecast_ets_ANN, dataTest)

# Automatic
ets = ets(dataTrain) #AAN
ets

# Calculate additional metrics
aicc_values <- c(ets_AAN$aicc, ets_MAN$aicc, ets_MMN$aicc, ets_ANN$aicc)
ljung_box_pvalues <- c(
  Box.test(residuals(ets_AAN), lag = 10, type = "Ljung-Box")$p.value,
  Box.test(residuals(ets_MAN), lag = 10, type = "Ljung-Box")$p.value,
  Box.test(residuals(ets_MMN), lag = 10, type = "Ljung-Box")$p.value,
  Box.test(residuals(ets_ANN), lag = 10, type = "Ljung-Box")$p.value
)

# Create the data frame with all metrics
accuracy_table <- data.frame(
  Model = c("ETS(A, A, N)", "ETS(M, A, N)", "ETS(M, M, N)", "ETS(A, N, N)"),
  MAPE = c(accuracy(Forecast_ets_AAN, dataTest)["Test set", "MAPE"],
           accuracy(Forecast_ets_MAN, dataTest)["Test set", "MAPE"],
           accuracy(Forecast_ets_MMN, dataTest)["Test set", "MAPE"],
           accuracy(Forecast_ets_ANN, dataTest)["Test set", "MAPE"]),
  RMSE = c(accuracy(Forecast_ets_AAN, dataTest)["Test set", "RMSE"],
           accuracy(Forecast_ets_MAN, dataTest)["Test set", "RMSE"],
           accuracy(Forecast_ets_MMN, dataTest)["Test set", "RMSE"],
           accuracy(Forecast_ets_ANN, dataTest)["Test set", "RMSE"]),
  AICc = round(aicc_values, 2),
  Ljung_Box_pvalue = round(ljung_box_pvalues, 4),
  Residuals = c("No Autocorrelation", "No Autocorrelation", "No Autocorrelation", "No Autocorrelation")
)

# Print the accuracy table
print(accuracy_table)

#autoplot for best ets model
autoplot(Forecast_ets_ANN)


# Model part 3
####AVERAGING ALL MODELS

# Combine the forecasts from all models
all_forecasts <- list(Forecasted_111, Forecast_ets_ANN)

# Calculate the average forecast
average_forecast <- Reduce("+", lapply(all_forecasts, function(forecast) forecast$mean)) / length(all_forecasts)

# Plot the average forecast with one of the forecasts for context
plot(Forecasted_111, main = "Average Forecast")
lines(average_forecast, col = "red")

# Create a time series object for the average forecast (if needed for accuracy calculation)
average_forecast_ts <- ts(average_forecast, start = start(Forecasted_111$mean), frequency = frequency(Forecasted_111$mean))

# Print and calculate the accuracy of the average forecast
print(accuracy(average_forecast_ts, dataTest))

#Autoplot for the chosen model (arima 111)
autoplot(Forecasted_111)



