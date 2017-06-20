#Consolidate all codes for creating features
install.packages("DataCombine",repos = "http://cran.uk.r-project.org")
library(DataCombine)
library(lubridate)

#read data and data processing
setwd('C:/Users/AA_2/Dropbox/KDD17/Raw Data/Phase2')
train_time <- read.csv("training2_20min_avg_travel_time.csv")

Sys.timezone()
#get date time
get_date_time <- function(x)
{
  time_splited <- t(data.frame(sapply(x, strsplit,split = ',',fixed = T)))
  start_time <- as.POSIXct(substring(time_splited[,1],2),format="%Y-%m-%d %H:%M:%S")
  end_time <- as.POSIXct(substring(time_splited[,2],1,19),format="%Y-%m-%d %H:%M:%S")
  date <- as.Date(start_time,format="%d/%m/%Y",tz="Asia/Kuala_Lumpur")
  hour <- as.integer(hour(start_time))
  interval <- minute(start_time)/20+1
  df <- data.frame(date,start_time,end_time,hour,interval)
  colnames(df) <- c("date","start_time","end_time","hour","interval")
  return(df)
}

train_time_1 <- cbind(train_time,get_date_time(as.character(train_time$time_window)))

#data processing
train2_temp <- train_time_1[train_time_1$hour %in% c(8,9,17,18),]
train_temp_2_A <- subset(train2_temp, tollgate_id==2 & intersection_id == 'A')
train_temp_1_C <- subset(train2_temp, tollgate_id==1 & intersection_id == 'C')
train_temp_1_B <- subset(train2_temp, tollgate_id==1 & intersection_id == 'B')
train_temp_3_A <- subset(train2_temp, tollgate_id==3 & intersection_id == 'A')
train_temp_3_B <- subset(train2_temp, tollgate_id==3 & intersection_id == 'B')
train_temp_3_C <- subset(train2_temp, tollgate_id==3 & intersection_id == 'C')

intersectionv <- factor(c('A','B','C'))

tollgatev <- c(1,2,3)
intervalv <- c(1,2,3)
hourv <- seq(0,23, by=1)
min_date <- min(train_time_1$date)
max_date <- max(train_time_1$date)
datev_time <- seq(min_date,max_date,by='day')
datev_volume <- seq(min_date,max_date,by='day')

train_time_na <- expand.grid(intervalv,hourv,datev_time,intersectionv,tollgatev)
head(train_time_na)

train_time_na <- subset(train_time_na, (Var4 == 'A' & Var5 == 3)|(Var4== 'A' & Var5 == 2)|
                          (Var4 == 'B' & Var5 == 3)|(Var4 == 'B' & Var5 == 1)|
                          (Var4 == 'C')&(Var5 == 1)|(Var4 == 'C')&(Var5 == 3))

colnames(train_time_na) <- c("interval","hour","date","intersection_id","tollgate_id")
head(train_time_na)
train_time_1$interval <- minute(train_time_1$start_time)/20 +1
head(train_time_1)##Colume names match, ok to merge
train_time_all <- merge(train_time_na,train_time_1, by=c("intersection_id","tollgate_id","date","hour","interval"),all.x=T)


#Create lags
train_time_2_A <- subset(train_time_all, tollgate_id == 2 & intersection_id == 'A')
train_time_1_C <- subset(train_time_all, tollgate_id == 1 & intersection_id == 'C')
train_time_1_B <- subset(train_time_all, tollgate_id == 1 & intersection_id == 'B')
train_time_3_A <- subset(train_time_all, tollgate_id == 3 & intersection_id == 'A')
train_time_3_B <- subset(train_time_all, tollgate_id == 3 & intersection_id == 'B')
train_time_3_C <- subset(train_time_all, tollgate_id == 3 & intersection_id == 'C')

train_time_2_A_lag7 <- slide(train_time_2_A, Var = "avg_travel_time", slideBy = -seq(1,7,1))
train_time_1_B_lag7 <- slide(train_time_1_B, Var = "avg_travel_time", slideBy = -seq(1,7,1))
train_time_1_C_lag7 <- slide(train_time_1_C, Var = "avg_travel_time", slideBy = -seq(1,7,1))
train_time_3_A_lag7 <- slide(train_time_3_A, Var = "avg_travel_time", slideBy = -seq(1,7,1))
train_time_3_B_lag7 <- slide(train_time_3_B, Var = "avg_travel_time", slideBy = -seq(1,7,1))
train_time_3_C_lag7 <- slide(train_time_3_C, Var = "avg_travel_time", slideBy = -seq(1,7,1))

train2_temp_2_A_lag <- merge(train_temp_2_A, train_time_2_A_lag7[,c("date","hour","interval","avg_travel_time","avg_travel_time-1","avg_travel_time-2","avg_travel_time-3","avg_travel_time-4","avg_travel_time-5","avg_travel_time-6","avg_travel_time-7")], by = c("date","hour","interval"), all.x = T)
train2_temp_1_B_lag <- merge(train_temp_1_B, train_time_1_B_lag7[,c("date","hour","interval","avg_travel_time","avg_travel_time-1","avg_travel_time-2","avg_travel_time-3","avg_travel_time-4","avg_travel_time-5","avg_travel_time-6","avg_travel_time-7")], by = c("date","hour","interval"), all.x = T)
train2_temp_1_C_lag <- merge(train_temp_1_C, train_time_1_C_lag7[,c("date","hour","interval","avg_travel_time","avg_travel_time-1","avg_travel_time-2","avg_travel_time-3","avg_travel_time-4","avg_travel_time-5","avg_travel_time-6","avg_travel_time-7")], by = c("date","hour","interval"), all.x = T)
train2_temp_3_A_lag <- merge(train_temp_3_A, train_time_3_A_lag7[,c("date","hour","interval","avg_travel_time","avg_travel_time-1","avg_travel_time-2","avg_travel_time-3","avg_travel_time-4","avg_travel_time-5","avg_travel_time-6","avg_travel_time-7")], by = c("date","hour","interval"), all.x = T)
train2_temp_3_B_lag <- merge(train_temp_3_B, train_time_3_B_lag7[,c("date","hour","interval","avg_travel_time","avg_travel_time-1","avg_travel_time-2","avg_travel_time-3","avg_travel_time-4","avg_travel_time-5","avg_travel_time-6","avg_travel_time-7")], by = c("date","hour","interval"), all.x = T)
train2_temp_3_C_lag <- merge(train_temp_3_C, train_time_3_C_lag7[,c("date","hour","interval","avg_travel_time","avg_travel_time-1","avg_travel_time-2","avg_travel_time-3","avg_travel_time-4","avg_travel_time-5","avg_travel_time-6","avg_travel_time-7")], by = c("date","hour","interval"), all.x = T)

write.csv(train2_temp_2_A_lag, file = 'train2_time_2A.csv')
write.csv(train2_temp_1_B_lag, file = 'train2_time_1B.csv')
write.csv(train2_temp_1_C_lag, file = 'train2_time_1C.csv')
write.csv(train2_temp_3_A_lag, file = 'train2_time_3A.csv')
write.csv(train2_temp_3_B_lag, file = 'train2_time_3B.csv')
write.csv(train2_temp_3_C_lag, file = 'train2_time_3C.csv')


##### create lag for time ######
test_time <- read.csv("test_features/test_time.csv")

Sys.timezone()
#get date time
get_date_time <- function(x)
{
  time_splited <- t(data.frame(sapply(x, strsplit,split = ',',fixed = T)))
  start_time <- as.POSIXct(substring(time_splited[,1],2),format="%Y-%m-%d %H:%M:%S")
  end_time <- as.POSIXct(substring(time_splited[,2],1,19),format="%Y-%m-%d %H:%M:%S")
  date <- as.Date(start_time,format="%d/%m/%Y",tz="Asia/Kuala_Lumpur")
  hour <- as.integer(hour(start_time))
  interval <- minute(start_time)/20+1
  df <- data.frame(date,start_time,end_time,hour,interval)
  colnames(df) <- c("date","start_time","end_time","hour","interval")
  return(df)
}

train_time_1 <- cbind(test_time,get_date_time(as.character(test_time$time_window)))

#data processing
train2_temp <- train_time_1[train_time_1$hour %in% c(8,9,17,18),]
train_temp_2_A <- subset(train2_temp, tollgate_id==2 & intersection_id == 'A')
train_temp_1_C <- subset(train2_temp, tollgate_id==1 & intersection_id == 'C')
train_temp_1_B <- subset(train2_temp, tollgate_id==1 & intersection_id == 'B')
train_temp_3_A <- subset(train2_temp, tollgate_id==3 & intersection_id == 'A')
train_temp_3_B <- subset(train2_temp, tollgate_id==3 & intersection_id == 'B')
train_temp_3_C <- subset(train2_temp, tollgate_id==3 & intersection_id == 'C')

intersectionv <- factor(c('A','B','C'))

tollgatev <- c(1,2,3)
intervalv <- c(1,2,3)
hourv <- seq(0,23, by=1)
min_date <- min(train_time_1$date)
max_date <- max(train_time_1$date)
datev_time <- seq(min_date,max_date,by='day')

train_time_na <- expand.grid(intervalv,hourv,datev_time,intersectionv,tollgatev)
head(train_time_na)

train_time_na <- subset(train_time_na, (Var4 == 'A' & Var5 == 3)|(Var4== 'A' & Var5 == 2)|
                          (Var4 == 'B' & Var5 == 3)|(Var4 == 'B' & Var5 == 1)|
                          (Var4 == 'C')&(Var5 == 1)|(Var4 == 'C')&(Var5 == 3))

colnames(train_time_na) <- c("interval","hour","date","intersection_id","tollgate_id")
head(train_time_na)
train_time_1$interval <- minute(train_time_1$start_time)/20 +1
head(train_time_1)##Colume names match, ok to merge
train_time_all <- merge(train_time_na,train_time_1, by=c("intersection_id","tollgate_id","date","hour","interval"),all.x=T)


#Create lags
train_time_2_A <- subset(train_time_all, tollgate_id == 2 & intersection_id == 'A')
train_time_1_C <- subset(train_time_all, tollgate_id == 1 & intersection_id == 'C')
train_time_1_B <- subset(train_time_all, tollgate_id == 1 & intersection_id == 'B')
train_time_3_A <- subset(train_time_all, tollgate_id == 3 & intersection_id == 'A')
train_time_3_B <- subset(train_time_all, tollgate_id == 3 & intersection_id == 'B')
train_time_3_C <- subset(train_time_all, tollgate_id == 3 & intersection_id == 'C')

train_time_2_A_lag7 <- slide(train_time_2_A, Var = "avg_travel_time", slideBy = -seq(1,7,1))
train_time_1_B_lag7 <- slide(train_time_1_B, Var = "avg_travel_time", slideBy = -seq(1,7,1))
train_time_1_C_lag7 <- slide(train_time_1_C, Var = "avg_travel_time", slideBy = -seq(1,7,1))
train_time_3_A_lag7 <- slide(train_time_3_A, Var = "avg_travel_time", slideBy = -seq(1,7,1))
train_time_3_B_lag7 <- slide(train_time_3_B, Var = "avg_travel_time", slideBy = -seq(1,7,1))
train_time_3_C_lag7 <- slide(train_time_3_C, Var = "avg_travel_time", slideBy = -seq(1,7,1))

train2_temp_2_A_lag <- merge(train_temp_2_A, train_time_2_A_lag7[,c("date","hour","interval","avg_travel_time-1","avg_travel_time-2","avg_travel_time-3","avg_travel_time-4","avg_travel_time-5","avg_travel_time-6","avg_travel_time-7")], by = c("date","hour","interval"), all.x = T)
train2_temp_1_B_lag <- merge(train_temp_1_B, train_time_1_B_lag7[,c("date","hour","interval","avg_travel_time-1","avg_travel_time-2","avg_travel_time-3","avg_travel_time-4","avg_travel_time-5","avg_travel_time-6","avg_travel_time-7")], by = c("date","hour","interval"), all.x = T)
train2_temp_1_C_lag <- merge(train_temp_1_C, train_time_1_C_lag7[,c("date","hour","interval","avg_travel_time-1","avg_travel_time-2","avg_travel_time-3","avg_travel_time-4","avg_travel_time-5","avg_travel_time-6","avg_travel_time-7")], by = c("date","hour","interval"), all.x = T)
train2_temp_3_A_lag <- merge(train_temp_3_A, train_time_3_A_lag7[,c("date","hour","interval","avg_travel_time-1","avg_travel_time-2","avg_travel_time-3","avg_travel_time-4","avg_travel_time-5","avg_travel_time-6","avg_travel_time-7")], by = c("date","hour","interval"), all.x = T)
train2_temp_3_B_lag <- merge(train_temp_3_B, train_time_3_B_lag7[,c("date","hour","interval","avg_travel_time-1","avg_travel_time-2","avg_travel_time-3","avg_travel_time-4","avg_travel_time-5","avg_travel_time-6","avg_travel_time-7")], by = c("date","hour","interval"), all.x = T)
train2_temp_3_C_lag <- merge(train_temp_3_C, train_time_3_C_lag7[,c("date","hour","interval","avg_travel_time-1","avg_travel_time-2","avg_travel_time-3","avg_travel_time-4","avg_travel_time-5","avg_travel_time-6","avg_travel_time-7")], by = c("date","hour","interval"), all.x = T)

write.csv(train2_temp_2_A_lag, file = 'test_features/test_time_2A.csv')
write.csv(train2_temp_1_B_lag, file = 'test_features/test_time_1B.csv')
write.csv(train2_temp_1_C_lag, file = 'test_features/test_time_1C.csv')
write.csv(train2_temp_3_A_lag, file = 'test_features/test_time_3A.csv')
write.csv(train2_temp_3_B_lag, file = 'test_features/test_time_3B.csv')
write.csv(train2_temp_3_C_lag, file = 'test_features/test_time_3C.csv')
