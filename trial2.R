install.packages("sparklyr")
install.packages("caret")
library(sparklyr)
library(caret)

setwd("")
df <- read.csv("train.csv",header = T,sep = "|")
items <- read.csv("items.csv",header = T,sep="|")
train <- merge(df,items,by="pid")
train[,c("click","basket","revenue")] <- NULL
id <- createDataPartition(train$order,p=0.8,list=F)
train_80 <- train[id,]
test <- train[-id,]
pred <- as.data.frame(test$order)
test$order <- NULL

sc <- spark_connect(master = "local",spark_home = Sys.getenv("SPARK_HOME_VERSION"))

train.spark <- copy_to(sc,train_80)
test.spark <- copy_to(sc,test)
lr.model <- ml_logistic_regression(train.spark,order~.)
predictions <- sdf_predict(lr.model,test.spark$order)
