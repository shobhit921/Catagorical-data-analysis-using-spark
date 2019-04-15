####################################################################################
                  #Logistic regression using Spark#
####################################################################################
starttime <- Sys.time()

#set working directory
setwd("C:/Users/jayalakshmi/Desktop/MEMS/APA/DMC_2017_task")

# Get auxillary functions 
#source("helper.R")

# Load datasets
df <- read.csv("train.csv",header = T,sep = "|")
items <- read.csv("items.csv",header = T,sep="|")

# Merging dataset with Customer behaviour and product details
train <- merge(df,items,by="pid")
str(train)


########### Data Preparation #################

#Replacing NAs in competitor price
idxNA <- which(is.na(train$competitorPrice)) # create index for NAs

# create linear regression (competitorPrice ~ price)  to predict NAs
fit <- lm(competitorPrice ~ price, data = train)
pred <- predict(fit, newdata = train)
pred <- array(pred)

# impute the NAs with with regression results
train$competitorPrice[idxNA] <- pred[idxNA]
train$competitorPrice <- round(x=train$competitorPrice, digits=2)

# create dummy column to indicate whether the value for copetitorPrice was missing
train$cPriceNA <- 0
train$cPriceNA[idxNA] <- 1

# Add features
train$pricediff <- train$price - train$competitorPrice
train$clickpred <- ifelse(train$pricediff>1,1,0)

# Creating the test and the training set
set.seed(123)
id <- createDataPartition(train$order,p=0.2,list=F)
train_80 <- train[id,]
test <- train[2700000:2756003,]
table(train_80$order)/nrow(train_80)
table(test$order)/nrow(test)

# Storing order varibale for comparing predictions
pred <- setNames(cbind.data.frame(test$lineID,test$order,test$price),c("ID","actual","price"))
pred$benchmark_0 <- 0
pred$benchmark_1 <- 1

# Removing unwanted variables
varlist <- c("lineID","group","content","unit","pharmForm","campaignIndex","category","click","basket","revenue")
train_80[,varlist] <- NULL
test[,varlist] <- NULL
test$order <- NULL
rm(df,id,items,fit,idxNA,pack)

########### Logistic Regression using Spark #################

#Connect to spark 
sc <- spark_connect(master = "local",spark_home = Sys.getenv("SPARK_HOME_VERSION"))

#Load train and test data sets into spark 
train.spark <- copy_to(sc,train_80,overwrite = T)
test.spark <- copy_to(sc,test,overwrite = T)

#Logistic regression using Sparkml 
lr.model.spark.v2 <- ml_logistic_regression(train.spark,order~.)

#Predicting on the test set 
predictions.v1 <- sdf_predict(lr.model.v2,test.spark) 

# Get predicted probabilities for comparison
prob.spark <- dplyr::collect(predictions.v1)
summary(prob.spark$probability_0) # Looking at the distributions
pred$lr.spark.prob.v1 <- prob.spark$probability_0

################## Model selection ###################

# Find optimal cutoff using ROC curve

basicplot <- ggplot(pred, aes(d = actual, m = lr.spark.prob.v1)) + 
    geom_roc(labelround = 2, 
             cutoffs.at = NULL, 
             cutoff.labels = NULL,
             show.legend = NA, 
             inherit.aes = TRUE,
             color = "#FF99FF") +
    style_roc(xlab = "False Positive Rate (1 - Specificity)", ylab = "True Positive Rate (Sensitivity)")

# output interactive plot
plot_interactive_roc(basicplot)

# Check auc
auc(roc(pred$actual,pred$lr.spark.prob.v1))

################## Model Evaluation ###################

# Converted probabilities into predictions 
pred$lr.spark.predict.v1 <- ifelse(pred$lr.spark.prob.v1>=0.74 ,0,1)

# Compare distributions
table(pred$lr.spark.predict.v1)/nrow(pred)
table(pred$actual)/nrow(pred)

# Confusion Matrix
cf_full <- confusionMatrix(pred$actual,pred$lr.spark.predict.v1)

# checking the error values 
user(pred$actual,pred$benchmark_0)/nrow(pred)
user(pred$actual,pred$benchmark_1)/nrow(pred)
user(pred$actual,pred$lr.spark.predict.v1)/nrow(pred)


############ Logistic regression using glm package ################

lm.model.r.v1 <- glm(order~.,data = train_80,family = binomial(link = "logit"))
pred$lr.r.prob.v1 <- predict(lm.model.r,test,type ="response")
summary(pred$lr.r.prob.v1)
pred$lr.r.predict.v1 <- ifelse(pred$lr.r.prob.v1>=0.25,1,0)
table(pred$lr.r.predict.v1 )/nrow(pred)
auc(roc(pred$actual,pred$lr.r.prob.v1))
cf_v2 <- confusionMatrix(pred$actual,pred$lr.r.predict.v1)
user(pred$actual,pred$lr.r.predict.v1)

######################################################################

closeAllConnections()
saveRDS(pred,"predictictions_v1.RDS")
endtime <- Sys.time()
duration <- endtime - starttime
