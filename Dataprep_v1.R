####################################################################################
                            ## Data Preparation ##
####################################################################################

source("helper.R")

setwd("") #Add path to directory with the file 

# Loading datasets
df <- read.csv("train.csv",header = T,sep = "|")
items <- read.csv("items.csv",header = T,sep="|")

# Merging dataset with Customer behaviour and product details
train <- merge(df,items,by="pid")

########### Data Exploration #################

ggplot(train, aes(x=train$price, y=train$price-train$competitorPrice)) + 
  geom_point(aes(col=train$order, size=train$revenue)) + 
  labs(y="Price - Competitor price", 
       x="Price ", 
       title="Effect of difference in prices on revenue")

############ Cleaning dataset ###############

# Removing unwanted variables
train[,c("lineID","click","basket","revenue")] <- NULL

# Removing X from the content column
train$content<-as.numeric(train$content)
containx<-train %>% filter(str_detect(content, 'X')) #filter for X
xsep<-separate(containx,col=content,into=c("new","ln"), sep="X",extra="drop") #separate the values 
combined<-as.numeric(xsep$new)*as.numeric(xsep$ln) #multiply the values prior separated by X 
withx<-grepl(pattern="X", x=train$content) # select only observations with X and overwrite with new value
train$content[withx] <-combined

# Capitalize pharm form column to get same factor levels
train$pharmForm <- as.character(train$pharmForm)
train$pharmForm <- sapply(train$pharmForm, toupper)

# Change factor variable 
factorvars <- c("adFlag","pharmForm","availability","order","genericProduct","salesIndex","category")
train[,factorvars] <- lapply(train[,factorvars],factor)

# add new factor level (missing)
levels(train$pharmForm) <- c(levels(train$pharmForm), "(missing)")
levels(train$category) <- c(levels(train$category), "(missing)")
levels(train$campaignIndex) <- c(levels(train$campaignIndex), "(missing)")

# impute (missing) for no value ('')
train$pharmForm[train$pharmForm == ''] <- '(missing)'
train$campaignIndex[train$campaignIndex == ''] <- '(missing)'

# impute (missing) for NA
train$pharmForm[is.na(train$pharmForm)] <- '(missing)'
train$category[is.na(train$category)] <- '(missing)'

# remove unused factor levels
train$pharmForm <- droplevels(train$pharmForm)
train$campaignIndex <- droplevels(train$campaignIndex)

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

