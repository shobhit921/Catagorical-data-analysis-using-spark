####################################################################################
                        ## Helper Functions ##
####################################################################################

#Fuction to load packages

pack = c("caret","plyr","foreach","e1071","sparklyr","plotROC","pROC",
         "kernlab","doParallel","compiler", "dplyr","ggplot2",
         "stringr", "tidyr", "tibble", "corrplot", "klaR")

lapply(pack, function(x) if (!(x %in% installed.packages())) {
  install.packages(x)
})

lapply(pack, library, character.only = TRUE)

# Evaluation metric
rmse = function(actual, pred) {
  error = sqrt(mean((actual - pred)^2))
  return(error)
}

user <- function(actual, pred) {
  rsse = sqrt(sum((actual - pred)^2))
  return(rsse)
}

