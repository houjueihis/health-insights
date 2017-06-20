library(datarobot)
library(httr)

# Get API_Token, this needs to be done once only
token = POST('http://10.246.66.22/api/v1/api_token', authenticate(username, password))
content(token)
rawtoken = GET('http://10.246.66.22/api/v1/api_token', authenticate(username, password))
api_token = unname(unlist(content(rawtoken)))
content(rawtoken)

# Input credentials
username = 'hou.jue@ihis.com.sg'
password = 'password'
apiToken = api_token
inputFile = 'volume_train_feat2_fu_sa.csv'
#inputFile = "~/Downloads/data/kdd/train.csv"
server = "http://10.246.66.22"
numWorkers = 2
target = "volume"
userMetric = 'MAPE'

# connect to the DataRobot server
ConnectToDataRobot(endpoint = paste0(server, "/api/v2"), token = apiToken)

# Select Features in Dataset to Use
feat <- c('volume','hour','pressure','sea_pressure','wind_direction','wind_speed','temperature','rel_humidity','precipitation'
          ,'volume.1','volume.2','volume.3','volume.4','volume.5','volume.6','volume.7','volume_prev_same_weekday','cum_mean_pre_weekday'
          ,'lastweek_average')
featurelist <- CreateFeaturelist(project = project$projectId, listName = 'feat1',featureNames = feat)

project = SetupProject(inputFile, projectName = "volume")
SetTarget(project, target, featurelistId = featurelist$featurelistID)

UpdateProject(project$projectId, workerCount = numWorkers)
WaitForAutopilot(project, verbosity = 1, timeout = 999999)

allModels = GetAllModels(project)
# get the predictions
validationMetrics = unlist(lapply(allModels, function(x) return((x$metrics[[userMetric]])$crossValidation)))

# get the best model
model = allModels[[which.min(validationMetrics)]]

#testFile = "~/Downloads/data/kdd/test.csv"
testDataId = UploadPredictionDataset(project, testFile)

predictJobId = RequestPredictionsForDataset(project, model$modelId, testDataId$id)
predictions <- GetPredictions(project, predictJobId)

