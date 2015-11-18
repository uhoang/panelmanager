#' Store raw demographic and political variables for all unique emails in the panel
#' @aliases storeVar store storeV storeVariable
#'
#' @param var a vector of variabels to store
#' @param Data a data frame of the input dataset to extract raw variabels from
#' @param toFile a vector of paths of the output datasets 
#' @param fromFile a vector of paths of the input datasets 
#' @param idvar a vector of id variables for the input and output datasets
#' @param source a string of source of the input dataset
#' @param reportFile a string path of a report file 
#' @examples 
#' 
#' library(plyr)
#' setwd("~/Dropbox (Vox Pop Labs)/Data/Panel/CanadaPanel/RawData")
#' parents.dir <- list.files()
#' fromFilePaths <- unlist(apply(as.matrix(parents.dir), 1, function(x) paste0(x, "/", list.files(x))))
#' Data <- list()
#' 
#' for (i in 1:length(fromFilePaths)) {
#' Data[[i]] <- read.csv(fromFilePaths[i], stringsAsFactors = FALSE)
#' Data[[i]]$source <- gsub("\\/", "-", gsub("EmailData.csv", "", fromFilePaths[i]))
#' if ( grepl("Toronto2014", fromFilePaths[i]) ) { Data[[i]]$province <- "Ontario" 
#' } else if ( grepl("Alberta2015", fromFilePaths[i]) ) {
#' 		Data[[i]]$province <- "Alberta" 
#' }
#' 		names(Data)[names(Data) %in% "province"] <- "provinceRaw"
#' }
#' varList <- c("genderRaw", "birthYearRaw", "religionRaw", "incomeRaw", "educationRaw", "selfPlacementRaw", "polInterestRaw", "voteChoiceRaw")
#' varList <- c("occupationRaw")
#' varList <- c("province", "postal_code", "riding")
#' toFilePaths <- paste0("../Variables/", varList, ".csv")
#' l_ply(Data, function(D) { storeVar(varList, D, toFilePaths, idvar = c("date", "uniqueID"), source = unique(D$source), reportFile = "../Report.csv") })
#'
#' 
storeVar <- function(var, Data, toFile, fromFile = NA, idvar = NA, source = NA, reportFile = NA, ...) {

	if ( missing(Data) & !is.na(fromFile)) {
  		if ( !file.exists(fromFile) ) stop(paste0("Could not find ", basename(fromFile), " in ", dirname(fromFile), "."))	
  		Data <- read.csv(fromFile, stringsAsFactors = FALSE)
  	} 
  	if (! is.data.frame(Data)) stop("The data has a wrong format.")
	Data$source <- source 
	# Data$email[is.na(Data$email)] <- Data$Email[is.na(Data$email)]
	# Data$email <- gsub("^\\s+|\\s$|\\ ", "", Data$email)
	# Data$email <- ifelse(grepl("^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$", Data$email) == TRUE, Data$email, NA)
	# Data <- subset(Data, !is.na(Data$email))
	# if (any(!is.na(source))) { 
	# 	cat("----------------------------------------\n")
	# 	cat("Start storing variables:", paste(var, collapse = ", "), "in", source, "\n")
	# } else {
	# 	cat("----------------------------------------\n")
	# 	cat("Start storing variables:", paste(var, collapse = ", "), "\n")
	# }
	Data$source <- source 
	args <- match.call()
	if (any("emailVars" %in% names(args))) {
		Data <- cleanEmail(Data, emailVars = emailVars)
	} else {
		Data <- cleanEmail(Data)
	}
	if (any(!is.na(source))) { 
		cat("----------------------------------------\n")
		cat("Start storing variables:", paste(var, collapse = ", "), "in", source, "\n")
	} else {
		cat("----------------------------------------\n")
		cat("Start storing variables:", paste(var, collapse = ", "), "\n")
	}

  	# if ( any("toData" %in% args) ) stop("You need to specify file name for outputs.")
	# if ( length(var) != length(toFile) ) stop("The number of outputs' file names must be equal to the number of variables.")


  	if ( missing(toFile) ) stop("You need to specify file name for outputs.")
	if ( length(var) != length(toFile) ) stop("The number of file names must be equal to the number of variables.")

	# Update the report
	if ( !is.na(reportFile) ) {
		if (file.exists(reportFile)) {
			Report <- read.csv(reportFile, stringsAsFactors = FALSE)
			newEntry <- data.frame( var = var, toFile = toFile, fromFile = source, date = date())
			Report <- plyr::rbind.fill(Report, newEntry) 
		} else {
			Report <- data.frame( var = var, toFile = toFile, fromFile = source, date = date())
		}
		write.csv(Report, reportFile, row.names = FALSE)
	}

	if ( any( ! var %in% names(Data)) ) warnings(paste0("These variables: ",var[!var %in% names(Data)], ", are not included in the dataset."))
	updateList <- data.frame(var = var[var %in% names(Data)], toFile = toFile[var %in% names(Data)], stringsAsFactors = FALSE)	
	
	D <- updateList
	if ( nrow(updateList) > 0 ){
		plyr::d_ply(updateList, .(var, toFile), function(D) {
		
		NewToData <- Data[ , as.character(na.omit(unique(c("email", D$var, idvar, "source"))))] 	
		
		if ( !file.exists(dirname(D$toFile)) ) stop(paste0("Could not find ", dirname(D$toFile), "."))
		if ( file.exists(D$toFile) ) {
			ToData <- read.csv(D$toFile, stringsAsFactors = FALSE)
			ToData <- plyr::rbind.fill(ToData, NewToData)
			cat(paste0("The ", basename(D$toFile)," has been updated in ", dirname(D$toFile), ".\n"))
		} else {			
			ToData <- NewToData
			cat(paste0("The ", basename(D$toFile)," has been created in ", dirname(D$toFile), ".\n"))
		}		
		ToData <- ToData[!duplicated(ToData) & !is.na(ToData[ , D$var]), ]
		if ( all(is.na(ToData$source)) ) ToData$source <- NULL
		if ("date" %in% names(ToData)) ToData <- plyr::ddply(ToData, .(email), function(D) D[order(D$date, decreasing = TRUE), ])		
		write.csv(ToData, D$toFile, row.names = FALSE)}
		)
	}
}	


cleanEmail <- function(Data, emailVars = c("email", "Email")){
	if (length(emailVars) == 1) {
		Data$email <- Data[ , emailVars]
	} else if (length(emailVars) == 2) {
		Data$email <- Data[ , emailVars[1]]
		Data$email[is.na(Data$email)] <- Data[ , emailVars[2]][is.na(Data$email)]
	} else {
		Data$email <- apply(Data[ , emailVars], 1, function(x) na.omit(x)[1])
	}
	Data$email <- gsub("^\\s+|\\s$|\\ ", "", Data$email)
	Data$email <- tolower(Data$email)
	Data$email <- ifelse(grepl("^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$", Data$email) == TRUE, Data$email, NA)
	Data <- subset(Data, !is.na(Data$email))
	return(Data)
}



#' Update the master file
#' @aliases updateM updateMaster updatemaster master Master
#' 
#' @param masterFile a data frame or a path of the master file
#' @param fromFile a list of data frames or paths of the input datasets 
#' @param toFile a string path of the updated master file
#' @param id a string of variable is used as id variable. By default, id variable is email.
#' @param dateRange a vector of lower and upper bound of dates that are selected from the input file.
#' @param newIdOnly a logical variable to indicate whether or not to update only new IDs. 
#' @param newIdFile a string of the path that contains new IDs.
#' 
#' @examples
#' 
#' masterFile <- read.csv("../Variables/email.csv", stringsAsFactors = FALSE)
#' 
#' fromFileList <- paste0("../Variables/", list.files("../Variables/")[!grepl("email.csv", list.files("../Variables"))])
#' updateMaster(masterFile, as.list(fromFileList), "../Canada master file.csv")
#' masterFile <- masterFile[-sample(1:nrow(masterFile), 200), ]
#' updateMaster(masterFile, fromFileList, "../Canada master file2.csv", newIdOnly = TRUE, newIdFile = "../Variables/email.csv")

updateMaster <- function(masterFile, fromFile, toFile, idvar = "email", dateRange = c(0, Inf), newIdOnly = FALSE, newIdFile) {
	if ( missing(toFile) ) stop("You need to input the output path.")
	if ( missing(masterFile) ) stop("You need to input the master file.")
	if ( missing(fromFile) ) stop("You need to input the files to update.")

	if (!is.data.frame(masterFile)) stop("A master file must be a data frame.")
	
	# Update only emails that doesn't exist in the master file.
	if ( newIdOnly ) {
		temp <- masterFile
		# Check the new id file or its path
		if ( is.data.frame(newIdFile) ) {
			masterFile <- dplyr::anti_join(newIdFile, masterFile, by = id)
		} else if ( is.character(newIdFile) ) {
			if ( file.exists(newIdFile) ) {
				newIdFile <- read.csv(newIdFile, stringsAsFactors = FALSE)
				masterFile <- dplyr::anti_join(newIdFile, masterFile, by = id)
			} else {
				stop(paste0("Could not find ", basename(newIdFile[i]), " in ", dirname(newIdFile[i])))
			}
		} else {
			stop("The new ID file must be a data frame or a path string.")
		}

	}
	# Update the master file by using the input file list of the variables or the file path stores those variables.
	for ( i in 1:length(fromFile) )	{
		if ( is.data.frame(fromFile[[i]]) ) {
			fromFileData <- fromFile[[i]]
		} else if ( is.character(fromFile[[i]])) {
			if (file.exists(fromFile[[i]])) {
				fromFileData <- read.csv(fromFile[[i]], stringsAsFactors = FALSE)
			} else {
				stop(paste0("Could not find ", basename(fromFile[[i]]), " in ", dirname(fromFile[[i]])) )
			}
		} else { 
			stop("The input file must be a data frame or a path string.")
		}
		if ( "date" %in% names(fromFileData) )fromFileData <- subset(fromFileData, date >= dateRange[1] & date <= dateRange[2])
		if ( idvar %in% names(fromFileData)) { fromFileData <- fromFileData[!duplicated(fromFileData[, idvar]), ]
		} else { stop(paste0("There is no ", idvar, " in the input file.")) }
	
		if (ncol(fromFileData) > 2) names(fromFileData)[3:ncol(fromFileData)] <- paste0(names(fromFileData)[2], ".", 
																						names(fromFileData)[3:ncol(fromFileData)])
		cat("Update the latest values of", names(fromFileData)[2], "into the master file.\n") 
		masterFile <- dplyr::left_join(masterFile, fromFileData, by = idvar)
	}
	if ( newIdOnly ) masterFile <- plyr::rbind.fill(temp, masterFile)
	write.csv(masterFile, toFile, row.names = FALSE)
}




