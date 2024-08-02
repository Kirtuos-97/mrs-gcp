#Importing required packages
from pyspark.sql.types import StructType

#---------------------------------------
# CLoud storage path
#---------------------------------------
bucket="gs://snehal-movie-databucket"
temp_bucket="gs://mrs-dataproc-temp-bucket"
temp_bucket_name = "mrs-dataproc-temp-bucket"




filePaths={
    "movie-data":"movie-metadata/movies_metadata.csv",
    "rating-data":"rating-metadata/ratings.csv",
    "credits-data":"credits-metadata/credits.csv",
    "keywords-data":"keywords-metadata/keywords.csv",
    "links-data" : "links-metadata/links.csv"

}

ExtTablePaths={
    "movie_data_raw":"gs://mrs-ods-bucket/src-movie-data-raw/",
    "rating_data_raw":"gs://mrs-ods-bucket/src-rating-raw/",
    "keywords_data_raw":"gs://mrs-ods-bucket/src-keywords-raw/",
    "credits_data_raw" :"gs://mrs-ods-bucket/src-credits-raw",
    "links_data_raw" :"gs://mrs-ods-bucket/src-links-raw"
}

#---------------------------------------
# Big query table names
#---------------------------------------
mrs_project="scenic-genre-422311-g5"

ds_ods_mrs="ods_mrs"

tab_movie_data="src_movie_data"

project_id = "scenic-genre-422311-g5"

movie_data_raw="gs://mrs-ods-bucket/src-movie-data-raw/"


