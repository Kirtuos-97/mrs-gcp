#Importing required packages
from pyspark.sql.types import StructType

#---------------------------------------
# CLoud storage path
#---------------------------------------
bucket="gs://snehal-movie-databucket"

filePaths={
    "movie-data":"movie-metadata/movie_metadata_small_sample.csv",
    "rating-data":""
}

#---------------------------------------
# Required Structures
#---------------------------------------
movie_structures={

    "belongs_to_collection": StructType.fromJson(
        {
            "type":"struct",
            "fields":[
                {
                    "name":"id",
                    "type":"integer",
                    "nullable":True,
                    "metadata":{}
                },
                {
                    "name":"name",
                    "type":"string",
                    "nullable":True,
                    "metadata":{}
                }
            ]
        }
    )

}