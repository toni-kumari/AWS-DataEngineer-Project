import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script  for node Artist
Artist_node1747998254227 = glueContext.create_dynamic_frame.from_options(format_options=
    {
        "quoteChar": "\"", 
        "withHeader": True, 
        "separator": ",", 
        "optimizePerformance": False
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://spotify.project.datewithdata/staging/artists.csv"], "recurse": True}, 
    transformation_ctx="Artist_node1747998254227")

# Script  for node track
track_node1747998266817 = glueContext.create_dynamic_frame.from_options(format_options=
    {
        "quoteChar": "\"",
        "withHeader": True, 
        "separator": ",", 
        "optimizePerformance": False
    }, 
    connection_type="s3", 
    format="csv", 
    connection_options={"paths": ["s3://spotify.project.datewithdata/staging/track.csv"], "recurse": True}, 
    transformation_ctx="track_node1747998266817")

# Script  for node album
album_node1747998264595 = glueContext.create_dynamic_frame.from_options(format_options=
    {
        "quoteChar": "\"", 
        "withHeader": True, 
        "separator": ",", 
        "optimizePerformance": False
    }, 
    connection_type="s3", 
    format="csv", 
    connection_options=
    {
        "paths": ["s3://spotify.project.datewithdata/staging/albums.csv"], "recurse": True
    }, 
    transformation_ctx="album_node1747998264595")

# Script  for node Join Album & Artist
JoinAlbumArtist_node1747998614474 = Join.apply(
    frame1=album_node1747998264595, 
    frame2=Artist_node1747998254227, 
    keys1=["artist_id"], keys2=["id"], 
    transformation_ctx="JoinAlbumArtist_node1747998614474"
)

# Script  for node Join with Track
JoinwithTrack_node1747998942235 = Join.apply(
    frame1=track_node1747998266817, 
    frame2=JoinAlbumArtist_node1747998614474, 
    keys1=["track_id"], keys2=["track_id"], 
    transformation_ctx="JoinwithTrack_node1747998942235")

# Script generated for node Drop Fields
DropFields_node1748005558675 = DropFields.apply(
    frame=JoinwithTrack_node1747998942235, 
    paths=["track_id", "id"], 
    transformation_ctx="DropFields_node1748005558675"
)

# Script  for node Destination
EvaluateDataQuality().process_rows(
    frame=DropFields_node1748005558675, 
    ruleset=DEFAULT_DATA_QUALITY_RULESET, 
    publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1747998203628", "enableDataQualityResultsPublishing": True}, 
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
    )
Destination_node1748005696590 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1748005558675, 
    connection_type="s3", format="glueparquet", 
    connection_options={"path": "s3://spotify.project.datewithdata/datawarehouse/", "partitionKeys": []}, 
    format_options={"compression": "snappy"}, transformation_ctx="Destination_node1748005696590")

job.commit()