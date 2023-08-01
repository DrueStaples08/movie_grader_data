import pandas as pd
import sys
sys.path.append('../downloaded_datasets/')
sys.path.append('../lib/')

from big_query_client import gcs_client, gcs_get_bucket, gcs_get_bucket_blob, gcs_upload_extracted_column_data, main_bq_create_table, gcs_insert_to_bigquery, gcs_upload_data, gcs_load_csv_file


# def send_dataframe_gcs():
#     # upload to gcs 
#     orig_df = pd.read_csv('../downloaded_datasets/imdb_top_1000.csv')

#     # upload to gcs
#     movie_df = orig_df[['Series_Title', 'Released_Year', 'IMDB_Rating', 'No_of_Votes']]
#     movie_df = orig_df[['Series_Title', 'Released_Year']]
#     movie_df['Grade'] = pd.Series(dtype=float)
#     movie_df['No_of_Votes'] = pd.Series(dtype=int)
#     return movie_df



def gcs_imbd_orig(project_id: str, foldername: str, filename: str, destination_blob_name: str='third_party_movie_data', orig: bool=True):
    bucket = gcs_get_bucket(project_id)
    # print(bucket)
    blob = gcs_get_bucket_blob(bucket, destination_blob_name, foldername, filename)
    # print(blob)
    # movie_df = send_dataframe_gcs_x()
    # print(movie_df)
    if orig:
        return gcs_upload_data(project_id, blob, destination_blob_name, foldername, filename)
    else:
        return gcs_upload_extracted_column_data(project_id, blob, destination_blob_name, foldername, filename)


if __name__ == '__main__':

    # Upload dataframe to GCS in 'third_party_movie_data/downloaded_datasets' and 'third_party_movie_data/extracted_datasets'
    project_id = "movie-grader-394211"
    destination_blob_name = 'third_party_movie_data'
    foldername1 = 'downloaded_datasets'
    filename1= 'imdb_top_1000.csv'
    foldername2 = 'extracted_datasets'
    filename2 = 'imdb_top_1000_title_year.csv'
    print(gcs_imbd_orig(project_id, foldername1, filename1))
    print(gcs_imbd_orig(project_id, foldername2, filename2, orig=False))


    # Generate 'movie_info' Table in movie_grader_dataset
#     project_id = "movie-grader-394211"
#     dataset_id = "movie_grader_dataset"
#     table_id = "movie_info"
#     schema_field = [
#     bigquery.SchemaField('movie_title', 'STRING'),
#     bigquery.SchemaField('release_year', 'STRING'),
#     bigquery.SchemaField('movie_votes', 'INTEGER'),
#     bigquery.SchemaField('movie_grade', 'FLOAT'),
#     bigquery.SchemaField('movie_letter_grade', 'STRING')
#     ]
#     print(main_bq_create_table(project_id, dataset_id, table_id, schema_field))
   




    # # UPDATE movie_info table with imbd dataframe that was loaded from GCS
    # project_id = "movie-grader-394211"
    # bucket_name = "cinegrade_app"
    # filename = "imdb_top_1000_title_year.csv"
    # foldername = 'extracted_datasets'
    # destination_blob_name = 'third_party_movie_data'
    # # Load the CSV data into a Pandas DataFrame
    # df = gcs_load_csv_file(project_id, bucket_name, destination_blob_name, foldername, filename)
    # print(df.keys())
    # # Insert DataFrame data into BigQuery table
    # dataset_id = "movie_grader_dataset"
    # table_id = "movie_info"
    # # gcs_insert_to_bigquery(project_id, dataset_id, table_id, df)

