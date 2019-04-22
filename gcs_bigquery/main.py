from google.cloud import storage, bigquery

def load_data(data, context):
    client = bigquery.Client()
    # 'data' reference is here:
    # https://cloud.google.com/storage/docs/json_api/v1/objects?hl=ja

    ################################
    # TO DO: Set [YOUR PROJECT ID] #
    ################################
    project_id = '[YOUR PROJECT ID]'

    ################################
    # TO DO: Set [YOUR DATASET ID] #
    ################################
    dataset_id = '[YOUR DATASET ID]'

    bucket_name = data['bucket']
    file_name = data['name']
    file_ext = file_name.split('.')[-1]

    if file_ext == 'csv':
        uri = 'gs://' + bucket_name + '/' + file_name
        file_ext = file_name.split('.')[-1]
        suffix  = datetime.now().strftime("_%Y%m%d%H%M%S")
        table_id = 'tweets' + suffix

        # Job Configuration
        dataset_ref = client.dataset(dataset_id)
        job_config = bigquery.LoadJobConfig(autodetect=True)
        job_config.skip_leading_rows = 1
        job_config.source_format = bigquery.SourceFormat.CSV

        # Job Request
        load_job = client.load_table_from_uri(
            uri,
            dataset_ref.table(table_id),
            job_config=job_config
        ) 
        print('Started job {}'.format(load_job.job_id))

        load_job.result()  # Waits for table load to complete.
        print('Job finished.')

        destination_table = client.get_table(dataset_ref.table(table_id))
        print('Loaded {} rows.'.format(destination_table.num_rows))

    else:
        print('Nothing To Do')
