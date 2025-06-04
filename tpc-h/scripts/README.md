## Explanation

There are two notebooks in this directory:

1. `iceberg-local-test.ipynb`

    This notebook was only used as a proof of concept to try inserting data into iceberg using local standalone spark. Once we managed to insert the data into iceberg using this script, we decided to move to AWS Glue for faster execution, so we used the second script. But to run this script:

    1. Fill out the environment variables needed, based on `.env.example`.

        - Copy env template

            ```
            cp .env.example .env
            ```

        - Fill the `ACCESS_KEY` and `SECRET_ACCESS_KEY` from your AWS IAM user

    2. Install requirements
        ```
        pip install -r requirements.txt
        ```
    3. Run the code blocks in the notebook

2. `iceberg-glue.ipynb`

    This is the actual script that we used to insert the data to iceberg, but it should be run in the AWS Glue Notebook
