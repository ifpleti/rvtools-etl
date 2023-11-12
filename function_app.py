import azure.functions as func
import logging
import os
import tempfile
import uuid
import pandas as pd
import json
from io import BytesIO
import base64
from openpyxl import load_workbook
import time

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="convert_xlsx_to_parquet")
def convert_xlsx_to_parquet(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    filename = req.params.get("filename")
    if not filename:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            filename = req_body.get("filename")

    content = req.params.get("content")
    if not content:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            content = req_body.get("content")

    if filename and content:
        # Generate a name for the temporary directory and files
        temp_dir_name = str(uuid.uuid4())
        logging.info("Temporary directory name generated successfully.")

        # Define working directory
        working_dir = os.path.join(tempfile.gettempdir(), temp_dir_name)
        logging.info("Working directory defined successfully.")

        # Create the working directory
        os.makedirs(working_dir)
        logging.info("Working directory created successfully.")

        # Pasar content a binario
        content_in_bytes = base64.b64decode(content)
        file_in_memory = BytesIO(content_in_bytes)

        # Lee todas las hojas en un diccionario de DataFrames
        with pd.ExcelFile(file_in_memory) as xls:
            excel_dfs = {}
            for page in xls.sheet_names:
                excel_dfs[page] = pd.read_excel(xls, page)

        excel_dfs["vHBA"]["vHBAPci"] = excel_dfs["vHBA"]["vHBAPci"].apply(str)
        excel_dfs["vNIC"]["vNicPci"] = excel_dfs["vNIC"]["vNicPci"].apply(str)
        excel_dfs["vMultiPath"]["vMultiPathModel"] = excel_dfs["vMultiPath"][
            "vMultiPathModel"
        ].apply(str)
        excel_parquets = []
        for key, value in excel_dfs.items():
            try:
                if (
                    key is not None
                    and isinstance(value, pd.DataFrame)
                    and not value.empty
                ):
                    value.insert(0, "sourceFilename", filename)
                    value.to_parquet(os.path.join(working_dir, f"{key}.parquet"))
                    logging.info(value)
                else:
                    logging.warning(
                        "key is None, value is not a non-empty DataFrame, or value is None"
                    )
            except Exception as e:
                logging.error(f"An error occurred: {str(e)}")

        for parquet_name in os.listdir(working_dir):
            # Comprobar si el archivo es un archivo parquet
            if parquet_name.endswith(".parquet"):
                # Obtener la clave del nombre del archivo (sin la extensión)
                page_name = parquet_name.replace(".parquet", "")
                # Leer el archivo parquet y almacenarlo en el diccionario
                with open(os.path.join(working_dir, parquet_name), "rb") as f:
                    page_parquet = {}
                    page_parquet["page_name"] = page_name
                    page_parquet["content"] = base64.b64encode(f.read()).decode("utf-8")
                    excel_parquets.append(page_parquet)
                    logging.info(
                        f"File {parquet_name} read successfully and stored in the excel_parquets dictionary."
                    )

        # Return the excel_parquets dictionary as a JSON object
        logging.info(
            f"Hello, {filename}. This HTTP triggered function executed successfully."
        )
        return func.HttpResponse(
            headers={"Content-Type": "application/json"},
            body=json.dumps(excel_parquets),
            status_code=200,
        )

    else:
        return func.HttpResponse(
            "This HTTP triggered function executed unsuccessfully.", status_code=400
        )


@app.route(route="merge_parquets", auth_level=func.AuthLevel.FUNCTION)
def merge_parquets(req: func.HttpRequest) -> func.HttpResponse:
    # Log a message indicating the start of the function execution
    logging.info("Starting the merge_parquets function.")

    # Attempt to get 'parquet_contents' from the query parameters
    parquet_contents = req.params.get("parquet_contents")

    # If 'parquet_contents' was not found in the query, try reading it from the request body
    if not parquet_contents:
        try:
            req_body = req.get_json()
        except ValueError:
            # If the request body is not JSON, log an error
            logging.error("Request body is not a valid JSON.")
            return func.HttpResponse("Invalid JSON in request body.", status_code=400)
        else:
            parquet_contents = req_body.get("parquet_contents")

    # Extract the actual content from 'parquet_contents' if it's not None
    if parquet_contents:
        parquet_contents = [
            item["$content"] for item in parquet_contents if "$content" in item
        ]

    # Log the number of parquet files to be processed
    logging.info(f"Number of parquet files received: {len(parquet_contents)}")

    # Create a temporary directory to store intermediate files
    temp_dir_name = str(uuid.uuid4())
    working_dir = os.path.join(tempfile.gettempdir(), temp_dir_name)
    os.makedirs(working_dir)
    logging.info("Created a temporary directory for processing.")

    # Initialize an empty list to store dataframes
    page_dfs = []
    for content in parquet_contents:
        # Decode the base64 content to binary
        content_in_bytes = base64.b64decode(content)
        # Read the parquet file into a pandas DataFrame
        df = pd.read_parquet(BytesIO(content_in_bytes))

        # Check if the DataFrame is not empty and not all elements are NA
        if not df.empty and not df.isna().all().all():
            page_dfs.append(df)
        else:
            logging.warning("Skipping an empty DataFrame.")

    # If we have valid DataFrames, concatenate them
    if page_dfs:
        # Concatenate the DataFrames
        start_merge_time = time.time()
        merged_df = pd.concat(page_dfs, ignore_index=True)
        logging.info(
            f"Time taken to merge the DataFrames: {time.time()-start_merge_time}"
        )

        # Convert all columns to string to avoid issues with mixed data types
        start_fix_time = time.time()
        for column in merged_df.columns:
            if column == "vNicDuplex":
                merged_df[column] = merged_df[column].astype(str)
            elif column == "vInfoVISDKAPI":
                merged_df[column] = merged_df[column].astype(str)
            elif column == "vHostBiosDate":
                merged_df[column] = merged_df[column].astype(str)
            elif column == "vMultiPathRevision":
                merged_df[column] = merged_df[column].astype(str)
            elif column == "vMultiPathUUID":
                merged_df[column] = merged_df[column].astype(str)
        logging.info(f"Time taken to fix the data types: {time.time()-start_fix_time}")

        merged_file_path = os.path.join(working_dir, "merged.parquet")

        try:
            # Save the DataFrame to a parquet file
            start_save_time = time.time()
            merged_df.to_parquet(merged_file_path)
            logging.info("DataFrames merged and saved to a parquet file.")
            logging.info(
                f"Time taken to save the merged DataFrame to parquet: {time.time()-start_save_time}"
            )

        except Exception as e:
            # Log the exception if the to_parquet conversion fails
            logging.error(f"Failed to write DataFrame to parquet file: {e}")
            return func.HttpResponse(
                "Failed to write DataFrame to parquet file.", status_code=500
            )

        # Read the merged parquet file and encode it in base64 to return as JSON
        with open(merged_file_path, "rb") as f:
            start_read_time = time.time()
            merged_parquet_base64 = base64.b64encode(f.read()).decode("utf-8")
            logging.info(
                f"Time taken to read the merged parquet file and encode it in base64: {time.time()-start_read_time}"
            )

        # Return the base64 encoded parquet file as a response
        logging.info("Function executed successfully, returning the merged content.")
        return func.HttpResponse(
            headers={"Content-Type": "application/json"},
            body=json.dumps({"content": merged_parquet_base64}),
            status_code=200,
        )
    else:
        # If there are no valid DataFrames to merge, log a message and return an empty response
        logging.info("No valid DataFrames found to merge.")
        return func.HttpResponse("No valid DataFrames to merge.", status_code=200)
