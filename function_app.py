import azure.functions as func
import logging
import os
import tempfile
import uuid
import pandas as pd
from io import BytesIO
import base64
from openpyxl import load_workbook

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="convert_xlsx_to_df")
def convert_xlsx_to_df(req: func.HttpRequest) -> func.HttpResponse:
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
        excel_parquets = {}
        for key, value in excel_dfs.items():
            logging.info(key)
            try:
                if (
                    key is not None
                    and isinstance(value, pd.DataFrame)
                    and not value.empty
                ):
                    # logging.info(f"key: {key}, value: {value}")
                    value.to_parquet(os.path.join(working_dir, f"{key}.parquet"))
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
                key = parquet_name[:-8]
                # Leer el archivo parquet y almacenarlo en el diccionario
                with open(os.path.join(working_dir, parquet_name), "rb") as f:
                    excel_parquets[key] = f.read()

        logging.info(excel_parquets)

        # excel_dfs["filename"] = filename
        logging.info(
            f"Hello, {filename}. This HTTP triggered function executed successfully."
        )

        return func.HttpResponse(
            f"Hello, {filename}. This HTTP triggered function executed successfully."
        )
    else:
        return func.HttpResponse(
            "This HTTP triggered function executed unsuccessfully.", status_code=400
        )
