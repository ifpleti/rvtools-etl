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
