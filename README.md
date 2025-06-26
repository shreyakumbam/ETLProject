Extraction
    Created a table in PostgreSQL named it 'etl_data'
    Extracted the Source FieldName Column 
    Connected to JDBC via jaydebeapi
    Constructed SQL dynamically after connection
    Saved the extracted file to 'extracted_data.csv'

Transformation 
    Performed Source-to-Target Mapping 
    Mapped the Source FieldName to Target FieldName
    Loaded the extracted data file
    Converted the data types accordingly and matched the fields.
    Saved the Transformed file to 'transformed_data.csv'

Loader
    *TBD*
    Delta lake 
    RDBMS
    Bulk loader - those libraries/utilities
    Parquet file 