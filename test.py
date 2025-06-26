import pandas as pd

# Step 1: Load the config CSV
input_path = "/Users/shreyakumbam/Documents/ETLProject/extract/etl_config.csv"
df = pd.read_csv(input_path)

# Step 2: Save it as extracted_data.csv
output_path = "/Users/shreyakumbam/Documents/ETLProject/extract/extracted_data.csv"
df.to_csv(output_path, index=False)

print(f"CSV extraction complete. File saved to: {output_path}")
