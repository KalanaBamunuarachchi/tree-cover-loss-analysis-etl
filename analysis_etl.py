import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"

# Extraction

# extract data from csv
def extract_tree_cover(treecover_loss_by_region_ha):

    log("Extracting tree cover data")

    df_tree = pd.read_csv(treecover_loss_by_region_ha)

    log(f"Extracted {len(df_tree)} rows")
    return df_tree

def extract_regions(adm1_metadata):

    log("Extracting region metadata")

    df_region = pd.read_csv(adm1_metadata)

    log(f"Extracted {len(df_region)} regions")
    return df_region

#Trasnform

def transform_data(tree_df, region_df):
    log("Merging datasets")
    merged_df = pd.merge(
        tree_df,
        region_df,
        on="adm1",
        how="left"
    )

    log(f"Merged dataset has {len(merged_df)} rows")

    return merged_df

def calculate_most_affected_districts(df):

    log("Calculating most affected districts")

    district_loss = (
        df.groupby("name")["umd_tree_cover_loss__ha"]
        .sum()
        .reset_index()
        .rename(columns={
            "umd_tree_cover_loss__ha": "total_tree_cover_loss_ha"
        })
        .sort_values(by="total_tree_cover_loss_ha", ascending=False)
    )

    log("District analysis complete")

    return district_loss

# Load Data in to new csv

def load_to_csv(df, target_file):
    log(f"Saving transformed data to {target_file}")

    df.to_csv(target_file, index=False)

    log("Data saved successfully")

    
# Logging Function

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as f:
        f.write(f"{timestamp} - {message}\n")



tree_df = extract_tree_cover("treecover_loss_by_region__ha.csv")

region_df = extract_regions("adm1_metadata.csv")

merged_df = transform_data(tree_df, region_df)

district_loss = calculate_most_affected_districts(merged_df)

load_to_csv(district_loss, target_file)

log("ETL pipeline completed")

