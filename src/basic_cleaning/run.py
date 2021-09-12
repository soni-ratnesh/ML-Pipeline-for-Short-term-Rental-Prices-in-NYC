#!/usr/bin/env python
"""
 Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact.
"""
import os
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):
    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    logger.info("Downloading artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    raw_data = pd.read_csv(artifact_local_path, low_memory=False)

    logger.info("Data cleaning : Remove price Outlier")
    idx = raw_data['price'].between(args.min_price, args.max_price)
    data = raw_data[idx].copy()

    logger.info("Data Cleaning: Remove rows with redundant geolocation ")
    idx = data['longitude'].between(-74.25, -73.50) & data['latitude'].between(40.5, 41.2)
    data = data[idx].copy()

    logger.info("Data cleaning : Convert last_review to datetime")
    data['last_review'] = pd.to_datetime(data['last_review'])

    logger.info("Data cleaning : Saving data to local")
    filename = "clean_sample.csv"
    data.to_csv(filename, index=False)

    logger.info("Creating Artifact")
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")

    logger.info("Logging artifact")
    run.log_artifact(artifact)

    logger.info("Deleting local clean data filw")
    os.remove(filename)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=" A very basic data cleaning")

    parser.add_argument(
        "--input_artifact",
        type=str,
        help="name of input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Name of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="Artifact type",
        required=True
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="Artifact description",
        required=True
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="Minimum accepted price",
        required=True
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="Maximum accepted price",
        required=True
    )

    args = parser.parse_args()

    go(args)
