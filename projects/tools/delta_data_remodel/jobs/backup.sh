#!/bin/zsh
aws s3 cp \
    s3://sfc-main-datasets/domain=CQC_delta/dataset=full_locations_api_cleaned/ \
    s3://sfc-1036-fix-locations-full-datasets/domain=CQC_delta/dataset=full_locations_api_cleaned/ \
    --recursive
#    --dryrun