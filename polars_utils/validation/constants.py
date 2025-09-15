import pointblank as pb

GLOBAL_THRESHOLDS = pb.Thresholds(warning=1)
GLOBAL_ACTIONS = pb.Actions(
    warning="{LEVEL}: {type} validation failed on step {step} for column {col} with value {val}}."
)
