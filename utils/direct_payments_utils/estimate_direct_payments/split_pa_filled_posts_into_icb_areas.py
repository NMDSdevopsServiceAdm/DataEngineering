from pyspark.sql import DataFrame

from utils import utils


def main(sourcepostcode_directory_source, pa_filled_posts_source, destination): ...


if __name__ == "__main__":
    (
        postcode_directory_source,
        pa_filled_posts_source,
        destination,
    ) = utils.collect_arguments(
        (
            "--postcode_directory_source",
            "Source s3 directory for cleaned ons postcode directory",
        )(
            "--pa_filled_posts_source",
            "Source s3 directory for estimated pa filled posts split by la area dataset",
        ),
        (
            "--destination",
            "A destination directory for outputting pa filled posts split by icb area data.",
        ),
    )

    main(
        postcode_directory_source,
        pa_filled_posts_source,
        destination,
    )
