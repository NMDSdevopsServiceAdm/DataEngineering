from pyspark.sql import DataFrame

from utils import utils


def main(postcode_directory_source, pa_filled_posts_source, destination):
    # todo 1 - create a dataframe from the cleaned ons postcode directory.

    # todo 2 - for each LA in the postcode directory, make a list of the ICB areas it grouped up into.

    # todo 3 - count the number of rows (postcodes) for each LA area, count the number of rows (postcodes) for each hybrid_area.

    # todo 4 - get the percentage value for hybrid_area_count out of LA_area_count.

    # todo 5 - remove duplicates of LA area and ICB area.

    # todo 6 - join pa filled posts on la area name.

    # todo 7 - create new column which is pa filled posts multiplied by percentage value.

    # todo 8 - remove the original pa filled post by LA column.
    ...


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
