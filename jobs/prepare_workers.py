import argparse
import sys

from utils import utils


COLUMNS = ['period', 'establishmentid', 'tribalid', 'tribalid_worker', 'parentid',
       'orgid', 'nmdsid', 'workerid', 'wrkglbid', 'wkplacestat', 'createddate',
       'updateddate', 'cqcpermission', 'lapermission', 'regtype', 'providerid',
       'locationid', 'esttype', 'regionid', 'cssr', 'lauthid', 'mainstid',
       'emplstat', 'mainjrid', 'strtdate', 'age', 'gender', 'disabled',
       'ethnicity', 'isbritish', 'nationality', 'britishcitizen', 'borninuk',
       'countryofbirth', 'yearofentry', 'homeregionid', 'homecssrid',
       'homelauthid', 'distwrkk', 'scerec',
       'startsec', 'startage', 'dayssick', 'zerohours', 'averagehours',
       'conthrs', 'salaryint', 'salary', 'hrlyrate', 'ccstatus', 'apprentice',
       'scqheld', 'levelscqheld', 'nonscqheld', 'levelnonscqheld',
       'listqualsachflag', 'listhiqualev', 'jd16registered', 'amhp',
       'trainflag', 'flujab2020',
       'derivedfrom_hasbulkuploaded', 'previous_pay', 'previous_mainjrid',
       'version', 'year', 'month', 'day', 'import_date']


def main(source, destination):
    return True


def get_dataset_worker(source):
    spark = utils.get_spark()

    print(f"Reading worker parquet from {source}")

    worker_df = (
        spark.read.option("basePath", source)
        .parquet(source)
        .select(COLUMNS)
    )

    return worker_df


def collect_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source",
        help="A CSV file or directory of files used as job input",
        required=True,
    )
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting parquet files",
        required=True,
    )

    args, unknown = parser.parse_known_args()

    return args.source, args.destination


if __name__ == "__main__":
    print("Spark job 'prepare_workers' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination = collect_arguments()
    main(source, destination)

    print("Spark job 'prepare_workers' complete")
