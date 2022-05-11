from datetime import datetime
from pathlib import Path
from utils import utils
import shutil
import unittest
from pyspark.sql import SparkSession
import botocore.session
from botocore.stub import Stubber
from botocore.response import StreamingBody
from io import BytesIO
import boto3
import json

class UtilsTests(unittest.TestCase):

    test_csv_path = "tests/test_data/example_csv.csv"
    test_csv_custom_delim_path = "tests/test_data/example_csv_custom_delimiter.csv"
    tmp_dir = "tmp-out"

    def setUp(self):
        spark = SparkSession.builder.appName(
            "sfc_data_engineering_csv_to_parquet"
        ).getOrCreate()
        self.df = spark.read.csv(self.test_csv_path, header=True)

    def tearDown(self):
        try:
            shutil.rmtree(self.tmp_dir)
        except OSError as e:
            pass  # Ignore dir does not exist

    def test_get_s3_objects_list_returns_all_objects(self):
        # TODO DO THIS TEST
        pass

    def test_get_s3_objects_doesnt_return_directories(self):
        # TODO DO THIS TEST
        pass

    def test_get_s3_objects_list_returns_filtered_objects(self):
        s3 = boto3.resource("s3")
        stubber = Stubber(s3.meta.client)

        partial_response = {
            "Contents": [{
                "Key": "version=1.0.0/import_date=20210101/some-data-file.csv",
                'Size': 123
            }]
        }

        expected_params = {"Bucket": "test-bucket",
                           "Prefix": "version=1.0.0/import_date=20210101/"}

        stubber.add_response("list_objects", partial_response, expected_params)
        stubber.activate()

        object_list = utils.get_s3_objects_list(
            "test-bucket", "version=1.0.0/import_date=20210101/", s3)

        print(f"Object list {object_list}")
        self.assertEqual(
            object_list, ["version=1.0.0/import_date=20210101/some-data-file.csv"])
        self.assertEqual(len(object_list), 1)


    def test_read_partial_csv_content(self):
        s3 = boto3.client("s3")
        stubber = Stubber(s3)

        body_data = "Id,SepalLengthCm,SepalWidthCm,PetalLengthCm,PetalWidthCm,Species"

        body_encoded = body_data.encode("utf-8")
        byte_string_length = len(body_encoded)

        body = StreamingBody(
            BytesIO(body_encoded),
            byte_string_length
        )

        partial_response = {
            'Body': body,
            'ContentLength': byte_string_length * 100
        }

        expected_params = {"Bucket": "test-bucket", "Key": "my-test/key/"}

        stubber.add_response("get_object", partial_response, expected_params)
        stubber.activate()

        obj_partial_content = utils.read_partial_csv_content(
            "test-bucket", "my-test/key/", s3)

        print(f"Object partial content: {obj_partial_content}")
        self.assertEqual(
            obj_partial_content, "Id,SepalLengthCm,SepalWidthCm,PetalLengthCm,PetalWidthCm,Species")

    def test_identify_csv_delimiter_can_identify_comma(self):
        sample = "Id,SepalLengthCm,SepalWidthCm,PetalLengthCm,PetalWidthCm,Species"
        delimiter = utils.identify_csv_delimiter(sample)
        self.assertEqual(delimiter, ",")

    def test_identify_csv_delimiter_can_identify_pipe(self):
        sample = "period|establishmentid|tribalid|parentid|orgid|nmdsid|wkplacestat|estabcreateddate|logincount_month|logincount_year|lastloggedin|previous_logindate|updatecount_month|updatecount_year|estabupdateddate|estabsavedate|workerupdate|mupddate|previous_mupddate|derivedfrom_hasbulkuploaded|isbulkuploader|lastbulkuploaddate|isparent|parentpermission|workersavedate|cqcpermission|lapermission|regtype|providerid|locationid|esttype|esttype_changedate|esttype_savedate|establishmentname|address|postcode|regionid|cssr|lauthid|totalstaff|totalstaff_changedate|totalstaff_savedate|wkrrecs|wkrrecs_changedate|wkrrecs_wdfsavedate|totalstarters|totalstarters_changedate|totalstarters_savedate|totalleavers|totalleavers_changedate|totalleavers_savedate|totalvacancies|totalvacancies_changedate|totalvacancies_savedate|mainstid|mainstid_changedate|mainstid_savedate|jr28flag|jr28perm|jr28temp|jr28pool|jr28agcy|jr28oth|jr28emp|jr28work|jr28strt|jr28stop|jr28vacy|jr29flag|jr29perm|jr29temp|jr29pool|jr29agcy|jr29oth|jr29emp|jr29work|jr29strt|jr29stop|jr29vacy|jr30flag|jr30perm|jr30temp|jr30pool|jr30agcy|jr30oth|jr30emp|jr30work|jr30strt|jr30stop|jr30vacy|jr31flag|jr31perm|jr31temp|jr31pool|jr31agcy|jr31oth|jr31emp|jr31work|jr31strt|jr31stop|jr31vacy|jr32flag|jr32perm|jr32temp|jr32pool|jr32agcy|jr32oth|jr32emp|jr32work|jr32strt|jr32stop|jr32vacy|jr01flag|jr01perm|jr01temp|jr01pool|jr01agcy|jr01oth|jr01emp|jr01work|jr01strt|jr01stop|jr01vacy|jr02flag|jr02perm|jr02temp|jr02pool|jr02agcy|jr02oth|jr02emp|jr02work|jr02strt|jr02stop|jr02vacy|jr03flag|jr03perm|jr03temp|jr03pool|jr03agcy|jr03oth|jr03emp|jr03work|jr03strt|jr03stop|jr03vacy|jr04flag|jr04perm|jr04temp|jr04pool|jr04agcy|jr04oth|jr04emp|jr04work|jr04strt|jr04stop|jr04vacy|jr05flag|jr05perm|jr05temp|jr05pool|jr05agcy|jr05oth|jr05emp|jr05work|jr05strt|jr05stop|jr05vacy|jr06flag|jr06perm|jr06temp|jr06pool|jr06agcy|jr06oth|jr06emp|jr06work|jr06strt|jr06stop|jr06vacy|jr07flag|jr07perm|jr07temp|jr07pool|jr07agcy|jr07oth|jr07emp|jr07work|jr07strt|jr07stop|jr07vacy|jr08flag|jr08perm|jr08temp|jr08pool|jr08agcy|jr08oth|jr08emp|jr08work|jr08strt|jr08stop|jr08vacy|jr09flag|jr09perm|jr09temp|jr09pool|jr09agcy|jr09oth|jr09emp|jr09work|jr09strt|jr09stop|jr09vacy|jr10flag|jr10perm|jr10temp|jr10pool|jr10agcy|jr10oth|jr10emp|jr10work|jr10strt|jr10stop|jr10vacy|jr11flag|jr11perm|jr11temp|jr11pool|jr11agcy|jr11oth|jr11emp|jr11work|jr11strt|jr11stop|jr11vacy|jr15flag|jr15perm|jr15temp|jr15pool|jr15agcy|jr15oth|jr15emp|jr15work|jr15strt|jr15stop|jr15vacy|jr16flag|jr16perm|jr16temp|jr16pool|jr16agcy|jr16oth|jr16emp|jr16work|jr16strt|jr16stop|jr16vacy|jr17flag|jr17perm|jr17temp|jr17pool|jr17agcy|jr17oth|jr17emp|jr17work|jr17strt|jr17stop|jr17vacy|jr22flag|jr22perm|jr22temp|jr22pool|jr22agcy|jr22oth|jr22emp|jr22work|jr22strt|jr22stop|jr22vacy|jr23flag|jr23perm|jr23temp|jr23pool|jr23agcy|jr23oth|jr23emp|jr23work|jr23strt|jr23stop|jr23vacy|jr24flag|jr24perm|jr24temp|jr24pool|jr24agcy|jr24oth|jr24emp|jr24work|jr24strt|jr24stop|jr24vacy|jr25flag|jr25perm|jr25temp|jr25pool|jr25agcy|jr25oth|jr25emp|jr25work|jr25strt|jr25stop|jr25vacy|jr26flag|jr26perm|jr26temp|jr26pool|jr26agcy|jr26oth|jr26emp|jr26work|jr26strt|jr26stop|jr26vacy|jr27flag|jr27perm|jr27temp|jr27pool|jr27agcy|jr27oth|jr27emp|jr27work|jr27strt|jr27stop|jr27vacy|jr34flag|jr34perm|jr34temp|jr34pool|jr34agcy|jr34oth|jr34emp|jr34work|jr34strt|jr34stop|jr34vacy|jr35flag|jr35perm|jr35temp|jr35pool|jr35agcy|jr35oth|jr35emp|jr35work|jr35strt|jr35stop|jr35vacy|jr36flag|jr36perm|jr36temp|jr36pool|jr36agcy|jr36oth|jr36emp|jr36work|jr36strt|jr36stop|jr36vacy|jr37flag|jr37perm|jr37temp|jr37pool|jr37agcy|jr37oth|jr37emp|jr37work|jr37strt|jr37stop|jr37vacy|jr38flag|jr38perm|jr38temp|jr38pool|jr38agcy|jr38oth|jr38emp|jr38work|jr38strt|jr38stop|jr38vacy|jr39flag|jr39perm|jr39temp|jr39pool|jr39agcy|jr39oth|jr39emp|jr39work|jr39strt|jr39stop|jr39vacy|jr40flag|jr40perm|jr40temp|jr40pool|jr40agcy|jr40oth|jr40emp|jr40work|jr40strt|jr40stop|jr40vacy|jr41flag|jr41perm|jr41temp|jr41pool|jr41agcy|jr41oth|jr41emp|jr41work|jr41strt|jr41stop|jr41vacy|jr42flag|jr42perm|jr42temp|jr42pool|jr42agcy|jr42oth|jr42emp|jr42work|jr42strt|jr42stop|jr42vacy|ut_changedate|ut_savedate|ut01flag|ut02flag|ut22flag|ut23flag|ut25flag|ut26flag|ut27flag|ut46flag|ut03flag|ut28flag|ut06flag|ut29flag|ut05flag|ut04flag|ut07flag|ut08flag|ut31flag|ut09flag|ut45flag|ut18flag|ut19flag|ut20flag|ut21flag|st_changedate|st_savedate|st01flag|st01cap|st01cap_changedate|st01cap_savedate|st01util|st01util_changedate|st01util_savedate|st02flag|st02cap|st02cap_changedate|st02cap_savedate|st02util|st02util_changedate|st02util_savedate|st53flag|st53util|st53util_changedate|st53util_savedate|st05flag|st05cap|st05cap_changedate|st05cap_savedate|st05util|st05util_changedate|st05util_savedate|st06flag|st06cap|st06cap_changedate|st06cap_savedate|st06util|st06util_changedate|st06util_savedate|st07flag|st07cap|st07cap_changedate|st07cap_savedate|st07util|st07util_changedate|st07util_savedate|st10flag|st10util|st10util_changedate|st10util_savedate|st08flag|st08util|st08util_changedate|st08util_savedate|st54flag|st54util|st54util_changedate|st54util_savedate|st74flag|st74util|st74util_changedate|st74util_savedate|st55flag|st55util|st55util_changedate|st55util_savedate|st73flag|st73util|st73util_changedate|st73util_savedate|st12flag|st12util|st12util_changedate|st12util_savedate|st13flag|st15flag|st18flag|st20flag|st19flag|st17flag|st17cap|st17cap_changedate|st17cap_savedate|st17util|st17util_changedate|st17util_savedate|st14flag|st16flag|st21flag|st63flag|st61flag|st62flag|st64flag|st66flag|st68flag|st67flag|st69flag|st70flag|st71flag|st75flag|st72flag|st60flag|st52flag|hasmandatorytraining"
        delimiter = utils.identify_csv_delimiter(sample)
        self.assertEqual(delimiter, "|")


    def test_generate_s3_dir_date_path(self):

        dec_first_21 = datetime(2021, 12, 1)
        dir_path = utils.generate_s3_dir_date_path(
            "test_domain", "test_dateset", dec_first_21
        )
        self.assertEqual(
            dir_path,
            "s3://sfc-data-engineering/domain=test_domain/dataset=test_dateset/version=1.0.0/year=2021/month=12/day=01/import_date=20211201",
        )

    def test_read_csv(self):
        df = utils.read_csv(self.test_csv_path)
        self.assertEqual(df.columns, ["col_a", "col_b", "col_c", "date_col"])
        self.assertEqual(df.count(), 3)

    def test_read_with_custom_delimiter(self):
        df = utils.read_csv(self.test_csv_custom_delim_path, "|")

        self.assertEqual(df.columns, ["col_a", "col_b", "col_c"])
        self.assertEqual(df.count(), 3)

    def test_write(self):
        df = utils.read_csv(self.test_csv_path)
        utils.write_to_parquet(df, self.tmp_dir)

        self.assertTrue(Path("tmp-out").is_dir())
        self.assertTrue(Path("tmp-out/_SUCCESS").exists())

    def test_format_date_fields(self):
        self.assertEqual(self.df.select("date_col").first()[0], "28/11/1993")
        formatted_df = utils.format_date_fields(self.df)
        self.assertEqual(
            str(formatted_df.select("date_col").first()[0]), "1993-11-28 00:00:00"
        )

    def test_is_csv(self):
        self.assertTrue("s3://sfc-data-engineering-raw/domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/Provision - March 2013 - IND - NMDS-SC - ASCWDS format.csv")

    def test_split_s3_uri(self):
        s3_uri = "s3://sfc-data-engineering-raw/domain=ASCWDS/dataset=workplace/"
        bucket_name, prefix = utils.split_s3_uri(s3_uri)
        self.assertEqual(bucket_name, "sfc-data-engineering-raw")
        self.assertEqual(prefix, "domain=ASCWDS/dataset=workplace/")

    def test_construct_s3_uri(self):
        uri = utils.construct_s3_uri("sfc-data-engineering-raw", "domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/Provision - March 2013 - IND - NMDS-SC - ASCWDS format.csv")
        self.assertEqual(uri, "s3://sfc-data-engineering-raw/domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/Provision - March 2013 - IND - NMDS-SC - ASCWDS format.csv")

    def test_get_file_directory(self):
        path = utils.get_file_directory("domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331/Provision - March 2013 - IND - NMDS-SC - ASCWDS format.csv")
        self.assertEqual(path, "domain=ASCWDS/dataset=workplace/version=0.0.1/year=2013/month=03/day=31/import_date=20130331")

if __name__ == "__main__":
    unittest.main(warnings="ignore")
